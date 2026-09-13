"""One container-image Lambda per stage, a Step Functions state machine with retry/catch, EventBridge at 06:00 UTC."""

from __future__ import annotations

import aws_cdk as cdk
from aws_cdk import aws_ecr as ecr
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_logs as logs
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as subs
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct

from stacks.data_stack import DataStack
from stacks.storage_stack import StorageStack

STAGES = ["ingest", "transform", "validate", "load", "report", "notify"]
STAGE_TIMEOUTS = {"ingest": 5, "transform": 10, "validate": 15, "load": 10, "report": 10, "notify": 2}
STAGE_MEMORY = {"ingest": 1024, "transform": 2048, "validate": 3008, "load": 1024, "report": 2048, "notify": 512}


class ComputeStack(cdk.Stack):
    """Pipeline compute and orchestration."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        prefix: str,
        storage: StorageStack,
        data: DataStack,
        image_tag: str,
        alert_email: str,
        **kwargs: object,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)  # type: ignore[arg-type]

        self.repository = ecr.Repository(
            self,
            "Repository",
            repository_name=f"{prefix}-pipeline",
            image_scan_on_push=True,
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )
        self.alert_topic = sns.Topic(self, "AlertTopic", topic_name=f"{prefix}-alerts", display_name="fin-dq alerts")
        self.failure_topic = sns.Topic(
            self, "FailureTopic", topic_name=f"{prefix}-failures", display_name="fin-dq failures"
        )
        for topic in (self.alert_topic, self.failure_topic):
            topic.add_subscription(subs.EmailSubscription(alert_email))

        role = iam.Role(
            self,
            "PipelineRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaVPCAccessExecutionRole")
            ],
        )
        # Least privilege: read raw, read/write staged + curated, publish to the alert topic, read one secret, put metrics.
        storage.raw_bucket.grant_read(role)
        storage.staged_bucket.grant_read_write(role)
        storage.curated_bucket.grant_read_write(role)
        self.alert_topic.grant_publish(role)
        data.secret.grant_read(role)
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["cloudwatch:PutMetricData"],
                resources=["*"],
                conditions={"StringEquals": {"cloudwatch:namespace": "FinDQEngine"}},
            )
        )

        env = {
            "FIN_DQ__ENVIRONMENT": "aws",
            "FIN_DQ__AWS__REGION": self.region,
            "FIN_DQ__AWS__RAW_BUCKET": storage.raw_bucket.bucket_name,
            "FIN_DQ__AWS__STAGED_BUCKET": storage.staged_bucket.bucket_name,
            "FIN_DQ__AWS__CURATED_BUCKET": storage.curated_bucket.bucket_name,
            "FIN_DQ__AWS__SNS_TOPIC_ARN": self.alert_topic.topic_arn,
            "FIN_DQ__AWS__SECRET_NAME": data.secret.secret_name,
            "FIN_DQ__PATHS__OUT_DIR": "/tmp/out",
            "FIN_DQ__PATHS__DATA_DIR": "/tmp/data",
            "FIN_DQ_CONFIG": "/app/config/settings.yaml",
        }
        self.functions: dict[str, lambda_.Function] = {}
        for stage in STAGES:
            self.functions[stage] = lambda_.DockerImageFunction(
                self,
                f"{stage.title()}Fn",
                function_name=f"{prefix}-{stage}",
                code=lambda_.DockerImageCode.from_ecr(
                    self.repository,
                    tag_or_digest=image_tag,
                    cmd=[f"fin_dq_engine.orchestration.lambda_handlers.{stage}_handler"],
                ),
                role=role,
                vpc=data.vpc,
                security_groups=[data.lambda_sg],
                timeout=cdk.Duration.minutes(STAGE_TIMEOUTS[stage]),
                memory_size=STAGE_MEMORY[stage],
                ephemeral_storage_size=cdk.Size.gibibytes(2),
                environment=env,
                log_retention=logs.RetentionDays.ONE_MONTH,
                architecture=lambda_.Architecture.X86_64,
            )

        notify_failure = tasks.SnsPublish(
            self,
            "NotifyFailure",
            topic=self.failure_topic,
            subject="[fin-dq] pipeline run failed",
            message=sfn.TaskInput.from_json_path_at("$"),
        )
        failed = notify_failure.next(
            sfn.Fail(self, "RunFailed", cause="A pipeline stage failed or the batch was aborted")
        )

        chain: sfn.Chain | None = None
        for stage in STAGES:
            task = tasks.LambdaInvoke(
                self,
                f"{stage.title()}Task",
                lambda_function=self.functions[stage],
                output_path="$.Payload",
                retry_on_service_exceptions=True,
            )
            task.add_retry(
                errors=["States.TaskFailed"], interval=cdk.Duration.seconds(30), max_attempts=2, backoff_rate=2.0
            )
            task.add_catch(failed, errors=["States.ALL"], result_path="$.error")
            chain = task if chain is None else chain.next(task)
        assert chain is not None
        definition = chain.next(sfn.Succeed(self, "RunSucceeded"))

        self.state_machine = sfn.StateMachine(
            self,
            "DailyPipeline",
            state_machine_name=f"{prefix}-daily",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=cdk.Duration.hours(2),
            logs=sfn.LogOptions(
                destination=logs.LogGroup(self, "SfnLogs", retention=logs.RetentionDays.ONE_MONTH),
                level=sfn.LogLevel.ERROR,
            ),
            tracing_enabled=True,
        )
        events.Rule(
            self,
            "DailySchedule",
            rule_name=f"{prefix}-daily-0600utc",
            schedule=events.Schedule.cron(minute="0", hour="6"),
            targets=[targets.SfnStateMachine(self.state_machine, input=events.RuleTargetInput.from_object({}))],
        )
        cdk.CfnOutput(self, "StateMachineArn", value=self.state_machine.state_machine_arn)
        cdk.CfnOutput(self, "RepositoryUri", value=self.repository.repository_uri)
