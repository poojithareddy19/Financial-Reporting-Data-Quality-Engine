"""CloudWatch dashboard and alarms: dq_pass_rate below 95%, any stage failure, state machine failures."""

from __future__ import annotations

import aws_cdk as cdk
from aws_cdk import aws_cloudwatch as cw
from aws_cdk import aws_cloudwatch_actions as cw_actions
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as subs
from constructs import Construct

from stacks.compute_stack import ComputeStack

NAMESPACE = "FinDQEngine"


class ObservabilityStack(cdk.Stack):
    """Dashboards and alarms."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        prefix: str,
        compute: ComputeStack,
        alert_email: str,
        **kwargs: object,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)  # type: ignore[arg-type]
        self.ops_topic = sns.Topic(self, "OpsTopic", topic_name=f"{prefix}-ops-alarms")
        self.ops_topic.add_subscription(subs.EmailSubscription(alert_email))
        action = cw_actions.SnsAction(self.ops_topic)

        def metric(name: str, stat: str = "Average") -> cw.Metric:
            return cw.Metric(namespace=NAMESPACE, metric_name=name, statistic=stat, period=cdk.Duration.hours(24))

        pass_rate = metric("dq_pass_rate", "Minimum")
        pass_rate_alarm = cw.Alarm(
            self,
            "DqPassRateAlarm",
            alarm_name=f"{prefix}-dq-pass-rate-below-95",
            metric=pass_rate,
            threshold=0.95,
            evaluation_periods=1,
            comparison_operator=cw.ComparisonOperator.LESS_THAN_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        )
        pass_rate_alarm.add_alarm_action(action)

        stage_failure_alarm = cw.Alarm(
            self,
            "StageFailureAlarm",
            alarm_name=f"{prefix}-stage-failure",
            metric=metric("stage_failure", "Sum"),
            threshold=0,
            evaluation_periods=1,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        )
        stage_failure_alarm.add_alarm_action(action)

        sfn_alarm = cw.Alarm(
            self,
            "StateMachineFailedAlarm",
            alarm_name=f"{prefix}-state-machine-failed",
            metric=compute.state_machine.metric_failed(period=cdk.Duration.hours(24)),
            threshold=0,
            evaluation_periods=1,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        )
        sfn_alarm.add_alarm_action(action)

        for stage, fn in compute.functions.items():
            alarm = cw.Alarm(
                self,
                f"{stage.title()}ErrorsAlarm",
                alarm_name=f"{prefix}-{stage}-errors",
                metric=fn.metric_errors(period=cdk.Duration.hours(24)),
                threshold=0,
                evaluation_periods=1,
                comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
                treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
            )
            alarm.add_alarm_action(action)

        dashboard = cw.Dashboard(self, "Dashboard", dashboard_name=f"{prefix}-pipeline")
        dashboard.add_widgets(
            cw.GraphWidget(
                title="DQ pass rate", left=[pass_rate], left_y_axis=cw.YAxisProps(min=0.9, max=1.0), width=12
            ),
            cw.GraphWidget(
                title="Rows processed vs quarantined",
                left=[metric("rows_processed", "Sum"), metric("rows_quarantined", "Sum")],
                width=12,
            ),
        )
        dashboard.add_widgets(
            cw.GraphWidget(
                title="Stage duration (seconds)",
                left=[
                    cw.Metric(
                        namespace=NAMESPACE,
                        metric_name="stage_duration_seconds",
                        statistic="Average",
                        period=cdk.Duration.hours(24),
                        dimensions_map={"stage": s},
                    )
                    for s in compute.functions
                ],
                width=12,
            ),
            cw.GraphWidget(title="Anomalies flagged", left=[metric("anomalies_flagged", "Sum")], width=6),
            cw.AlarmStatusWidget(title="Alarms", alarms=[pass_rate_alarm, stage_failure_alarm, sfn_alarm], width=6),
        )
