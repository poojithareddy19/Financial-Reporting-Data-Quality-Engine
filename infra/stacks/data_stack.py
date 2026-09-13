"""RDS PostgreSQL in private subnets, a Secrets Manager secret and a security group."""

from __future__ import annotations

import aws_cdk as cdk
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_rds as rds
from constructs import Construct


class DataStack(cdk.Stack):
    """The reporting warehouse."""

    def __init__(self, scope: Construct, construct_id: str, *, prefix: str, **kwargs: object) -> None:
        super().__init__(scope, construct_id, **kwargs)  # type: ignore[arg-type]
        self.vpc = ec2.Vpc(
            self,
            "Vpc",
            max_azs=2,
            nat_gateways=0,
            subnet_configuration=[
                ec2.SubnetConfiguration(name="isolated", subnet_type=ec2.SubnetType.PRIVATE_ISOLATED, cidr_mask=24),
            ],
        )
        # Lambdas in the isolated subnets reach S3 / SNS / CloudWatch / Secrets Manager through endpoints (no NAT cost).
        self.vpc.add_gateway_endpoint("S3Endpoint", service=ec2.GatewayVpcEndpointAwsService.S3)
        for name, svc in (
            ("Sns", ec2.InterfaceVpcEndpointAwsService.SNS),
            ("Secrets", ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER),
            ("Logs", ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS),
            ("Monitoring", ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_MONITORING),
            ("Ecr", ec2.InterfaceVpcEndpointAwsService.ECR_DOCKER),
            ("EcrApi", ec2.InterfaceVpcEndpointAwsService.ECR),
        ):
            self.vpc.add_interface_endpoint(f"{name}Endpoint", service=svc)

        self.lambda_sg = ec2.SecurityGroup(
            self, "LambdaSg", vpc=self.vpc, description="fin-dq lambdas", allow_all_outbound=True
        )
        self.db_sg = ec2.SecurityGroup(
            self, "DbSg", vpc=self.vpc, description="fin-dq postgres", allow_all_outbound=False
        )
        self.db_sg.add_ingress_rule(self.lambda_sg, ec2.Port.tcp(5432), "pipeline lambdas")

        self.secret = rds.DatabaseSecret(
            self, "DbSecret", username="fin_dq_app", secret_name=f"{prefix}/db-credentials"
        )
        self.database = rds.DatabaseInstance(
            self,
            "Postgres",
            engine=rds.DatabaseInstanceEngine.postgres(version=rds.PostgresEngineVersion.VER_16),
            instance_type=ec2.InstanceType.of(ec2.InstanceClass.BURSTABLE4_GRAVITON, ec2.InstanceSize.MICRO),
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED),
            security_groups=[self.db_sg],
            credentials=rds.Credentials.from_secret(self.secret),
            database_name="fin_dq",
            allocated_storage=20,
            max_allocated_storage=100,
            storage_encrypted=True,
            multi_az=False,
            backup_retention=cdk.Duration.days(7),
            deletion_protection=True,
            removal_policy=cdk.RemovalPolicy.SNAPSHOT,
            cloudwatch_logs_exports=["postgresql"],
        )
        cdk.CfnOutput(self, "DbEndpoint", value=self.database.db_instance_endpoint_address)
        cdk.CfnOutput(self, "DbSecretArn", value=self.secret.secret_arn)
