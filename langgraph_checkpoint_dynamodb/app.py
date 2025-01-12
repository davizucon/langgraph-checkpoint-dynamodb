#!/usr/bin/env python3
import os

import aws_cdk as cdk

from infra.checkpoint_stack import DynamoDBCheckpointStack, DynamoDBTableConfig

app = cdk.App()

# Example 1: Default on-demand table
DynamoDBCheckpointStack(
    app,
    "DynamoDBCheckpointStack",
    # If you don't specify 'env', this stack will be environment-agnostic.
    # Account/Region-dependent features and context lookups will not work,
    # but a single synthesized template can be deployed anywhere.
    # Uncomment the next line to specialize this stack for the AWS Account
    # and Region that are implied by the current CLI configuration.
    # env=cdk.Environment(account=os.getenv('CDK_DEFAULT_ACCOUNT'), region=os.getenv('CDK_DEFAULT_REGION')),
    # For more information, see https://docs.aws.amazon.com/cdk/latest/guide/environments.html
)

# Example 2: Provisioned capacity table
# tenant_id = "default"
# config = DynamoDBTableConfig(
#     table_name=f"langgraph-checkpoint-prod-{tenant_id}",
#     billing_mode=cdk.aws_dynamodb.BillingMode.PROVISIONED,
#     read_capacity=5,
#     write_capacity=5,
#     min_read_capacity=5,
#     max_read_capacity=100,
#     min_write_capacity=5,
#     max_write_capacity=100,
#     removal_policy=cdk.RemovalPolicy.RETAIN,
#     enable_point_in_time_recovery=True,
# )
# DynamoDBCheckpointStack(
#     app,
#     "DynamoDBCheckpointStackProd",
#     env={"region": "us-east-1"},
#     tags={"Environment": "prod", "Tenant": tenant_id},
#     table_config=config,
# )

app.synth()
