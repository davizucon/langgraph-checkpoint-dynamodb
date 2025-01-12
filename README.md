# LangGraph DynamoDB Checkpoint

A DynamoDB implementation of the LangGraph checkpointer interface for persisting graph state and enabling features like human-in-the-loop, memory, time travel, and fault tolerance. Support sync and async methods using efficient DynamoDB queries and custom table configuration.

## Installation

```bash
pip install langgraph-checkpoint-amazon-dynamodb
```

## Quick Start

```python
from langgraph_checkpoint_dynamodb import DynamoDBSaver

# Create checkpointer with default settings
checkpointer = DynamoDBSaver()

# Use with your LangGraph
graph = workflow.compile(checkpointer=checkpointer)
```

For custom configuration:
```python
from langgraph_checkpoint_dynamodb import DynamoDBSaver, DynamoDBConfig, DynamoDBTableConfig

config = DynamoDBConfig(
    table_config=DynamoDBTableConfig(
        # Customize table name as needed
        table_name="langgraph-checkpoint"
    )
)

checkpointer = DynamoDBSaver(config)
```

## Configuration

### DynamoDB Table Configuration

The `DynamoDBTableConfig` class provides configuration options for the DynamoDB table:

```python
from langgraph_checkpoint_dynamodb import DynamoDBTableConfig, BillingMode

table_config = DynamoDBTableConfig(
    table_name="langgraph-checkpoint",  # Name of the DynamoDB table
    billing_mode=BillingMode.PAY_PER_REQUEST,  # PAY_PER_REQUEST or PROVISIONED
    enable_encryption=True,  # Enable server-side encryption
    enable_point_in_time_recovery=False,  # Enable point-in-time recovery
    enable_ttl=False,  # Enable TTL for items
    ttl_attribute="ttl",  # TTL attribute name
    
    # For PROVISIONED billing mode only:
    read_capacity=None,  # Provisioned read capacity units
    write_capacity=None,  # Provisioned write capacity units
    
    # Optional auto-scaling configuration
    min_read_capacity=None,
    max_read_capacity=None,
    min_write_capacity=None,
    max_write_capacity=None
)
```

### DynamoDB Client Configuration

The `DynamoDBConfig` class provides configuration for the DynamoDB client:

```python
from langgraph_checkpoint_dynamodb import DynamoDBConfig

config = DynamoDBConfig(
    table_config=table_config,  # DynamoDBTableConfig instance
    region_name="us-west-2",  # AWS region
    endpoint_url=None,  # Custom endpoint URL (e.g., for local DynamoDB)
    max_retries=3,  # Maximum number of retries
    initial_retry_delay=0.1,  # Initial retry delay in seconds
    max_retry_delay=1.0,  # Maximum retry delay in seconds
    
    # Optional AWS credentials (if not using default credentials)
    aws_access_key_id=None,
    aws_secret_access_key=None,
    aws_session_token=None
)
```

## Infrastructure Setup

There are three ways to set up the required DynamoDB infrastructure:

### 1. Using DynamoDBSaver Methods

The simplest way to create and manage the table:

```python
from langgraph_checkpoint_dynamodb import DynamoDBSaver, DynamoDBConfig

config = DynamoDBConfig(...)

# Create table
checkpointer = DynamoDBSaver.create(config)

# Reuse an existing table
checkpointer = DynamoDBSaver(config)

# Delete table when no longer needed
checkpointer.destroy()
```

### 2. Using AWS CDK

For more advanced infrastructure management using AWS CDK:

```python
from aws_cdk import App
from langgraph_checkpoint_dynamodb.infra import DynamoDBCheckpointStack
from langgraph_checkpoint_dynamodb import DynamoDBTableConfig

app = App()

# Create the stack with custom configuration
table_config = DynamoDBTableConfig(
    table_name="langgraph-checkpoint",
    enable_point_in_time_recovery=True,
    enable_encryption=True
)

DynamoDBCheckpointStack(
    app,
    "LangGraphCheckpoint",
    table_config=table_config
)

app.synth()
```

### 3. Using CloudFormation

For simple deployments using CloudFormation:

```yaml
# template.yaml
AWSTemplateFormatVersion: '2010-09-09'
Description: 'DynamoDB table for LangGraph checkpoint storage'

Parameters:
  TableName:
    Type: String
    Default: langgraph-checkpoint
  Environment:
    Type: String
    Default: dev
    AllowedValues: [dev, staging, prod]

Resources:
  CheckpointTable:
    Type: AWS::DynamoDB::Table
    Properties:
      TableName: !Ref TableName
      BillingMode: PAY_PER_REQUEST
      AttributeDefinitions:
        - AttributeName: PK
          AttributeType: S
        - AttributeName: SK
          AttributeType: S
      KeySchema:
        - AttributeName: PK
          KeyType: HASH
        - AttributeName: SK
          KeyType: RANGE
      PointInTimeRecoverySpecification:
        PointInTimeRecoveryEnabled: true
      SSESpecification:
        SSEEnabled: true
```

Deploy using AWS CLI:
```bash
aws cloudformation deploy \
  --template-file template.yaml \
  --stack-name langgraph-checkpoint \
  --parameter-overrides TableName=langgraph-checkpoint Environment=prod
```

## Usage with LangGraph

Once configured, use the DynamoDB checkpointer with your LangGraph workflow:

```python
from langgraph.graph import StateGraph
from langgraph_checkpoint_dynamodb import DynamoDBSaver, DynamoDBConfig

# Create your graph
workflow = StateGraph()
# ... configure your graph ...

# Setup checkpointer
config = DynamoDBConfig(...)
checkpointer = DynamoDBSaver(config)

# Compile with checkpointer
graph = workflow.compile(checkpointer=checkpointer)

# Run with thread_id for persistence
config = {"configurable": {"thread_id": "unique-thread-id"}}
graph.invoke({"input": "value"}, config)

# Run with async methods
async for chunk in graph.astream({"input":  "value"}, config, stream_mode="values"):
    chunk["messages"][-1].pretty_print()
```

## Features

The DynamoDB checkpointer enables all LangGraph persistence features:

- **Human-in-the-loop**: Inspect, interrupt, and approve graph steps
- **Memory**: Retain state between interactions in the same thread
- **Time Travel**: Replay and debug specific graph steps
- **Fault Tolerance**: Recover from failures and resume from last successful step
- **Sync and Async**: Support sync and async methods using efficient DynamoDB KeyConditionExpressions for queries
## License

MIT