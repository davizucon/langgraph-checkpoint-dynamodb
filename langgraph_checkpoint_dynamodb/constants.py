from enum import Enum
from typing import Optional, TypedDict, Union
from boto3.dynamodb.types import Binary


class ItemType(str, Enum):
    """DynamoDB item types."""

    CHECKPOINT = "checkpoint"
    WRITE = "write"


# total=False: allow extra fields like TTL
class CheckpointItem(TypedDict, total=False):
    """Type for DynamoDB checkpoint items."""

    PK: str
    SK: str
    type: str
    checkpoint_id: str
    checkpoint: Union[str, Binary]
    metadata: Union[str, Binary]
    parent_checkpoint_id: Optional[str]
    # TTL attribute will be added dynamically with the configured name


# total=False: allow extra fields like TTL
class WriteItem(TypedDict, total=False):
    """Type for DynamoDB write items."""

    PK: str
    SK: str
    type: str
    task_id: str
    channel: str
    value: Union[str, Binary]
    idx: int
    # TTL attribute will be added dynamically with the configured name


DynamoDBItem = Union[CheckpointItem, WriteItem]
