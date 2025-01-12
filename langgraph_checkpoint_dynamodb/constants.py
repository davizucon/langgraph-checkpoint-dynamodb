from enum import Enum
from typing import TypedDict, Union


class ItemType(str, Enum):
    """DynamoDB item types."""

    CHECKPOINT = "checkpoint"
    WRITE = "write"


class CheckpointItem(TypedDict):
    """Type definition for checkpoint items."""

    PK: str
    SK: str
    type: str  # Serialization type
    checkpoint_id: str
    checkpoint: str  # Serialized data
    metadata: str  # Serialized data
    parent_checkpoint_id: str | None


class WriteItem(TypedDict):
    """Type definition for write items."""

    PK: str
    SK: str
    type: str  # Serialization type
    task_id: str
    channel: str
    value: str  # Serialized data
    idx: int


DynamoDBItem = Union[CheckpointItem, WriteItem]
