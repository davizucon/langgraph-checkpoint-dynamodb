import logging
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional, Sequence, Tuple

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    get_checkpoint_id,
)

from .config import BillingMode, DynamoDBConfig
from .errors import (
    DynamoDBConfigurationError,
    DynamoDBCheckpointError,
    DynamoDBValidationError,
)
from .utils import (
    create_checkpoint_item,
    create_write_item,
    validate_checkpoint_item,
    validate_write_item,
)

logger = logging.getLogger(__name__)


class DynamoDBSaver(BaseCheckpointSaver):
    """
    DynamoDB implementation of checkpoint saver.

    Stores checkpoints and writes in a single DynamoDB table using a composite
    key structure that enables efficient queries and lookups.
    """

    def __init__(
        self,
        config: Optional[DynamoDBConfig] = None,
    ) -> None:
        """
        Initialize DynamoDB checkpoint saver.

        Args:
            config: DynamoDB configuration
            create_table: Whether to create table if it doesn't exist
        """
        super().__init__()
        self.config = config or DynamoDBConfig()
        self.client = boto3.client("dynamodb", **config.get_client_config())
        self.resource = boto3.resource("dynamodb", **config.get_client_config())
        self.table = self.resource.Table(config.table_config.table_name)

    def get_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        """Get checkpoint tuple by config."""
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id = get_checkpoint_id(config)

        try:
            if checkpoint_id:
                # Query both checkpoint and writes in one operation
                prefix = f"{checkpoint_ns}#{checkpoint_id}"
                response = self.table.query(
                    KeyConditionExpression=Key("PK").eq(thread_id)
                    & Key("SK").begins_with(prefix),
                    ConsistentRead=True,
                )

                items = response.get("Items", [])
                if not items:
                    return None

                # Separate and validate checkpoint and writes
                checkpoint_item = None
                writes = []
                for item in items:
                    try:
                        if item["SK"].endswith("#checkpoint"):
                            checkpoint_item = validate_checkpoint_item(item)
                        elif "#write#" in item["SK"]:
                            writes.append(validate_write_item(item))
                    except DynamoDBValidationError as e:
                        logger.error(f"Invalid item found: {e}")
                        continue

                if not checkpoint_item:
                    return None

            else:
                # Get latest checkpoint
                response = self.table.query(
                    KeyConditionExpression=Key("PK").eq(thread_id)
                    & Key("SK").begins_with(f"{checkpoint_ns}#"),
                    FilterExpression=Attr("SK").contains("#checkpoint"),
                    ScanIndexForward=False,
                    Limit=1,
                    ConsistentRead=True,
                )

                if not response["Items"]:
                    return None

                try:
                    checkpoint_item = validate_checkpoint_item(response["Items"][0])
                except DynamoDBValidationError as e:
                    logger.error(f"Invalid checkpoint item found: {e}")
                    return None

                # Get writes for this checkpoint
                write_prefix = (
                    f"{checkpoint_ns}#{checkpoint_item['checkpoint_id']}#write#"
                )
                writes_response = self.table.query(
                    KeyConditionExpression=Key("PK").eq(thread_id)
                    & Key("SK").begins_with(write_prefix)
                )

                writes = []
                for item in writes_response.get("Items", []):
                    try:
                        writes.append(validate_write_item(item))
                    except DynamoDBValidationError as e:
                        logger.error(f"Invalid write item found: {e}")
                        continue

            # Process validated writes
            pending_writes = [
                (
                    write["task_id"],
                    write["channel"],
                    self.serde.loads_typed((write["type"], write["value"])),
                )
                for write in writes
            ]

            return CheckpointTuple(
                config={
                    "configurable": {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": checkpoint_item["checkpoint_id"],
                    }
                },
                checkpoint=self.serde.loads_typed(
                    (checkpoint_item["type"], checkpoint_item["checkpoint"])
                ),
                metadata=self.serde.loads_typed(
                    (checkpoint_item["type"], checkpoint_item["metadata"])
                ),
                parent_config=(
                    {
                        "configurable": {
                            "thread_id": thread_id,
                            "checkpoint_ns": checkpoint_ns,
                            "checkpoint_id": checkpoint_item["parent_checkpoint_id"],
                        }
                    }
                    if checkpoint_item.get("parent_checkpoint_id")
                    else None
                ),
                pending_writes=pending_writes,
            )

        except ClientError as e:
            raise DynamoDBCheckpointError("Failed to get checkpoint") from e

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Store checkpoint with metadata."""
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id = checkpoint["id"]

        # Serialize data
        type_, checkpoint_data = self.serde.dumps_typed(checkpoint)
        _, metadata_data = self.serde.dumps_typed(metadata)

        # Create and validate item
        item = create_checkpoint_item(
            thread_id,
            checkpoint_ns,
            checkpoint_id,
            type_,
            checkpoint_data,
            metadata_data,
            config["configurable"].get("checkpoint_id"),
        )

        try:
            self.table.put_item(Item=item)
            return {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint_id,
                }
            }
        except ClientError as e:
            raise DynamoDBCheckpointError("Failed to put checkpoint") from e

    def list(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[Dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> Iterator[CheckpointTuple]:
        """List checkpoints matching criteria."""
        if not config:
            raise DynamoDBConfigurationError("Config required for listing checkpoints")

        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")

        try:
            # Build query conditions
            condition = Key("PK").eq(thread_id)
            sk_prefix = f"{checkpoint_ns}#"

            # Add before condition if specified
            if before and (before_id := get_checkpoint_id(before)):
                condition = condition & Key("SK").lt(
                    f"{sk_prefix}{before_id}#checkpoint"
                )

            query_params = {
                "KeyConditionExpression": condition & Key("SK").begins_with(sk_prefix),
                "FilterExpression": Attr("SK").contains(
                    "#checkpoint"
                ),  # Only get checkpoint items
                "ScanIndexForward": False,
            }

            if limit:
                query_params["Limit"] = limit

            response = self.table.query(**query_params)
            items = response.get("Items", [])

            # Process each checkpoint
            for item in items:
                try:
                    checkpoint_item = validate_checkpoint_item(item)

                    # Apply metadata filter if specified
                    if filter:
                        metadata = self.serde.loads_typed(
                            (checkpoint_item["type"], checkpoint_item["metadata"])
                        )
                        if not all(metadata.get(k) == v for k, v in filter.items()):
                            continue

                    # Get writes for this checkpoint
                    write_prefix = (
                        f"{checkpoint_ns}#{checkpoint_item['checkpoint_id']}#write#"
                    )
                    writes_response = self.table.query(
                        KeyConditionExpression=Key("PK").eq(thread_id)
                        & Key("SK").begins_with(write_prefix)
                    )

                    writes = []
                    for write_item in writes_response.get("Items", []):
                        try:
                            writes.append(validate_write_item(write_item))
                        except DynamoDBValidationError as e:
                            logger.error(f"Invalid write item found: {e}")
                            continue

                    pending_writes = [
                        (
                            write["task_id"],
                            write["channel"],
                            self.serde.loads_typed((write["type"], write["value"])),
                        )
                        for write in writes
                    ]

                    yield CheckpointTuple(
                        config={
                            "configurable": {
                                "thread_id": thread_id,
                                "checkpoint_ns": checkpoint_ns,
                                "checkpoint_id": checkpoint_item["checkpoint_id"],
                            }
                        },
                        checkpoint=self.serde.loads_typed(
                            (checkpoint_item["type"], checkpoint_item["checkpoint"])
                        ),
                        metadata=self.serde.loads_typed(
                            (checkpoint_item["type"], checkpoint_item["metadata"])
                        ),
                        parent_config=(
                            {
                                "configurable": {
                                    "thread_id": thread_id,
                                    "checkpoint_ns": checkpoint_ns,
                                    "checkpoint_id": checkpoint_item[
                                        "parent_checkpoint_id"
                                    ],
                                }
                            }
                            if checkpoint_item.get("parent_checkpoint_id")
                            else None
                        ),
                        pending_writes=pending_writes,
                    )

                except DynamoDBValidationError as e:
                    logger.error(f"Invalid checkpoint item found: {e}")
                    continue

        except ClientError as e:
            raise DynamoDBCheckpointError("Failed to list checkpoints") from e

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[Tuple[str, Any]],
        task_id: str,
    ) -> None:
        """
        Store writes for a checkpoint.

        Implements batch writing with automatic chunking and retry handling.
        """
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id = config["configurable"]["checkpoint_id"]

        try:
            with self.table.batch_writer() as batch:
                for idx, (channel, value) in enumerate(writes):
                    type_, value_data = self.serde.dumps_typed(value)

                    # Create and validate write item
                    item = create_write_item(
                        thread_id,
                        checkpoint_ns,
                        checkpoint_id,
                        task_id,
                        idx,
                        channel,
                        type_,
                        value_data,
                    )

                    batch.put_item(Item=item)

        except ClientError as e:
            raise DynamoDBCheckpointError("Failed to put writes") from e

    @classmethod
    @contextmanager
    def create(
        cls, config: DynamoDBConfig, create_table: bool = False
    ) -> Iterator["DynamoDBSaver"]:
        """
        Create DynamoDBSaver with table management.

        Args:
            config: DynamoDB configuration
            create_table: Whether to create/update table if needed

        Example:
            with DynamoDBSaver.create(
                DynamoDBConfig(
                    table_config=TableConfig(
                        table_name="my-checkpoints",
                        billing_mode=BillingMode.PAY_PER_REQUEST
                    )
                ),
                create_table=True
            ) as saver:
                # Use saver
        """
        config.table_config.validate()
        saver = cls(config)

        try:
            if create_table:
                saver._create_or_update_table()
            yield saver
        finally:
            saver.client.close()

    def _create_or_update_table(self) -> None:
        """Create or update DynamoDB table based on configuration."""
        table_config = self.config.table_config

        try:
            # Check if table exists
            existing_table = self.client.describe_table(
                TableName=table_config.table_name
            )["Table"]

            # Update if needed
            updates_needed = []

            # Check billing mode
            if (
                existing_table.get("BillingModeSummary", {}).get("BillingMode")
                != table_config.billing_mode
            ):
                updates_needed.append(self._update_billing_mode())

            # Check capacity if provisioned
            if table_config.billing_mode == BillingMode.PROVISIONED:
                current_capacity = existing_table["ProvisionedThroughput"]
                if (
                    current_capacity["ReadCapacityUnits"] != table_config.read_capacity
                    or current_capacity["WriteCapacityUnits"]
                    != table_config.write_capacity
                ):
                    updates_needed.append(self._update_capacity())

            # Check point-in-time recovery
            if table_config.enable_point_in_time_recovery:
                pitr_status = self.client.describe_continuous_backups(
                    TableName=table_config.table_name
                )["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"][
                    "PointInTimeRecoveryStatus"
                ]
                if pitr_status != "ENABLED":
                    updates_needed.append(self._enable_pitr())

            # Execute updates if needed
            for update in updates_needed:
                update()

        except self.client.exceptions.ResourceNotFoundException:
            # Create new table
            create_args = {
                "TableName": table_config.table_name,
                "KeySchema": [
                    {"AttributeName": "PK", "KeyType": "HASH"},
                    {"AttributeName": "SK", "KeyType": "RANGE"},
                ],
                "AttributeDefinitions": [
                    {"AttributeName": "PK", "AttributeType": "S"},
                    {"AttributeName": "SK", "AttributeType": "S"},
                ],
                "BillingMode": table_config.billing_mode,
            }

            if table_config.billing_mode == BillingMode.PROVISIONED:
                create_args["ProvisionedThroughput"] = {
                    "ReadCapacityUnits": table_config.read_capacity,
                    "WriteCapacityUnits": table_config.write_capacity,
                }

            if table_config.enable_encryption:
                create_args["SSESpecification"] = {"Enabled": True, "SSEType": "KMS"}

            self.client.create_table(**create_args)

            # Wait for table to be active
            waiter = self.client.get_waiter("table_exists")
            waiter.wait(TableName=table_config.table_name)

            # Enable PITR if configured
            if table_config.enable_point_in_time_recovery:
                self._enable_pitr()

            # Enable TTL if configured
            if table_config.enable_ttl:
                self.client.update_time_to_live(
                    TableName=table_config.table_name,
                    TimeToLiveSpecification={
                        "Enabled": True,
                        "AttributeName": table_config.ttl_attribute,
                    },
                )

    def _update_billing_mode(self) -> None:
        """Update table billing mode."""
        self.client.update_table(
            TableName=self.config.table_config.table_name,
            BillingMode=self.config.table_config.billing_mode,
        )

    def _update_capacity(self) -> None:
        """Update provisioned capacity."""
        self.client.update_table(
            TableName=self.config.table_config.table_name,
            ProvisionedThroughput={
                "ReadCapacityUnits": self.config.table_config.read_capacity,
                "WriteCapacityUnits": self.config.table_config.write_capacity,
            },
        )

    def _enable_pitr(self) -> None:
        """Enable point-in-time recovery."""
        self.client.update_continuous_backups(
            TableName=self.config.table_config.table_name,
            PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": True},
        )
