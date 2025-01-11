import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, List, Optional, Sequence, Tuple

import aioboto3
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
    execute_with_retry,
    validate_checkpoint_item,
    validate_write_item,
)

logger = logging.getLogger(__name__)


class AsyncDynamoDBSaver(BaseCheckpointSaver):
    """
    Async DynamoDB implementation of checkpoint saver.

    Uses aioboto3 for true async operations.
    """

    def __init__(
        self,
        config: Optional[DynamoDBConfig] = None,
    ) -> None:
        """Initialize async DynamoDB saver."""
        super().__init__()
        self.config = config or DynamoDBConfig()
        self.session = aioboto3.Session()
        self._client = None
        self._table = None
        self._resource = None

    async def __aenter__(self) -> "AsyncDynamoDBSaver":
        """Initialize async resources."""
        # Create resource and client
        self._resource = await self.session.resource(
            "dynamodb", **self.config.get_client_config()
        ).__aenter__()
        self._client = await self.session.client(
            "dynamodb", **self.config.get_client_config()
        ).__aenter__()

        # Get table
        self._table = await self._resource.Table(self.config.table_config.table_name)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Clean up async resources."""
        if self._client:
            await self._client.__aexit__(exc_type, exc_val, exc_tb)
        if self._resource:
            await self._resource.__aexit__(exc_type, exc_val, exc_tb)

    async def aget_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        """Get checkpoint tuple asynchronously with validation."""
        if not self._table:
            raise DynamoDBConfigurationError(
                "Saver must be used as async context manager"
            )

        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id = get_checkpoint_id(config)

        try:
            if checkpoint_id:
                # Query both checkpoint and writes in one operation
                prefix = f"{checkpoint_ns}#{checkpoint_id}"
                response = await self._table.query(
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
                # Get latest checkpoint with validation
                response = await self._table.query(
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

                # Get and validate writes
                write_prefix = (
                    f"{checkpoint_ns}#{checkpoint_item['checkpoint_id']}#write#"
                )
                writes_response = await self._table.query(
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

            # Build checkpoint tuple from validated item
            checkpoint = self.serde.loads_typed(
                (checkpoint_item["type"], checkpoint_item["checkpoint"])
            )
            metadata = self.serde.loads_typed(
                (checkpoint_item["type"], checkpoint_item["metadata"])
            )

            parent_config = None
            if parent_id := checkpoint_item.get("parent_checkpoint_id"):
                parent_config = {
                    "configurable": {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": parent_id,
                    }
                }

            return CheckpointTuple(
                config={
                    "configurable": {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": checkpoint_item["checkpoint_id"],
                    }
                },
                checkpoint=checkpoint,
                metadata=metadata,
                parent_config=parent_config,
                pending_writes=pending_writes,
            )

        except ClientError as e:
            raise DynamoDBCheckpointError(
                f"Failed to get checkpoint: {e.response['Error']['Code']}"
            ) from e

    async def alist(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[Dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> AsyncIterator[CheckpointTuple]:
        """List checkpoints asynchronously."""
        if not self._table:
            raise DynamoDBConfigurationError(
                "Saver must be used as async context manager"
            )

        if not config:
            raise DynamoDBConfigurationError("Config required for listing checkpoints")

        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")

        try:
            # Build query conditions
            condition = Key("PK").eq(thread_id)
            sk_prefix = f"{checkpoint_ns}#"

            if before and (before_id := get_checkpoint_id(before)):
                condition = condition & Key("SK").lt(
                    f"{sk_prefix}{before_id}#checkpoint"
                )

            query_params = {
                "KeyConditionExpression": condition & Key("SK").begins_with(sk_prefix),
                "FilterExpression": Attr("SK").contains("#checkpoint"),
                "ScanIndexForward": False,
            }

            if limit:
                query_params["Limit"] = limit

            response = await execute_with_retry(
                lambda: self._table.query(**query_params),
                self.config,
                "Failed to list checkpoints",
            )

            items = response.get("Items", [])

            for item in items:
                try:
                    checkpoint_item = validate_checkpoint_item(item)

                    # Apply metadata filter
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
                    writes_response = await execute_with_retry(
                        lambda: self._table.query(
                            KeyConditionExpression=Key("PK").eq(thread_id)
                            & Key("SK").begins_with(write_prefix)
                        ),
                        self.config,
                        "Failed to get writes",
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

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Store checkpoint asynchronously."""
        if not self._table:
            raise DynamoDBConfigurationError(
                "Saver must be used as async context manager"
            )

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
            # Put item with retries
            await execute_with_retry(
                lambda: self._table.put_item(Item=item),
                self.config,
                "Failed to put checkpoint",
            )

            return {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint_id,
                }
            }
        except ClientError as e:
            raise DynamoDBCheckpointError("Failed to put checkpoint") from e

    async def _batch_write_items(
        self, items: List[Dict[str, Any]], allow_partial_failure: bool = False
    ) -> None:
        """
        Execute batch write operation with retries.

        Args:
            items: List of items to write
            allow_partial_failure: Whether to continue on partial batch failures
        """
        batch_size = 25  # DynamoDB batch write limit
        for i in range(0, len(items), batch_size):
            batch = items[i : i + batch_size]
            unprocessed = batch

            retry_count = 0
            while unprocessed and retry_count < self.config.max_retries:
                if retry_count > 0:
                    await asyncio.sleep(
                        min(
                            self.config.initial_retry_delay * (2**retry_count),
                            self.config.max_retry_delay,
                        )
                    )

                response = await self._client.batch_write_item(
                    RequestItems={
                        self.config.table_config.table_name: [
                            {"PutRequest": {"Item": item}} for item in unprocessed
                        ]
                    }
                )

                unprocessed = response.get("UnprocessedItems", {}).get(
                    self.config.table_config.table_name, []
                )
                retry_count += 1

            if unprocessed and not allow_partial_failure:
                raise DynamoDBCheckpointError(
                    f"Failed to write {len(unprocessed)} items after {retry_count} retries"
                )

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[Tuple[str, Any]],
        task_id: str,
    ) -> None:
        """Store writes asynchronously with efficient batch processing."""
        if not self._table:
            raise DynamoDBConfigurationError(
                "Saver must be used as async context manager"
            )

        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id = config["configurable"]["checkpoint_id"]

        try:
            # Process writes in batches
            batch_size = 25  # DynamoDB batch write limit
            for i in range(0, len(writes), batch_size):
                batch_writes = writes[i : i + batch_size]
                batch_items = []

                for idx, (channel, value) in enumerate(batch_writes, start=i):
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

                    batch_items.append({"PutRequest": {"Item": item}})

                # Execute batch write with retry
                await execute_with_retry(
                    lambda: self._client.batch_write_item(
                        RequestItems={self.config.table_config.table_name: batch_items}
                    ),
                    self.config,
                    "Failed to put writes",
                )

        except ClientError as e:
            raise DynamoDBCheckpointError("Failed to put writes") from e

    @classmethod
    @asynccontextmanager
    async def create(
        cls, config: DynamoDBConfig, create_table: bool = False
    ) -> AsyncIterator["AsyncDynamoDBSaver"]:
        """Create async saver with table management."""
        config.table_config.validate()
        saver = cls(config)

        try:
            async with saver as s:
                if create_table:
                    await s._create_or_update_table()
                yield s
        finally:
            await saver.__aexit__(None, None, None)

    async def _create_or_update_table(self) -> None:
        """Create or update DynamoDB table based on configuration."""
        if not self._client:
            raise DynamoDBConfigurationError(
                "Saver must be used as async context manager"
            )

        table_config = self.config.table_config

        try:
            # Check if table exists
            existing_table = await self._client.describe_table(
                TableName=table_config.table_name
            )
            table_desc = existing_table["Table"]

            # Update if needed
            updates_needed = []

            # Check billing mode
            if (
                table_desc.get("BillingModeSummary", {}).get("BillingMode")
                != table_config.billing_mode
            ):
                updates_needed.append(self._update_billing_mode())

            # Check capacity if provisioned
            if table_config.billing_mode == BillingMode.PROVISIONED:
                current_capacity = table_desc["ProvisionedThroughput"]
                if (
                    current_capacity["ReadCapacityUnits"] != table_config.read_capacity
                    or current_capacity["WriteCapacityUnits"]
                    != table_config.write_capacity
                ):
                    updates_needed.append(self._update_capacity())

            # Check point-in-time recovery
            if table_config.enable_point_in_time_recovery:
                pitr_desc = await self._client.describe_continuous_backups(
                    TableName=table_config.table_name
                )
                pitr_status = pitr_desc["ContinuousBackupsDescription"][
                    "PointInTimeRecoveryDescription"
                ]["PointInTimeRecoveryStatus"]
                if pitr_status != "ENABLED":
                    updates_needed.append(self._enable_pitr())

            # Execute updates if needed
            for update in updates_needed:
                await update()

        except self._client.exceptions.ResourceNotFoundException:
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

            await self._client.create_table(**create_args)

            # Wait for table to be active
            waiter = self._client.get_waiter("table_exists")
            await waiter.wait(TableName=table_config.table_name)

            # Enable PITR if configured
            if table_config.enable_point_in_time_recovery:
                await self._enable_pitr()

            # Enable TTL if configured
            if table_config.enable_ttl:
                await self._client.update_time_to_live(
                    TableName=table_config.table_name,
                    TimeToLiveSpecification={
                        "Enabled": True,
                        "AttributeName": table_config.ttl_attribute,
                    },
                )

    async def _update_billing_mode(self) -> None:
        """Update table billing mode asynchronously."""
        await execute_with_retry(
            lambda: self._client.update_table(
                TableName=self.config.table_config.table_name,
                BillingMode=self.config.table_config.billing_mode,
            ),
            self.config,
            "Failed to update billing mode",
        )

    async def _update_capacity(self) -> None:
        """Update provisioned capacity asynchronously."""
        await execute_with_retry(
            lambda: self._client.update_table(
                TableName=self.config.table_config.table_name,
                ProvisionedThroughput={
                    "ReadCapacityUnits": self.config.table_config.read_capacity,
                    "WriteCapacityUnits": self.config.table_config.write_capacity,
                },
            ),
            self.config,
            "Failed to update capacity",
        )

    async def _enable_pitr(self) -> None:
        """Enable point-in-time recovery asynchronously."""
        await execute_with_retry(
            lambda: self._client.update_continuous_backups(
                TableName=self.config.table_config.table_name,
                PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": True},
            ),
            self.config,
            "Failed to enable point-in-time recovery",
        )
