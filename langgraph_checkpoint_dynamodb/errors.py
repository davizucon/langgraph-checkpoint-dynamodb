class DynamoDBCheckpointError(Exception):
    """Base exception class for DynamoDB checkpoint errors."""

    pass


class DynamoDBConnectionError(DynamoDBCheckpointError):
    """Raised when there are issues connecting to DynamoDB."""

    pass


class DynamoDBConfigurationError(DynamoDBCheckpointError):
    """Raised when there are configuration issues."""

    pass


class DynamoDBValidationError(DynamoDBCheckpointError):
    """Raised when item validation fails."""

    pass
