class IngestionError(Exception):
    """Base class for ingestion-level errors."""

    pass


class IngestionFileNotFound(IngestionError):
    """Raised when an ingestion source file does not exist."""

    pass


class InvalidSchemaError(IngestionError):
    """Raised when columns or dtypes do not match expected schema."""

    pass


class CorruptedFileError(IngestionError):
    """Raised when a file is found to be corrupted or unreadable."""

    pass

class IngestionMemoryError(IngestionError):
    """Raised when a dataset exceeds memory constraints."""

    pass


class IngestionProfilerWarning(IngestionError):
    """Raised when profiling indicates excessive CPU or RAM."""

    pass
