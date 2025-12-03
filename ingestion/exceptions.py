
class IngestionError(Exception):
    """Base class for ingestion-related errors."""
    pass

class FileNotFoundError(IngestionError):
    """Raised when a required file is not found."""
    pass

class InvalidSchemaError(IngestionError):
    """Raised when the data format is invalid or unsupported."""
    pass

class CorruptedFileError(IngestionError):
    """Raised when a file is found to be corrupted or unreadable."""
    pass