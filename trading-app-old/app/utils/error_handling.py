from typing import Any, Mapping
from app.utils.custom_logging import CustomLogger


class DuplicateEntryError(Exception):
    """
    Raised when an attempt is made to create a record that already exists in the database.

    Attributes:
        message (str): A detailed error message explaining the conflict.
        primary_keys (Dict[str, Any]): The primary keys that caused the conflict.
    """

    def __init__(self, message: str, primary_keys: Mapping[str, Any]) -> None:
        super().__init__(message)
        self.message = message
        self.primary_keys = primary_keys
        CustomLogger(name="DuplicateEntryError").error(f"{message} (Primary Keys: {primary_keys})")

    def __str__(self) -> str:
        return f"{self.message} (Primary Keys: {self.primary_keys})"
