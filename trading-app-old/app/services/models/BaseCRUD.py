from sqlalchemy import Engine
from sqlalchemy.orm import Session
from sqlalchemy.inspection import inspect
from typing import (
    Type,
    TypeVar,
    Generic,
    Mapping,
    cast,
    Any,
    Optional,
    Tuple,
    Union,
    List,
)
from app.utils.error_handling import DuplicateEntryError
from app.models import Base


# Generic type for models
model_base = TypeVar("model_base", bound=Base)
model_primary_keys = TypeVar("model_primary_keys", bound=Mapping[str, Any])
model_update_keys = TypeVar("model_update_keys", bound=Mapping[str, Any])
model_return_type = TypeVar("model_return_type", bound=Mapping[str, Any])


class CRUD(
    Generic[model_base, model_return_type, model_update_keys, model_primary_keys]
):
    """
    A generic CRUD (Create, Read, Update, Delete) utility class for interacting with SQLAlchemy models.

    Generic Types:
        model_base: The SQLAlchemy ORM model class (e.g., `User`, `Product`).
        model_return_type: A dictionary or TypedDict representation of the model.
        model_primary_keys: A dictionary or TypedDict representing the primary key fields and their values.
    """

    def __init__(self, model: Type[model_base], session: Session, engine: Engine):
        """
        Initializes the CRUD class.

        Args:
            model (model_base): The SQLAlchemy model class.
            session (Session): The SQLAlchemy session instance.

        Raises:
            AssertionError: If the model is invalid or missing primary key metadata.
        """
        self.model = model

        model_attr = inspect(model)
        assert model_attr

        self.primary_keys = [key.name for key in model_attr.primary_key]
        self.session = session
        self.engine = engine

    def _get_existing_instance(
        self, query: model_return_type | model_update_keys
    ) -> Tuple[model_primary_keys, Optional[model_base]]:
        """
        Retrieves an existing database instance based on primary key values.

        Args:
            query (model_return_type): A dictionary-like object representing input data.

        Returns:
            Tuple[model_primary_keys, Optional[model_base]]: A tuple containing:
                - The primary key data extracted from the query.
                - The existing model instance if found, otherwise None.

        Raises:
            AssertionError: If the query does not include all primary keys.
        """
        # Ensure the input contains all primary keys
        primary_key_data_uncasted = {
            key: query[key] for key in self.primary_keys if key in query
        }
        assert len(primary_key_data_uncasted) == len(self.primary_keys), (
            f"Query must include all primary keys: {self.primary_keys}. "
            f"Received keys: {primary_key_data_uncasted.keys()}"
        )
        primary_key_data = cast(model_primary_keys, primary_key_data_uncasted)

        # Query the database to check if an entry already exists
        existing_instance_base = (
            self.session.query(self.model).filter_by(**primary_key_data).first()
        )
        existing_instance: Optional[model_base] = (
            existing_instance_base if existing_instance_base else None
        )

        return (
            primary_key_data,
            existing_instance,
        )  # Returns None if no instance is found

    def _convert_to_model_return_type(self, instance: model_base) -> model_return_type:
        """
        Converts a SQLAlchemy model instance to the expected return type.

        Args:
            instance (model_base): The ORM model instance.

        Returns:
            model_return_type: A dictionary or TypedDict representation of the instance.

        Raises:
            AssertionError: If the instance is invalid or lacks attribute metadata.
        """
        # Make sure the instance is a valid SQLAlchemy model instance
        model_attr = inspect(instance)

        # Check that it's a valid SQLAlchemy model with a mapper
        if model_attr is None:
            raise ValueError(
                f"The provided instance is not a valid SQLAlchemy model: {instance}"
            )

        mapper = model_attr.mapper

        # Convert to a dictionary of column names and values
        return cast(
            model_return_type,
            {
                column.key: getattr(instance, column.key)
                for column in mapper.column_attrs  # Use mapper to get columns
            },
        )

    def create(self, data: model_return_type, to_commit: bool = True) -> bool:
        """
        Creates a new record in the database if it doesn't already exist.

        Args:
            data (model_return_type): A dictionary containing the fields required to create a new instance.
            to_commit (bool): Whether to immediately commit the changes to the database. Default is True.

        Returns:
            bool: True if the record was successfully created.

        Raises:
            DuplicateEntryError: If a record with the same primary keys already exists.
            KeyError: If any required primary key fields are missing from the input data.
            SQLAlchemyError: If there is a database error during the operation.
        """
        primary_key_data, existing_instance = self._get_existing_instance(data)
        if existing_instance:
            raise DuplicateEntryError(
                f"An entry with primary keys {primary_key_data} already exists.",
                primary_key_data,
            )

        # Otherwise, create a new entry
        instance = self.model(**data)
        self.session.add(instance)
        if to_commit:
            self.session.commit()
        return True

    def create_all(self, data: List[model_return_type]) -> bool:
        """
        Creates multiple records in the database.

        Args:
            data (List[model_return_type]): A list of dictionaries containing fields for new instances.

        Returns:
            bool: True if all records were successfully created.

        Raises:
            DuplicateEntryError: If any record with the same primary keys already exists.
        """
        [self.create(single_data_point, False) for single_data_point in data]
        self.session.commit()
        return True

    def read(self, filters: model_primary_keys | None) -> List[model_return_type]:
        """
        Reads records from the database based on the provided filters.

        Args:
            filters (model_primary_keys): A dictionary containing the primary key fields and their values.

        Returns:
            List[model_return_type]: A list of dictionary or TypedDict representations of the retrieved records.

        Raises:
            ValueError: If no records match the given filters.
        """
        query = self.session.query(self.model)
        if filters is None:
            return [self._convert_to_model_return_type(i) for i in query.all()]
        for key, value in filters.items():
            query = query.filter(getattr(self.model, key) == value)
        return [self._convert_to_model_return_type(i) for i in query.all()]

    def update(self, updated_data: model_return_type | model_update_keys) -> bool:
        """
        Updates a record in the database based on primary keys.

        Args:
            updated_data (model_return_type): A dictionary containing updated fields, including primary keys.

        Returns:
            bool: True if the record was successfully updated.

        Raises:
            ValueError: If no record matches the primary key filters.
        """
        primary_key_data, existing_instance = self._get_existing_instance(updated_data)
        if not existing_instance:
            raise ValueError(
                f"No {self.model.__name__} found matching filters: {primary_key_data}"
            )
            return False

        for key, value in updated_data.items():
            setattr(existing_instance, key, value)
        self.session.commit()
        return True

    def create_or_update(
        self, updated_data: model_return_type, to_commit: bool = True
    ) -> bool:
        """
        Updates a record in the database based on primary keys, Creates record if not exists

        Args:
            updated_data (model_return_type): A dictionary containing updated fields, including primary keys, and all fields

        Returns:
            bool: True if the record was created, False if updated
        """
        _, existing_instance = self._get_existing_instance(updated_data)
        # Create if not exists
        if not existing_instance:
            self.create(updated_data, to_commit)
            return True

        # Update existing_instance
        for key, value in updated_data.items():
            setattr(existing_instance, key, value)
        if to_commit:
            self.session.commit()
        return False

    def create_or_update_all(self, updated_data: List[model_return_type]) -> List[bool]:
        """
        Updates records in the database based on primary keys, Creates records if not exists

        Args:
            updated_data (model_return_type): A list of dictionaries containing updated fields, including primary keys, and all fields

        Returns:
            List[bool]: True if the record was created, False if updated
        """
        res = [self.create_or_update(data, False) for data in updated_data]
        self.session.commit()
        return res

    def delete(
        self, filters: Union[model_primary_keys, model_return_type, model_base]
    ) -> bool:
        """
        Deletes a record from the database based on filters.

        Args:
            filters (Union[model_primary_keys, model_return_type, model_base]):
                The criteria for identifying the record to delete. This can be:
                - A dictionary containing the primary key fields and their values (`model_primary_keys`).
                - A dictionary or TypedDict representation of the model (`model_return_type`).
                - An instance of the model class (`model_base`).

        Returns:
            bool: True if the record was successfully deleted.

        Raises:
            ValueError: If no record matches the given filters.
        """
        if isinstance(filters, self.model):
            # Case 1: Filters is a model instance
            primary_key_data = {key: getattr(filters, key) for key in self.primary_keys}
        elif isinstance(filters, dict):
            # Case 2: Filters is a dictionary (either model_primary_keys or model_return_type)
            primary_key_data = {
                key: filters[key] for key in self.primary_keys if key in filters
            }
            if len(primary_key_data) != len(self.primary_keys):
                raise ValueError(
                    f"Filters must include all primary keys: {self.primary_keys}. "
                    f"Received: {filters.keys()}"
                )
        else:
            raise TypeError(
                f"Invalid filters type. Expected model_primary_keys, model_return_type, or model_base. "
                f"Received: {type(filters).__name__}"
            )

        # Query the database to find the record
        existing_instance = (
            self.session.query(self.model).filter_by(**primary_key_data).first()
        )
        if not existing_instance:
            raise ValueError(
                f"No {self.model.__name__} found matching filters: {primary_key_data}"
            )

        # Delete the record
        self.session.delete(existing_instance)
        self.session.commit()
        return True
