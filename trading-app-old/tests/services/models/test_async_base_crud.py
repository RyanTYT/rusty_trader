from typing_extensions import NotRequired
import pytest
from app.utils import custom_logging
from app.utils.error_handling import DuplicateEntryError
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Dict, Any, List, Optional, TypedDict
from sqlalchemy import Column, Integer, String, DateTime, select
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine, create_async_engine
from sqlalchemy.orm import declarative_base, Mapped, mapped_column
from sqlalchemy.exc import NoInspectionAvailable, SQLAlchemyError
from datetime import datetime, timezone
from app.services.models.AsyncBaseCRUD import AsyncCRUD
import re

# Test model definitions
Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(50))
    email: Mapped[str] = mapped_column(String(100))
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.now(timezone.utc)
    )


class UserProfile(Base):
    __tablename__ = "user_profiles"

    user_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    profile_type: Mapped[str] = mapped_column(
        String(20), primary_key=True
    )  # Composite primary key
    bio: Mapped[str] = mapped_column(String(500))
    website: Mapped[str] = mapped_column(String(100))


class UserPrimaryKeys(TypedDict):
    id: int


class UserUpdateKeys(UserPrimaryKeys):
    name: NotRequired[str]
    email: NotRequired[str]
    created_at: NotRequired[datetime]


class UserDict(UserPrimaryKeys):
    name: str
    email: str
    created_at: datetime


class UserProfilePrimaryKeys(TypedDict):
    user_id: int
    profile: str


class UserProfileUpdateKeys(UserProfilePrimaryKeys):
    bio: NotRequired[str]
    website: NotRequired[str]


class UserProfileDict(UserProfilePrimaryKeys):
    bio: str
    website: str


@pytest_asyncio.fixture
async def mock_engine():
    """Mock async engine for testing"""
    engine = AsyncMock(spec=AsyncEngine)
    return engine


@pytest_asyncio.fixture
async def mock_session():
    """Mock async session for testing"""
    session = AsyncMock(spec=AsyncSession)
    return session


@pytest_asyncio.fixture
async def user_crud(mock_session, mock_engine):
    """AsyncCRUD instance for User model"""
    return AsyncCRUD[User, UserDict, UserUpdateKeys, UserPrimaryKeys](
        model=User, session=mock_session, engine=mock_engine
    )


@pytest_asyncio.fixture
async def user_profile_crud(mock_session, mock_engine):
    """AsyncCRUD instance for UserProfile model (composite primary key)"""
    return AsyncCRUD[
        UserProfile, UserProfileDict, UserProfileUpdateKeys, UserProfilePrimaryKeys
    ](model=UserProfile, session=mock_session, engine=mock_engine)


@pytest_asyncio.fixture
def sample_user_data():
    """Sample user data for testing"""
    return {
        "id": 1,
        "name": "John Doe",
        "email": "john@example.com",
        "created_at": datetime(2023, 1, 1),
    }


@pytest_asyncio.fixture
def sample_user_profile_data():
    """Sample user profile data for testing (composite key)"""
    return {
        "user_id": 1,
        "profile_type": "public",
        "bio": "Software Developer",
        "website": "https://johndoe.com",
    }


class TestAsyncCRUDInitialization:
    """Test AsyncCRUD initialization and setup"""

    def test_init_with_single_primary_key(self, mock_session, mock_engine):
        """Test initialization with single primary key model"""
        crud = AsyncCRUD(User, mock_session, mock_engine)
        assert crud.model is User
        assert crud.session == mock_session
        assert crud.engine == mock_engine
        assert crud.primary_keys == ["id"]

    def test_init_with_composite_primary_key(self, mock_session, mock_engine):
        """Test initialization with composite primary key model"""
        crud = AsyncCRUD(UserProfile, mock_session, mock_engine)
        assert crud.model is UserProfile
        assert crud.primary_keys == ["user_id", "profile_type"]


class TestGetExistingInstance:
    """Test _get_existing_instance method"""

    @pytest.mark.asyncio
    async def test_get_existing_instance_found(
        self, user_crud, mock_session, sample_user_data
    ):
        """Test retrieving an existing instance"""
        mock_user = User(**sample_user_data)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_user
        mock_session.execute.return_value = mock_result

        primary_keys, instance = await user_crud._get_existing_instance(
            sample_user_data
        )

        assert primary_keys == {"id": 1}
        assert instance == mock_user
        mock_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_existing_instance_not_found(
        self, user_crud, mock_session, sample_user_data
    ):
        """Test retrieving non-existent instance"""
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result

        primary_keys, instance = await user_crud._get_existing_instance(
            sample_user_data
        )

        assert primary_keys == {"id": 1}
        assert instance is None

    @pytest.mark.asyncio
    async def test_get_existing_instance_missing_primary_keys(self, user_crud):
        """Test error when primary keys are missing"""
        incomplete_data = {"name": "John", "email": "john@example.com"}

        with pytest.raises(AssertionError, match="Query must include all primary keys"):
            await user_crud._get_existing_instance(incomplete_data)

    @pytest.mark.asyncio
    async def test_get_existing_instance_composite_key(
        self, user_profile_crud, mock_session, sample_user_profile_data
    ):
        """Test retrieving instance with composite primary key"""
        mock_profile = UserProfile(**sample_user_profile_data)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_profile
        mock_session.execute.return_value = mock_result

        primary_keys, instance = await user_profile_crud._get_existing_instance(
            sample_user_profile_data
        )

        assert primary_keys == {"user_id": 1, "profile_type": "public"}
        assert instance == mock_profile


class TestConvertToModelReturnType:
    """Test _convert_to_model_return_type method"""

    def test_convert_valid_instance(self, user_crud, sample_user_data):
        """Test converting valid SQLAlchemy instance to dict"""
        mock_user = User(**sample_user_data)

        result = user_crud._convert_to_model_return_type(mock_user)

        expected = {
            "id": 1,
            "name": "John Doe",
            "email": "john@example.com",
            "created_at": datetime(2023, 1, 1),
        }
        assert result == expected

    def test_convert_invalid_instance(self, user_crud):
        """Test error when converting invalid instance"""
        invalid_instance = "not a model instance"

        # with patch("sqlalchemy.inspect", return_value=None):
        #     with pytest.raises(ValueError, match="not a valid SQLAlchemy model"):
        #         user_crud._convert_to_model_return_type(invalid_instance)
        with pytest.raises(NoInspectionAvailable):
            user_crud._convert_to_model_return_type(invalid_instance)


class TestCreate:
    """Test create method"""

    @pytest.mark.asyncio
    async def test_create_success(self, user_crud, mock_session, sample_user_data):
        """Test successful record creation"""
        # Mock that no existing instance is found
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result

        result = await user_crud.create(sample_user_data)

        assert result is True
        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_without_commit(
        self, user_crud, mock_session, sample_user_data
    ):
        """Test record creation without immediate commit"""
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result

        result = await user_crud.create(sample_user_data, to_commit=False)

        assert result is True
        mock_session.add.assert_called_once()
        mock_session.commit.assert_not_called()

    @pytest.mark.asyncio
    async def test_create_duplicate_entry(
        self, user_crud, mock_session, sample_user_data, caplog
    ):
        """Test creation with duplicate primary key"""
        mock_user = User(**sample_user_data)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_user
        mock_session.execute.return_value = mock_result

        with pytest.raises(
            DuplicateEntryError,
            match=re.escape(
                "An entry with primary keys {'id': 1} already exists. (Primary Keys: {'id': 1})"
            ),
        ) as exc_info:
            await user_crud.create(sample_user_data)

        assert exc_info.value.primary_keys == {"id": 1}
        mock_session.add.assert_not_called()


class TestCreateAll:
    """Test create_all method"""

    @pytest.mark.asyncio
    async def test_create_all_success(self, user_crud, mock_session):
        """Test successful creation of multiple records"""
        data_list = [
            {"id": 1, "name": "John", "email": "john@example.com"},
            {"id": 2, "name": "Jane", "email": "jane@example.com"},
        ]

        # Mock that no existing instances are found
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result

        result = await user_crud.create_all(data_list)

        assert result is True
        assert mock_session.add.call_count == 2
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_all_with_duplicate(self, user_crud, mock_session):
        """Test create_all with duplicate entry"""
        data_list = [
            {"id": 1, "name": "John", "email": "john@example.com"},
            {"id": 1, "name": "Jane", "email": "jane@example.com"},  # Duplicate ID
        ]

        # First call returns None, second returns existing user
        mock_user = User(id=1, name="Existing", email="existing@example.com")
        mock_result_none = MagicMock()
        mock_result_none.scalar_one_or_none.return_value = None
        mock_result_duplicate = MagicMock()
        mock_result_duplicate.scalar_one_or_none.return_value = mock_user

        mock_session.execute.side_effect = [mock_result_none, mock_result_duplicate]

        with pytest.raises(DuplicateEntryError):
            await user_crud.create_all(data_list)


class TestRead:
    """Test read method"""

    @pytest.mark.asyncio
    async def test_read_with_filters(self, user_crud, mock_session, sample_user_data):
        """Test reading records with filters"""
        mock_user = User(**sample_user_data)
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [mock_user]
        mock_session.execute.return_value = mock_result

        # Mock the conversion method
        with patch.object(
            user_crud, "_convert_to_model_return_type", return_value=sample_user_data
        ):
            result = await user_crud.read({"id": 1})

            assert result == [sample_user_data]
            mock_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_read_without_filters(
        self, user_crud, mock_session, sample_user_data
    ):
        """Test reading all records without filters"""
        mock_user = User(**sample_user_data)
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [mock_user]
        mock_session.execute.return_value = mock_result

        with patch.object(
            user_crud, "_convert_to_model_return_type", return_value=sample_user_data
        ):
            result = await user_crud.read(None)

            assert result == [sample_user_data]

    @pytest.mark.asyncio
    async def test_read_no_results(self, user_crud, mock_session):
        """Test reading with no matching records"""
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute.return_value = mock_result

        result = await user_crud.read({"id": 999})

        assert result == []


class TestUpdate:
    """Test update method"""

    @pytest.mark.asyncio
    async def test_update_success(self, user_crud, mock_session, sample_user_data):
        """Test successful record update"""
        mock_user = User(**sample_user_data)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_user
        mock_session.execute.return_value = mock_result

        update_data = {"id": 1, "name": "Updated Name", "email": "updated@example.com"}
        result = await user_crud.update(update_data)

        assert result is True
        assert mock_user.name == "Updated Name"
        assert mock_user.email == "updated@example.com"
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_not_found(self, user_crud, mock_session):
        """Test update with non-existent record"""
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result

        update_data = {"id": 999, "name": "Updated Name"}

        with pytest.raises(ValueError, match="No User found matching filters"):
            await user_crud.update(update_data)


class TestCreateOrUpdate:
    """Test create_or_update method"""

    @pytest.mark.asyncio
    async def test_create_or_update_creates_new(
        self, user_crud, mock_session, sample_user_data
    ):
        """Test create_or_update when record doesn't exist (creates new)"""
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result

        result = await user_crud.create_or_update(sample_user_data)

        assert result is True  # True means created
        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_or_update_updates_existing(
        self, user_crud, mock_session, sample_user_data
    ):
        """Test create_or_update when record exists (updates)"""
        mock_user = User(**sample_user_data)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_user
        mock_session.execute.return_value = mock_result

        update_data = {"id": 1, "name": "Updated Name", "email": "updated@example.com"}
        result = await user_crud.create_or_update(update_data)

        assert result is False  # False means updated
        assert mock_user.name == "Updated Name"
        mock_session.add.assert_not_called()
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_or_update_without_commit(
        self, user_crud, mock_session, sample_user_data
    ):
        """Test create_or_update without immediate commit"""
        mock_user = User(**sample_user_data)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_user
        mock_session.execute.return_value = mock_result

        result = await user_crud.create_or_update(sample_user_data, to_commit=False)

        assert result is False
        mock_session.commit.assert_not_called()


class TestCreateOrUpdateAll:
    """Test create_or_update_all method"""

    @pytest.mark.asyncio
    async def test_create_or_update_all_mixed(self, user_crud, mock_session):
        """Test create_or_update_all with mixed create/update operations"""
        data_list = [
            {"id": 1, "name": "John", "email": "john@example.com"},
            {"id": 2, "name": "Jane", "email": "jane@example.com"},
        ]

        # First record exists, second doesn't
        mock_user = User(id=1, name="Existing", email="existing@example.com")
        # 1st call: returns user so goes to update - done
        # 2nd call: returns no user -> goes to third call
        # 3rd call: call to create user in database
        mock_results = [
            MagicMock(scalar_one_or_none=MagicMock(return_value=mock_user)),  # Exists
            MagicMock(scalar_one_or_none=MagicMock(return_value=None)),  # Doesn't exist
            MagicMock(scalar_one_or_none=MagicMock(return_value=None)),  # Doesn't exist
        ]
        mock_session.execute.side_effect = mock_results

        results = await user_crud.create_or_update_all(data_list)

        assert results == [False, True]  # Updated first, created second
        mock_session.commit.assert_called_once()


class TestDelete:
    """Test delete method"""

    @pytest.mark.asyncio
    async def test_delete_with_dict_filters(
        self, user_crud, mock_session, sample_user_data
    ):
        """Test successful deletion with dictionary filters"""
        mock_user = User(**sample_user_data)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_user
        mock_session.execute.return_value = mock_result

        result = await user_crud.delete({"id": 1})

        assert result is True
        mock_session.delete.assert_called_once_with(mock_user)
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_with_model_instance(
        self, user_crud, mock_session, sample_user_data
    ):
        """Test deletion with model instance"""
        mock_user = User(**sample_user_data)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_user
        mock_session.execute.return_value = mock_result

        result = await user_crud.delete(mock_user)

        assert result is True
        mock_session.delete.assert_called_once_with(mock_user)

    @pytest.mark.asyncio
    async def test_delete_not_found(self, user_crud, mock_session):
        """Test deletion with non-existent record"""
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result

        with pytest.raises(ValueError, match="No User found matching filters"):
            await user_crud.delete({"id": 999})

    @pytest.mark.asyncio
    async def test_delete_incomplete_primary_keys(self, user_profile_crud):
        """Test deletion with incomplete primary key data"""
        incomplete_filters = {"user_id": 1}  # Missing profile_type

        with pytest.raises(ValueError, match="Filters must include all primary keys"):
            await user_profile_crud.delete(incomplete_filters)

    @pytest.mark.asyncio
    async def test_delete_invalid_filter_type(self, user_crud):
        """Test deletion with invalid filter type"""
        invalid_filters = "invalid_type"

        with pytest.raises(TypeError, match="Invalid filters type"):
            await user_crud.delete(invalid_filters)


class TestErrorHandling:
    """Test error handling scenarios"""

    @pytest.mark.asyncio
    async def test_database_error_during_create(
        self, user_crud, mock_session, sample_user_data
    ):
        """Test handling of database errors during create"""
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        mock_session.commit.side_effect = SQLAlchemyError("Database error")

        with pytest.raises(SQLAlchemyError):
            await user_crud.create(sample_user_data)

    @pytest.mark.asyncio
    async def test_database_error_during_read(self, user_crud, mock_session):
        """Test handling of database errors during read"""
        mock_session.execute.side_effect = SQLAlchemyError("Database error")

        with pytest.raises(SQLAlchemyError):
            await user_crud.read({"id": 1})


class TestCompositeKeys:
    """Test operations with composite primary keys"""

    @pytest.mark.asyncio
    async def test_composite_key_create(
        self, user_profile_crud, mock_session, sample_user_profile_data
    ):
        """Test create with composite primary key"""
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result

        result = await user_profile_crud.create(sample_user_profile_data)

        assert result is True
        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_composite_key_update(
        self, user_profile_crud, mock_session, sample_user_profile_data
    ):
        """Test update with composite primary key"""
        mock_profile = UserProfile(**sample_user_profile_data)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_profile
        mock_session.execute.return_value = mock_result

        update_data = {
            "user_id": 1,
            "profile_type": "public",
            "bio": "Updated bio",
            "website": "https://updated.com",
        }

        result = await user_profile_crud.update(update_data)

        assert result is True
        assert mock_profile.bio == "Updated bio"
        assert mock_profile.website == "https://updated.com"


class TestEdgeCases:
    """Test edge cases and boundary conditions"""

    @pytest.mark.asyncio
    async def test_empty_data_list(self, user_crud, mock_session):
        """Test create_all with empty data list"""
        result = await user_crud.create_all([])

        assert result is True
        mock_session.add.assert_not_called()
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_read_with_empty_filters(self, user_crud, mock_session):
        """Test read with empty filter dictionary"""
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute.return_value = mock_result

        result = await user_crud.read({})

        assert result == []

    @pytest.mark.asyncio
    async def test_update_with_partial_data(
        self, user_crud, mock_session, sample_user_data
    ):
        """Test update with only some fields"""
        mock_user = User(**sample_user_data)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_user
        mock_session.execute.return_value = mock_result

        # Only update name, keep other fields
        partial_update = {"id": 1, "name": "Updated Name Only"}
        result = await user_crud.update(partial_update)

        assert result is True
        assert mock_user.name == "Updated Name Only"
        # Other fields should remain unchanged
        assert mock_user.email == "john@example.com"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
