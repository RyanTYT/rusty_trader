import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import date, datetime
from typing import List, Dict, Any
from sqlalchemy import select, func, delete
from sqlalchemy.orm import aliased
from sqlalchemy.sql import outerjoin, and_
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

# Assuming these imports based on the code structure
from app.services.models.AsyncModelsCRUD import (
    AsyncCurrentStockPositionsCRUD,
    AsyncCurrentOptionPositionsCRUD,
    AsyncTargetStockPositionsCRUD,
    AsyncTargetOptionPositionsCRUD,
    CurrentStockPositions,
    CurrentOptionPositions,
    TargetStockPositions,
    TargetOptionPositions,
    StockStrategy,
    OptionStrategy,
    QuantityRequiredStock,
    QuantityRequiredOption,
)


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


@pytest.fixture
def mock_stock_model():
    """Mock CurrentStockPositions model."""
    model = MagicMock()
    model.stock = MagicMock()
    model.strategy = MagicMock()
    model.quantity = MagicMock()
    return model


@pytest.fixture
def mock_option_model():
    """Mock CurrentOptionPositions model."""
    model = MagicMock()
    model.stock = MagicMock()
    model.strategy = MagicMock()
    model.expiry = MagicMock()
    model.strike = MagicMock()
    model.multiplier = MagicMock()
    model.option_type = MagicMock()
    model.avg_price = MagicMock()
    model.quantity = MagicMock()
    return model


@pytest.fixture
def mock_target_stock_model():
    """Mock TargetStockPositions model."""
    model = MagicMock()
    model.stock = MagicMock()
    model.strategy = MagicMock()
    model.quantity = MagicMock()
    model.avg_price = MagicMock()
    return model


@pytest.fixture
def mock_target_option_model():
    """Mock TargetOptionPositions model."""
    model = MagicMock()
    model.stock = MagicMock()
    model.strategy = MagicMock()
    model.expiry = MagicMock()
    model.strike = MagicMock()
    model.multiplier = MagicMock()
    model.option_type = MagicMock()
    model.avg_price = MagicMock()
    model.quantity = MagicMock()
    return model


@pytest_asyncio.fixture
async def current_stock_crud(mock_session, mock_engine):
    """AsyncCurrentStockPositionsCRUD fixture."""
    crud = AsyncCurrentStockPositionsCRUD(
        CurrentStockPositions, mock_session, mock_engine
    )
    return crud


@pytest_asyncio.fixture
async def current_option_crud(mock_session, mock_engine):
    """AsyncCurrentOptionPositionsCRUD fixture."""
    crud = AsyncCurrentOptionPositionsCRUD(
        CurrentOptionPositions, mock_session, mock_engine
    )
    return crud


@pytest_asyncio.fixture
def target_stock_crud(mock_session, mock_engine):
    """AsyncCurrentStockPositionsCRUD fixture."""
    crud = AsyncTargetStockPositionsCRUD(
        TargetStockPositions, mock_session, mock_engine
    )
    return crud


@pytest_asyncio.fixture
async def target_option_crud(mock_session, mock_engine):
    """AsyncCurrentOptionPositionsCRUD fixture."""
    crud = AsyncTargetOptionPositionsCRUD(
        TargetOptionPositions, mock_session, mock_engine
    )
    return crud


@pytest.fixture
def sample_stock_strategy():
    """Sample StockStrategy object."""
    strategy = MagicMock()
    strategy.strategy = "momentum_strategy"
    return strategy


@pytest.fixture
def sample_option_strategy():
    """Sample OptionStrategy object."""
    strategy = MagicMock()
    strategy.strategy = "covered_call_strategy"
    return strategy


class TestAsyncCurrentStockPositionsCRUD:
    """Test suite for AsyncCurrentStockPositionsCRUD."""

    @pytest.mark.asyncio
    async def test_get_current_positions_for_strategy_success(self, current_stock_crud):
        """Test successful retrieval of current positions for a strategy."""
        # Arrange
        strategy = "momentum_strategy"
        mock_result = MagicMock()
        mock_result.all.return_value = [
            ("AAPL", "momentum_strategy"),
            ("GOOGL", "momentum_strategy"),
            ("MSFT", "momentum_strategy"),
        ]
        current_stock_crud.session.execute.return_value = mock_result

        # Act
        result = await current_stock_crud.get_current_positions_for_strategy(strategy)

        # Assert
        assert len(result) == 3
        assert result[0] == {"stock": "AAPL", "strategy": "momentum_strategy"}
        assert result[1] == {"stock": "GOOGL", "strategy": "momentum_strategy"}
        assert result[2] == {"stock": "MSFT", "strategy": "momentum_strategy"}
        current_stock_crud.session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_current_positions_for_strategy_empty_result(
        self, current_stock_crud
    ):
        """Test retrieval of current positions when no positions exist."""
        # Arrange
        strategy = "nonexistent_strategy"
        mock_result = MagicMock()
        mock_result.all.return_value = []
        current_stock_crud.session.execute.return_value = mock_result

        # Act
        result = await current_stock_crud.get_current_positions_for_strategy(strategy)

        # Assert
        assert result == []
        current_stock_crud.session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_current_positions_overall_success(self, current_stock_crud):
        """Test successful retrieval of overall current positions."""
        # Arrange
        mock_result = MagicMock()
        mock_result.all.return_value = [("AAPL", 100), ("GOOGL", 50), ("MSFT", 75)]
        current_stock_crud.session.execute.return_value = mock_result

        # Act
        result = await current_stock_crud.get_current_positions_overall()

        # Assert
        assert result == {"AAPL": 100, "GOOGL": 50, "MSFT": 75}
        current_stock_crud.session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_current_positions_overall_empty_result(self, current_stock_crud):
        """Test retrieval of overall positions when no positions exist."""
        # Arrange
        mock_result = MagicMock()
        mock_result.all.return_value = []
        current_stock_crud.session.execute.return_value = mock_result

        # Act
        result = await current_stock_crud.get_current_positions_overall()

        # Assert
        assert result == {}
        current_stock_crud.session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_current_positions_overall_with_decimals(
        self, current_stock_crud
    ):
        """Test overall positions conversion from decimal to int."""
        # Arrange
        mock_result = MagicMock()
        mock_result.all.return_value = [
            ("AAPL", 100.0),
            ("GOOGL", 50.5),  # This should be converted to int
        ]
        current_stock_crud.session.execute.return_value = mock_result

        # Act
        result = await current_stock_crud.get_current_positions_overall()

        # Assert
        assert result == {"AAPL": 100, "GOOGL": 50}
        current_stock_crud.session.execute.assert_called_once()


class TestAsyncCurrentOptionPositionsCRUD:
    """Test suite for AsyncCurrentOptionPositionsCRUD."""

    @pytest.mark.asyncio
    async def test_get_current_positions_for_strategy_success(
        self, current_option_crud
    ):
        """Test successful retrieval of current option positions for a strategy."""
        # Arrange
        strategy = "covered_call_strategy"
        mock_result = MagicMock()
        mock_result.all.return_value = [
            (
                "AAPL",
                "covered_call_strategy",
                date(2024, 12, 15),
                150.0,
                100,
                "CALL",
                5.25,
                2,
            ),
            (
                "GOOGL",
                "covered_call_strategy",
                date(2024, 11, 20),
                2800.0,
                100,
                "PUT",
                45.50,
                1,
            ),
        ]
        current_option_crud.session.execute.return_value = mock_result

        # Act
        result = await current_option_crud.get_current_positions_for_strategy(strategy)

        # Assert
        assert len(result) == 2
        expected_first = {
            "stock": "AAPL",
            "strategy": "covered_call_strategy",
            "expiry": date(2024, 12, 15),
            "strike": 150.0,
            "multiplier": 100,
            "option_type": "CALL",
            "avg_price": 5.25,
            "quantity": 2,
        }
        expected_second = {
            "stock": "GOOGL",
            "strategy": "covered_call_strategy",
            "expiry": date(2024, 11, 20),
            "strike": 2800.0,
            "multiplier": 100,
            "option_type": "PUT",
            "avg_price": 45.50,
            "quantity": 1,
        }
        assert result[0] == expected_first
        assert result[1] == expected_second
        current_option_crud.session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_current_positions_for_strategy_empty_result(
        self, current_option_crud
    ):
        """Test retrieval of current option positions when no positions exist."""
        # Arrange
        strategy = "nonexistent_strategy"
        mock_result = MagicMock()
        mock_result.all.return_value = []
        current_option_crud.session.execute.return_value = mock_result

        # Act
        result = await current_option_crud.get_current_positions_for_strategy(strategy)

        # Assert
        assert result == []
        current_option_crud.session.execute.assert_called_once()


class TestAsyncTargetStockPositionsCRUD:
    """Test suite for AsyncTargetStockPositionsCRUD."""

    @pytest.mark.asyncio
    async def test_clear_positions_success(self, target_stock_crud):
        """Test successful clearing of positions."""
        # Arrange
        strategy = "momentum_strategy"
        stock = "AAPL"

        # Act
        await target_stock_crud.clear_positions(strategy, stock)

        # Assert
        target_stock_crud.session.execute.assert_called_once()
        target_stock_crud.session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_clear_positions_database_error(self, target_stock_crud):
        """Test handling of database error during position clearing."""
        # Arrange
        strategy = "momentum_strategy"
        stock = "AAPL"
        target_stock_crud.session.execute.side_effect = Exception("Database error")

        # Act & Assert
        with pytest.raises(Exception, match="Database error"):
            await target_stock_crud.clear_positions(strategy, stock)

    @pytest.mark.asyncio
    async def test_get_order_quantities_required_success(
        self, target_stock_crud, sample_stock_strategy
    ):
        """Test successful retrieval of order quantities required."""
        # Arrange
        mock_result = MagicMock()
        mock_result.all.return_value = [
            ("AAPL", "momentum_strategy", 50, 100, 150.25),  # Need to buy 50 more
            ("GOOGL", "momentum_strategy", -25, 75, 2800.50),  # Need to sell 25
            ("MSFT", "momentum_strategy", 0, 50, 300.75),  # No change needed
        ]
        target_stock_crud.session.execute.return_value = mock_result

        # Act
        result = await target_stock_crud.get_order_quantities_required(
            sample_stock_strategy
        )

        # Assert
        assert len(result) == 3
        expected_results = [
            {
                "stock": "AAPL",
                "strategy": "momentum_strategy",
                "quantity_difference": 50,
                "quantity": 100,
                "avg_price": 150.25,
            },
            {
                "stock": "GOOGL",
                "strategy": "momentum_strategy",
                "quantity_difference": -25,
                "quantity": 75,
                "avg_price": 2800.50,
            },
            {
                "stock": "MSFT",
                "strategy": "momentum_strategy",
                "quantity_difference": 0,
                "quantity": 50,
                "avg_price": 300.75,
            },
        ]
        assert result == expected_results
        target_stock_crud.session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_order_quantities_required_empty_result(
        self, target_stock_crud, sample_stock_strategy
    ):
        """Test retrieval of order quantities when no positions exist."""
        # Arrange
        mock_result = MagicMock()
        mock_result.all.return_value = []
        target_stock_crud.session.execute.return_value = mock_result

        # Act
        result = await target_stock_crud.get_order_quantities_required(
            sample_stock_strategy
        )

        # Assert
        assert result == []
        target_stock_crud.session.execute.assert_called_once()


class TestAsyncTargetOptionPositionsCRUD:
    """Test suite for AsyncTargetOptionPositionsCRUD."""

    @pytest.mark.asyncio
    async def test_clear_positions_success(self, target_option_crud):
        """Test successful clearing of option positions."""
        # Arrange
        strategy = "covered_call_strategy"
        stock = "AAPL"

        # Act
        await target_option_crud.clear_positions(strategy, stock)

        # Assert
        target_option_crud.session.execute.assert_called_once()
        target_option_crud.session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_clear_positions_database_error(self, target_option_crud):
        """Test handling of database error during option position clearing."""
        # Arrange
        strategy = "covered_call_strategy"
        stock = "AAPL"
        target_option_crud.session.execute.side_effect = Exception("Database error")

        # Act & Assert
        with pytest.raises(Exception, match="Database error"):
            await target_option_crud.clear_positions(strategy, stock)

    @pytest.mark.asyncio
    async def test_get_order_quantities_required_success(
        self, target_option_crud, sample_option_strategy
    ):
        """Test successful retrieval of option order quantities required."""
        # Arrange
        mock_result = MagicMock()
        mock_result.all.return_value = [
            (
                "AAPL",
                "covered_call_strategy",
                date(2024, 12, 15),
                150.0,
                100,
                "CALL",
                2,
                5,
                5.25,
            ),
            (
                "GOOGL",
                "covered_call_strategy",
                date(2024, 11, 20),
                2800.0,
                100,
                "PUT",
                -1,
                3,
                45.50,
            ),
            (
                "MSFT",
                "covered_call_strategy",
                date(2024, 10, 18),
                300.0,
                100,
                "CALL",
                0,
                2,
                8.75,
            ),
        ]
        target_option_crud.session.execute.return_value = mock_result

        # Act
        result = await target_option_crud.get_order_quantities_required(
            sample_option_strategy
        )

        # Assert
        assert len(result) == 3
        expected_results = [
            {
                "stock": "AAPL",
                "strategy": "covered_call_strategy",
                "expiry": date(2024, 12, 15),
                "strike": 150.0,
                "multiplier": 100,
                "option_type": "CALL",
                "quantity_difference": 2,
                "quantity": 5,
                "avg_price": 5.25,
            },
            {
                "stock": "GOOGL",
                "strategy": "covered_call_strategy",
                "expiry": date(2024, 11, 20),
                "strike": 2800.0,
                "multiplier": 100,
                "option_type": "PUT",
                "quantity_difference": -1,
                "quantity": 3,
                "avg_price": 45.50,
            },
            {
                "stock": "MSFT",
                "strategy": "covered_call_strategy",
                "expiry": date(2024, 10, 18),
                "strike": 300.0,
                "multiplier": 100,
                "option_type": "CALL",
                "quantity_difference": 0,
                "quantity": 2,
                "avg_price": 8.75,
            },
        ]
        assert result == expected_results
        target_option_crud.session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_order_quantities_required_empty_result(
        self, target_option_crud, sample_option_strategy
    ):
        """Test retrieval of option order quantities when no positions exist."""
        # Arrange
        mock_result = MagicMock()
        mock_result.all.return_value = []
        target_option_crud.session.execute.return_value = mock_result

        # Act
        result = await target_option_crud.get_order_quantities_required(
            sample_option_strategy
        )

        # Assert
        assert result == []
        target_option_crud.session.execute.assert_called_once()


class TestIntegrationScenarios:
    """Integration tests for common workflows."""

    @pytest.mark.asyncio
    async def test_stock_position_workflow(
        self, current_stock_crud, target_stock_crud, sample_stock_strategy
    ):
        """Test complete workflow for stock positions."""
        # Test getting current positions
        mock_current_result = MagicMock()
        mock_current_result.all.return_value = [("AAPL", "momentum_strategy")]
        current_stock_crud.session.execute.return_value = mock_current_result

        current_positions = await current_stock_crud.get_current_positions_for_strategy(
            "momentum_strategy"
        )
        assert len(current_positions) == 1

        # Test clearing positions
        await target_stock_crud.clear_positions("momentum_strategy", "AAPL")
        target_stock_crud.session.execute.assert_called()
        target_stock_crud.session.commit.assert_called()

        # Test getting order quantities
        mock_target_result = MagicMock()
        mock_target_result.all.return_value = [
            ("AAPL", "momentum_strategy", 50, 100, 150.25)
        ]
        target_stock_crud.session.execute.return_value = mock_target_result

        order_quantities = await target_stock_crud.get_order_quantities_required(
            sample_stock_strategy
        )
        assert len(order_quantities) == 1
        assert order_quantities[0]["quantity_difference"] == 50

    @pytest.mark.asyncio
    async def test_option_position_workflow(
        self, current_option_crud, target_option_crud, sample_option_strategy
    ):
        """Test complete workflow for option positions."""
        # Test getting current option positions
        mock_current_result = MagicMock()
        mock_current_result.all.return_value = [
            (
                "AAPL",
                "covered_call_strategy",
                date(2024, 12, 15),
                150.0,
                100,
                "CALL",
                5.25,
                2,
            )
        ]
        current_option_crud.session.execute.return_value = mock_current_result

        current_positions = (
            await current_option_crud.get_current_positions_for_strategy(
                "covered_call_strategy"
            )
        )
        assert len(current_positions) == 1
        assert current_positions[0]["option_type"] == "CALL"

        # Test clearing option positions
        await target_option_crud.clear_positions("covered_call_strategy", "AAPL")
        target_option_crud.session.execute.assert_called()
        target_option_crud.session.commit.assert_called()

        # Test getting option order quantities
        mock_target_result = MagicMock()
        mock_target_result.all.return_value = [
            (
                "AAPL",
                "covered_call_strategy",
                date(2024, 12, 15),
                150.0,
                100,
                "CALL",
                1,
                3,
                5.25,
            )
        ]
        target_option_crud.session.execute.return_value = mock_target_result

        order_quantities = await target_option_crud.get_order_quantities_required(
            sample_option_strategy
        )
        assert len(order_quantities) == 1
        assert order_quantities[0]["quantity_difference"] == 1


class TestErrorHandling:
    """Test error handling scenarios."""

    @pytest.mark.asyncio
    async def test_database_connection_error(self, current_stock_crud):
        """Test handling of database connection errors."""
        # Arrange
        current_stock_crud.session.execute.side_effect = Exception("Connection failed")

        # Act & Assert
        with pytest.raises(Exception, match="Connection failed"):
            await current_stock_crud.get_current_positions_for_strategy("test_strategy")

    @pytest.mark.asyncio
    async def test_commit_error_during_clear(self, target_stock_crud):
        """Test handling of commit errors during position clearing."""
        # Arrange
        target_stock_crud.session.commit.side_effect = Exception("Commit failed")

        # Act & Assert
        with pytest.raises(Exception, match="Commit failed"):
            await target_stock_crud.clear_positions("test_strategy", "AAPL")

    @pytest.mark.asyncio
    async def test_invalid_strategy_object(self, target_stock_crud):
        """Test handling of invalid strategy objects."""
        # Arrange
        invalid_strategy = None

        # Act & Assert
        with pytest.raises(AttributeError):
            await target_stock_crud.get_order_quantities_required(invalid_strategy)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
