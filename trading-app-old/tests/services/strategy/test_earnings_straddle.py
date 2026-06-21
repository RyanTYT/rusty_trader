from typing import cast
from ib_async.contract import Contract, Option, Stock
from ib_async.objects import BarData
import pytz
import asyncio
from datetime import datetime
import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.future import select
from app.models_types import CurrentOptionPositionsDict, OptionType, Status
from app.services.models.AsyncModelsCRUD import (
    AsyncCurrentOptionPositionsCRUD,
    AsyncCurrentStockPositionsCRUD,
    AsyncHistoricalDataCRUD,
    AsyncHistoricalOptionsDataCRUD,
    AsyncStrategyCRUD,
    AsyncTargetOptionPositionsCRUD,
    AsyncTargetStockPositionsCRUD,
)
import pytest_asyncio
from unittest.mock import Mock, AsyncMock, patch, MagicMock
import importlib
import json

# from app.services.strategy.EarningsStraddle import EarningsStraddle
# from app.services.strategy.Noise import Noise


# class TestStrategyAbstractMethods:
#     """Test that concrete strategies properly implement abstract methods."""
#
#     def test_covered_call_implements_all_methods(self):
#         """Test that CoveredCallStrategy implements all abstract methods."""
#         strategy = EarningsStraddle
#
#         # Check that all abstract methods are implemented
#         abstract_methods = EarningsStraddle.__abstractmethods__
#         for method_name in abstract_methods:
#             assert hasattr(strategy, method_name)
#             assert callable(getattr(strategy, method_name))
#
#     def test_iron_condor_implements_all_methods(self):
#         """Test that IronCondorStrategy implements all abstract methods."""
#         strategy = Noise
#
#         # Check that all abstract methods are implemented
#         abstract_methods = Noise.__abstractmethods__
#         for method_name in abstract_methods:
#             assert hasattr(strategy, method_name)
#             assert callable(getattr(strategy, method_name))
@pytest_asyncio.fixture
def mock_async_session():
    """Fixture that mocks an async DB session."""
    # mock_session = AsyncMock()
    # mock_session.return_value = AsyncSession
    # Create mock session object
    mock_session = AsyncMock()

    # Create mock context manager for AsyncSessionLocal()
    mock_async_session_local = AsyncMock()
    mock_async_session_local.__aenter__.return_value = mock_session
    mock_async_session_local.__aexit__.return_value = None  # usually safe
    mock_async_session_local.execute = AsyncMock(
        return_value=MagicMock()
    )  # usually safe
    # yield mock_async_session_local
    return mock_session, mock_async_session_local


@pytest.fixture
def mock_async_engine():
    mock_engine = AsyncMock(spec=AsyncEngine)

    mock_conn_cm = AsyncMock()
    mock_engine.connect.return_value = mock_conn_cm
    mock_conn_cm.__aenter__.return_value = mock_engine  # or mock connection
    mock_conn_cm.__aexit__.return_value = None

    return mock_engine


@pytest.fixture(scope="function", autouse=False)
def mock_async_with_db_session():
    with patch(
        "app.utils.db.async_with_db_session_for_model"
    ) as mock_strategy_wrapper_removed:
        mock_strategy_crud = AsyncMock()
        mock_historical_data_crud = AsyncMock()
        mock_historical_option_data_crud = AsyncMock()
        mock_current_stock_position_crud = AsyncMock()
        mock_target_stock_position_crud = AsyncMock()
        mock_current_option_position_crud = AsyncMock()
        mock_target_option_position_crud = AsyncMock()

        def overall_wrapper(modelCRUD, _):
            model_crud = None
            if modelCRUD == AsyncStrategyCRUD:
                model_crud = mock_strategy_crud
            elif modelCRUD == AsyncHistoricalDataCRUD:
                model_crud = mock_historical_data_crud
            elif modelCRUD == AsyncHistoricalOptionsDataCRUD:
                model_crud = mock_historical_option_data_crud
            elif modelCRUD == AsyncCurrentStockPositionsCRUD:
                model_crud = mock_current_stock_position_crud
            elif modelCRUD == AsyncCurrentOptionPositionsCRUD:
                model_crud = mock_current_option_position_crud
            elif modelCRUD == AsyncTargetStockPositionsCRUD:
                model_crud = mock_target_stock_position_crud
            elif modelCRUD == AsyncTargetOptionPositionsCRUD:
                model_crud = mock_target_option_position_crud

            def wrapper_side_effect(func):
                async def inner(*args, **kwargs):
                    return await func(model_crud, *args, **kwargs)

                return inner

            return wrapper_side_effect

        # mock_strategy_wrapper = MagicMock()
        # mock_strategy_wrapper.side_effect = wrapper_side_effect
        # mock_strategy_wrapper.return_value = wrapper_side_effect
        mock_strategy_wrapper_removed.return_value = overall_wrapper
        mock_strategy_wrapper_removed.side_effect = overall_wrapper

        yield {
            "strategy": mock_strategy_crud,
            "historical_data": mock_historical_data_crud,
            "historical_option_data": mock_historical_option_data_crud,
            "current_stock_position": mock_current_stock_position_crud,
            "current_option_position": mock_current_option_position_crud,
            "target_stock_position": mock_target_stock_position_crud,
            "target_option_position": mock_target_option_position_crud,
        }


class MockResponse:
    def __init__(self, text, status):
        self._text = text
        self.status = status

    async def text(self):
        return self._text

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def __aenter__(self):
        return self

    async def json(self):
        return self._text


class TestEarningsStraddleMethods:
    """Test async methods with mocked database operations."""

    @pytest.mark.asyncio
    async def test_create_strategy_new(self, mock_async_with_db_session):
        """Test creating a new strategy when none exists."""

        # mock_session, mock_context_manager = mock_async_session
        # with patch("app.utils.db.async_engine", mock_async_engine):
        #     with patch(
        #         "app.utils.db.AsyncSessionLocal",
        #         return_value=mock_context_manager,
        #     ):

        mock_strategy_crud = mock_async_with_db_session["strategy"]

        mock_strategy_crud.create = AsyncMock(return_value=True)
        mock_strategy_crud.read = AsyncMock(return_value=[])

        import app.services.strategy.EarningsStraddle as es_module

        importlib.reload(es_module)
        EarningsStraddle = es_module.EarningsStraddle

        # Call the method
        await EarningsStraddle.create_strategy()

        # assert mock_select.call_count == 2
        # assert mock_select_stmt.filter.call_count == 1
        # assert mock_select_stmt.filter_by.call_count == 1
        mock_strategy_crud.read.assert_called_once()
        mock_strategy_crud.create.assert_called_once()

        # Verify strategy was created
        create_call_args = mock_strategy_crud.create.call_args[0][0]
        assert create_call_args["strategy"] == "earnings_straddle"
        assert create_call_args["capital"] == 100000
        assert create_call_args["initial_capital"] == 100000
        assert create_call_args["status"] == Status.active

    @pytest.mark.asyncio
    async def test_create_strategy_existing(self, mock_async_with_db_session):
        """Test behavior when strategy already exists."""
        mock_strategy_crud = mock_async_with_db_session["strategy"]

        mock_strategy_crud.create = AsyncMock(return_value=True)
        mock_strategy_crud.read = AsyncMock(
            return_value=[{"strategy": "earnings_straddle"}]
        )

        import app.services.strategy.EarningsStraddle as es_module

        importlib.reload(es_module)
        EarningsStraddle = es_module.EarningsStraddle

        # Call the method
        await EarningsStraddle.create_strategy()

        # assert mock_select.call_count == 2
        # assert mock_select_stmt.filter.call_count == 1
        # assert mock_select_stmt.filter_by.call_count == 1
        mock_strategy_crud.read.assert_called_once_with(
            {"strategy": EarningsStraddle.strategy}
        )
        mock_strategy_crud.create.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_stocks(
        self, mock_async_with_db_session
    ):  # Imported pytest fixture required to mock the wrappers properly
        """Test buy order generation for short straddle."""
        import app.services.strategy.EarningsStraddle as es_module

        importlib.reload(es_module)
        EarningsStraddle = es_module.EarningsStraddle

        resps = [
            MockResponse(
                {
                    "data": {
                        "rows": [
                            {"symbol": "SPY", "time": "time-not-supplied"},
                            {"symbol": "QQQ", "time": "time-pre-market"},
                            {"symbol": "AAPL", "time": "time-after-hours"},
                        ]
                    }
                },
                200,
            ),
            MockResponse({"data": {"rows": []}}, 200),
            MockResponse(
                {"option_contracts": [{"expiration_date": "2025-06-20"}]}, 200
            ),
            MockResponse(
                {"option_contracts": [{"expiration_date": "2025-06-20"}]}, 200
            ),
        ]

        today = pytz.timezone("US/Eastern").localize(datetime(2025, 1, 6, 0, 0, 0, 0))
        tmr = pytz.timezone("US/Eastern").localize(datetime(2025, 1, 7, 0, 0, 0, 0))
        with patch.object(
            EarningsStraddle,
            "get_dates_until_fri",
            new_callable=AsyncMock,
        ) as mock_get_dates_until_fri:
            mock_get_dates_until_fri.return_value = [today, tmr]
            with patch(
                "app.services.strategy.EarningsStraddle.aiohttp.ClientSession.get",
                side_effect=resps,
            ) as mock_get:
                ib = MagicMock()

                async def qualifyContracts(*args):
                    for i in args:
                        i.conId = 1
                    return args

                class ContractDetail:
                    contract = "lol"

                ib.qualifyContractsAsync = qualifyContracts
                ib.reqContractDetailsAsync = AsyncMock(
                    return_value=[ContractDetail, ContractDetail]
                )
                broker = MagicMock()
                broker.ib = ib

                await EarningsStraddle.get_stocks(broker)

                assert mock_get.call_count == 4
                assert len(EarningsStraddle.future_earnings_stocks) == 2
                assert all(
                    [len(i) == 2 for i in EarningsStraddle.possible_options.values()]
                )
                assert all(
                    [
                        i in EarningsStraddle.possible_options
                        for i in EarningsStraddle.future_earnings_stocks
                    ]
                )

    @pytest.mark.asyncio
    async def test_update_bars_till_present_no_curr_positions_no_historical_data(
        self, mock_async_with_db_session
    ):
        """Test buy order generation for short straddle."""
        current_option_position_crud = mock_async_with_db_session[
            "current_option_position"
        ]
        historical_data_crud = mock_async_with_db_session["historical_data"]
        historical_option_data_crud = mock_async_with_db_session[
            "historical_option_data"
        ]
        current_option_position_crud.get_current_positions_for_strategy = AsyncMock(
            return_value=[]
        )

        import app.services.strategy.EarningsStraddle as es_module

        importlib.reload(es_module)
        EarningsStraddle = es_module.EarningsStraddle

        EarningsStraddle.future_earnings_stocks = [
            Stock("SPY", "SMART", "USD", conId=1),
            Stock("QQQ", "SMART", "USD", conId=1213),
        ]
        EarningsStraddle.possible_options = {
            Stock("SPY", "SMART", "USD", conId=1): [
                Option("SPY", "20170721", 240, "C", "SMART")
            ],
            Stock("QQQ", "SMART", "USD", conId=1213): [
                Option("QQQ", "20170721", 240, "C", "SMART")
            ],
        }
        historical_data_crud.read_stock = AsyncMock(return_value=[])
        historical_option_data_crud.read_stock = AsyncMock(return_value=[])
        historical_data_crud.create_or_update_all = AsyncMock(return_value=[])
        historical_option_data_crud.create_or_update_all = AsyncMock(return_value=[])

        # Define IBKR
        ib = MagicMock()

        async def qualifyContracts(*args):
            for i in args:
                i.conId = 1
            return args

        class ContractDetail:
            contract = "lol"

        ib.qualifyContractsAsync = qualifyContracts
        ib.reqContractDetailsAsync = AsyncMock(
            return_value=[ContractDetail, ContractDetail]
        )
        broker = MagicMock()
        broker.ib = ib

        # set ibkr reqHistoricalDataAsync []
        ib.reqHistoricalDataAsync = AsyncMock(return_value=[])

        await EarningsStraddle.update_bars_till_present(broker)

        # Gets data for historical_data => Empty => Moves on without checking for options_contracts
        assert ib.reqHistoricalDataAsync.call_count == 2
        assert len(EarningsStraddle.future_earnings_stocks) == 0
        assert historical_data_crud.create_or_update_all.call_count == 0
        assert historical_option_data_crud.create_or_update_all.call_count == 0

    @pytest.mark.asyncio
    async def test_update_bars_till_present_no_curr_positions_valid_historical_data(
        self, mock_async_with_db_session
    ):
        """Test buy order generation for short straddle."""
        current_option_position_crud = mock_async_with_db_session[
            "current_option_position"
        ]
        historical_data_crud = mock_async_with_db_session["historical_data"]
        historical_option_data_crud = mock_async_with_db_session[
            "historical_option_data"
        ]
        current_option_position_crud.get_current_positions_for_strategy = AsyncMock(
            return_value=[]
        )

        import app.services.strategy.EarningsStraddle as es_module

        importlib.reload(es_module)
        EarningsStraddle = es_module.EarningsStraddle

        EarningsStraddle.future_earnings_stocks = [
            Stock("SPY", "SMART", "USD", conId=1),
            Stock("QQQ", "SMART", "USD", conId=1213),
        ]
        EarningsStraddle.possible_options = {
            Stock("SPY", "SMART", "USD", conId=1): [
                Option("SPY", "20170721", 240, "C", "SMART", multiplier="100")
            ],
            Stock("QQQ", "SMART", "USD", conId=1213): [
                Option("QQQ", "20170721", 240, "C", "SMART", multiplier="100")
            ],
        }
        historical_data_crud.read_stock = AsyncMock(return_value=[])
        historical_option_data_crud.read_stock = AsyncMock(return_value=[])
        historical_data_crud.create_or_update_all = AsyncMock(return_value=[])
        historical_option_data_crud.create_or_update_all = AsyncMock(return_value=[])

        # Define IBKR
        ib = MagicMock()

        async def qualifyContracts(*args):
            for i in args:
                i.conId = 1
            return args

        class ContractDetail:
            contract = "lol"

        ib.qualifyContractsAsync = qualifyContracts
        ib.reqContractDetailsAsync = AsyncMock(
            return_value=[ContractDetail, ContractDetail]
        )
        broker = MagicMock()
        broker.ib = ib

        # set ibkr reqHistoricalDataAsync []
        ib.reqHistoricalDataAsync = AsyncMock(
            side_effect=[
                [BarData(open=1, high=2, low=3, close=4)],
                [],
                [BarData(open=5, high=6, low=7, close=8)],
                [],
            ]
        )

        await EarningsStraddle.update_bars_till_present(broker)

        # Gets data for historical_data => Empty => Moves on without checking for options_contracts
        assert ib.reqHistoricalDataAsync.call_count == 4
        assert len(EarningsStraddle.future_earnings_stocks) == 2
        assert historical_data_crud.create_or_update_all.call_count == 2
        assert historical_option_data_crud.create_or_update_all.call_count == 2

    @pytest.mark.asyncio
    async def test_update_bars_till_present_some_curr_positions(
        self, mock_async_with_db_session
    ):
        """Test buy order generation for short straddle."""
        current_option_position_crud = mock_async_with_db_session[
            "current_option_position"
        ]
        historical_data_crud = mock_async_with_db_session["historical_data"]
        historical_option_data_crud = mock_async_with_db_session[
            "historical_option_data"
        ]
        current_option_position_crud.get_current_positions_for_strategy = AsyncMock(
            return_value=[
                cast(
                    CurrentOptionPositionsDict,
                    {
                        "stock": "SPY",
                        "expiry": "20250610",
                        "multiplier": 100,
                        "strike": 500,
                        "option_type": OptionType.C,
                        "quantity": 10,
                        "avg_price": 400,
                    },
                )
            ]
        )

        import app.services.strategy.EarningsStraddle as es_module

        importlib.reload(es_module)
        EarningsStraddle = es_module.EarningsStraddle

        EarningsStraddle.future_earnings_stocks = []
        EarningsStraddle.possible_options = {}
        EarningsStraddle.get_stocks = AsyncMock()
        historical_data_crud.read_stock = AsyncMock(return_value=[])
        historical_option_data_crud.read_stock = AsyncMock(return_value=[])
        historical_data_crud.create_or_update_all = AsyncMock(return_value=[])
        historical_option_data_crud.create_or_update_all = AsyncMock(return_value=[])

        # Define IBKR
        ib = MagicMock()

        async def qualifyContracts(*args):
            for i in args:
                i.conId = 1
            return args

        class ContractDetail:
            contract = "lol"

        ib.qualifyContractsAsync = qualifyContracts
        ib.reqContractDetailsAsync = AsyncMock(
            return_value=[ContractDetail, ContractDetail]
        )
        broker = MagicMock()
        broker.ib = ib

        now = datetime.now()
        # set ibkr reqHistoricalDataAsync []
        ib.reqHistoricalDataAsync = AsyncMock(
            side_effect=[
                [BarData(open=1, high=2, low=3, close=4, volume=5, date=now)],
                [BarData(open=5, high=6, low=7, close=8, volume=9)],
            ]
        )

        await EarningsStraddle.update_bars_till_present(broker)

        # Gets data for historical_data => Empty => Moves on without checking for options_contracts
        assert ib.reqHistoricalDataAsync.call_count == 1
        historical_option_data_crud.create_or_update_all.assert_called_once_with(
            [
                {
                    "stock": "SPY",
                    "expiry": "20250610",
                    "strike": 500,
                    "multiplier": 100,
                    "option_type": OptionType.C.value,
                    "time": now,
                    "open": 1,
                    "high": 2,
                    "low": 3,
                    "close": 4,
                    "volume": 5,
                }
            ]
        )


#     @pytest.mark.asyncio
#     async def test_get_sell_order(self, mock_broker, mock_option_contracts):
#         """Test sell order generation for short straddle."""
#         call_option, put_option = mock_option_contracts
#
#         # Mock prices
#         mock_broker.get_current_price.side_effect = [2.75, 2.25]  # call, put prices
#
#         with (
#             patch("your_module.Option") as mock_option_class,
#             patch("your_module.Contract") as mock_contract_class,
#             patch("your_module.ComboLeg") as mock_combo_leg_class,
#             patch("your_module.LimitOrder") as mock_limit_order_class,
#             patch("your_module.TagValue") as mock_tag_value_class,
#             patch("your_module.datetime") as mock_datetime,
#             patch("your_module.pytz") as mock_pytz,
#         ):
#             # Setup datetime and timezone mocks
#             mock_now = datetime(2023, 6, 15, 14, 30, 0)
#             mock_datetime.now.return_value = mock_now
#             mock_eastern = Mock()
#             mock_pytz.timezone.return_value = mock_eastern
#             mock_eastern.localize.return_value = mock_now.replace(
#                 tzinfo=pytz.timezone("US/Eastern")
#             )
#
#             # Setup option contract mocks
#             mock_option_class.side_effect = [call_option, put_option]
#
#             # Setup contract and order mocks
#             mock_contract = Mock()
#             mock_contract_class.return_value = mock_contract
#             mock_combo_leg_class.side_effect = [Mock(), Mock()]
#             mock_order = Mock()
#             mock_order.smartComboRoutingParams = []
#             mock_limit_order_class.return_value = mock_order
#             mock_tag_value_class.return_value = Mock()
#
#             # Call the method
#             result = await ShortStraddle.get_sell_order("SPY", mock_broker, 1.0, 450.0)
#
#             # Verify results
#             assert len(result) == 1
#             assert "contract" in result[0]
#             assert "order" in result[0]
#
#             # Verify order parameters
#             mock_limit_order_class.assert_called_once_with(
#                 "BUY", 1.0, round(1.005 * (2.75 + 2.25), 2), tif="GTC", allOrNone=True
#             )
#
#     @pytest.mark.asyncio
#     async def test_get_orders_for_quantity_difference_buy(self, mock_broker):
#         """Test get_orders_for_quantity_difference for buy scenario."""
#
#         quantity_differences = {
#             ("SPY", "20230616", 450.0, 1.0, OptionType.C): -1.0,
#             ("SPY", "20230616", 450.0, 1.0, OptionType.P): -1.0,
#         }
#
#         with patch.object(ShortStraddle, "get_buy_order") as mock_get_buy_order:
#             mock_get_buy_order.return_value = [{"contract": Mock(), "order": Mock()}]
#
#             result = await ShortStraddle.get_orders_for_quantity_difference(
#                 mock_broker, quantity_differences
#             )
#
#             # Should call get_buy_order since quantity is negative
#             mock_get_buy_order.assert_called_once_with("SPY", mock_broker, -1.0, 450.0)
#             assert len(result) == 1
#
#     @pytest.mark.asyncio
#     async def test_get_orders_for_quantity_difference_sell(self, mock_broker):
#         """Test get_orders_for_quantity_difference for sell scenario."""
#
#         quantity_differences = {
#             ("SPY", "20230616", 450.0, 1.0, OptionType.C): 1.0,
#             ("SPY", "20230616", 450.0, 1.0, OptionType.P): 1.0,
#         }
#
#         with patch.object(ShortStraddle, "get_sell_order") as mock_get_sell_order:
#             mock_get_sell_order.return_value = [{"contract": Mock(), "order": Mock()}]
#
#             result = await ShortStraddle.get_orders_for_quantity_difference(
#                 mock_broker, quantity_differences
#             )
#
#             # Should call get_sell_order since quantity is positive
#             mock_get_sell_order.assert_called_once_with("SPY", mock_broker, 1.0, 450.0)
#             assert len(result) == 1
#
#     @pytest.mark.asyncio
#     async def test_get_orders_for_quantity_difference_empty(self, mock_broker):
#         """Test get_orders_for_quantity_difference with empty input."""
#         result = await ShortStraddle.get_orders_for_quantity_difference(mock_broker, {})
#         assert result == []
#
#     def test_get_orders_for_quantity_difference_invalid_input(self, mock_broker):
#         """Test get_orders_for_quantity_difference with invalid input."""
#
#         # Test with wrong number of entries
#         quantity_differences = {
#             ("SPY", "20230616", 450.0, 1.0, OptionType.C): -1.0,
#         }
#
#         with pytest.raises(AssertionError):
#             asyncio.run(
#                 ShortStraddle.get_orders_for_quantity_difference(
#                     mock_broker, quantity_differences
#                 )
#             )
#
#         # Test with mismatched contract details
#         quantity_differences = {
#             ("SPY", "20230616", 450.0, 1.0, OptionType.C): -1.0,
#             ("SPY", "20230617", 450.0, 1.0, OptionType.P): -1.0,  # Different expiry
#         }
#
#         with pytest.raises(AssertionError):
#             asyncio.run(
#                 ShortStraddle.get_orders_for_quantity_difference(
#                     mock_broker, quantity_differences
#                 )
#             )
#
#
# class TestShortStraddleGetWeights:
#     """Test the get_weights method."""
#
#     @pytest.fixture
#     def mock_cruds_for_weights(self):
#         """Mock CRUD objects for get_weights testing."""
#         phantom_mock = AsyncMock()
#         positions_mock = AsyncMock()
#         volatility_mock = AsyncMock()
#         conn_mock = AsyncMock()
#         broker_mock = AsyncMock()
#
#         return phantom_mock, positions_mock, volatility_mock, conn_mock, broker_mock
#
#     @pytest.fixture
#     def mock_portfolio_data(self):
#         """Mock portfolio data structure."""
#         return {
#             "time": datetime(2023, 6, 15, 15, 0, 0, tzinfo=pytz.timezone("US/Eastern")),
#             "cash_portfolio_value": 98000.0,
#             "option_portfolio_value": -1500.0,
#             "bought_price": 4.80,
#             "strike": 450.0,
#             "peak": 5.20,
#             "paused": False,
#             "resume_trades": 0,
#         }
#
#     @pytest.fixture
#     def mock_volatility_data(self):
#         """Mock volatility data for testing."""
#         return [
#             {"open": 0.25, "time": datetime(2023, 6, 15, 14, 55, 0)},
#             {"open": 0.24, "time": datetime(2023, 6, 15, 14, 50, 0)},
#             {"open": 0.26, "time": datetime(2023, 6, 15, 14, 45, 0)},
#         ] * 26  # 78 entries total
#
#     @pytest.mark.asyncio
#     async def test_get_weights_basic_setup(
#         self, mock_cruds_for_weights, mock_portfolio_data, mock_volatility_data
#     ):
#         """Test basic setup and initialization of get_weights method."""
#         phantom_mock, positions_mock, volatility_mock, conn_mock, broker_mock = (
#             mock_cruds_for_weights
#         )
#
#         # Setup mock returns
#         phantom_mock.get_last_entry.return_value = [mock_portfolio_data, {}]
#         volatility_mock.read_for_stock_past.return_value = mock_volatility_data
#         positions_mock.get_current_positions_for_strategy.return_value = []
#
#         with (
#             patch(
#                 "your_module.async_historical_volatility_data_wrapper"
#             ) as mock_vol_wrapper,
#             patch("your_module.async_with_engine") as mock_engine_wrapper,
#             patch(
#                 "your_module.async_current_option_positions_wrapper"
#             ) as mock_positions_wrapper,
#             patch(
#                 "your_module.async_phantom_portfolio_value_wrapper"
#             ) as mock_phantom_wrapper,
#             patch("your_module.datetime") as mock_datetime,
#             patch("your_module.pytz") as mock_pytz,
#             patch("your_module.CustomLogger") as mock_logger,
#             patch.object(ShortStraddle, "update_portfolio_value_to_now") as mock_update,
#             patch("your_module.deque") as mock_deque,
#             patch("your_module.SortedList") as mock_sorted_list,
#         ):
#             # Setup datetime and timezone mocks
#             mock_now = datetime(2023, 6, 15, 15, 0, 0)
#             mock_datetime.now.return_value = mock_now
#             mock_datetime.combine.side_effect = lambda date, time: datetime.combine(
#                 date, time
#             )
#             mock_datetime.strptime.side_effect = (
#                 lambda time_str, fmt: datetime.strptime(time_str, fmt)
#             )
#
#             mock_eastern = pytz.timezone("US/Eastern")
#             mock_pytz.timezone.return_value = mock_eastern
#
#             # Setup wrapper decorators
#             async def wrapper_side_effect(func):
#                 async def inner(*args, **kwargs):
#                     return await func(
#                         phantom_mock,
#                         positions_mock,
#                         conn_mock,
#                         volatility_mock,
#                         broker_mock,
#                     )
#
#                 return inner
#
#             mock_vol_wrapper.side_effect = wrapper_side_effect
#             mock_engine_wrapper.side_effect = wrapper_side_effect
#             mock_positions_wrapper.side_effect = wrapper_side_effect
#             mock_phantom_wrapper.side_effect = wrapper_side_effect
#
#             # Setup other mocks
#             mock_update.return_value = None
#             mock_logger.return_value.info = Mock()
#             mock_deque.return_value = Mock()
#             mock_sorted_list.return_value = Mock()
#
#             # Call the method
#             result = await ShortStraddle.get_weights()
#
#             # Verify key method calls
#             mock_update.assert_called_once()
#             phantom_mock.get_last_entry.assert_called_once()
#             volatility_mock.read_for_stock_past.assert_called_once_with(
#                 "SPY", mock_portfolio_data["time"]
#             )
#             positions_mock.get_current_positions_for_strategy.assert_called_once_with(
#                 ShortStraddle.strategy
#             )
#
#     @pytest.mark.asyncio
#     async def test_get_weights_no_volatility_data(
#         self, mock_cruds_for_weights, mock_portfolio_data
#     ):
#         """Test get_weights when no volatility data is available."""
#         phantom_mock, positions_mock, volatility_mock, conn_mock, broker_mock = (
#             mock_cruds_for_weights
#         )
#
#         # Setup mock returns with empty volatility data
#         phantom_mock.get_last_entry.return_value = [mock_portfolio_data]
#         volatility_mock.read_for_stock_past.return_value = []
#
#         with (
#             patch(
#                 "your_module.async_historical_volatility_data_wrapper"
#             ) as mock_vol_wrapper,
#             patch("your_module.async_with_engine") as mock_engine_wrapper,
#             patch(
#                 "your_module.async_current_option_positions_wrapper"
#             ) as mock_positions_wrapper,
#             patch(
#                 "your_module.async_phantom_portfolio_value_wrapper"
#             ) as mock_phantom_wrapper,
#             patch.object(ShortStraddle, "update_portfolio_value_to_now") as mock_update,
#         ):
#             # Setup wrapper decorators
#             async def wrapper_side_effect(func):
#                 async def inner(*args, **kwargs):
#                     return await func(
#                         phantom_mock,
#                         positions_mock,
#                         conn_mock,
#                         volatility_mock,
#                         broker_mock,
#                     )
#
#                 return inner
#
#             mock_vol_wrapper.side_effect = wrapper_side_effect
#             mock_engine_wrapper.side_effect = wrapper_side_effect
#             mock_positions_wrapper.side_effect = wrapper_side_effect
#             mock_phantom_wrapper.side_effect = wrapper_side_effect
#
#             mock_update.return_value = None
#
#             # Call the method
#             result = await ShortStraddle.get_weights()
#
#             # Should return empty list when no volatility data
#             assert result == []
#
#
# class TestShortStraddleDataMethods:
#     """Test data fetching and updating methods."""
#
#     @pytest.fixture
#     def mock_broker_for_data(self):
#         """Mock broker for data methods."""
#         broker = AsyncMock()
#         broker.ib = AsyncMock()
#         broker.ib.qualifyContractsAsync = AsyncMock()
#         broker.ib.reqHistoricalDataAsync = AsyncMock()
#         return broker
#
#     @pytest.mark.asyncio
#     async def test_get_stocks(self, mock_broker_for_data):
#         """Test get_stocks method."""
#         with patch("your_module.Stock") as mock_stock_class:
#             mock_contract = Mock()
#             mock_stock_class.return_value = mock_contract
#
#             result = await ShortStraddle.get_stocks(mock_broker_for_data)
#
#             # Verify contract creation
#             mock_stock_class.assert_called_once_with("SPY", "SMART", "USD")
#
#             # Verify qualification
#             mock_broker_for_data.ib.qualifyContractsAsync.assert_called_once_with(
#                 mock_contract
#             )
#
#             # Verify result
#             assert result == [mock_contract]
#
#     @pytest.mark.asyncio
#     async def test_update_historical_data_to_present_sufficient_data(
#         self, mock_broker_for_data
#     ):
#         """Test update_historical_data_to_present when sufficient data exists."""
#         historical_data_mock = AsyncMock()
#         volatility_data_mock = AsyncMock()
#
#         # Mock sufficient data counts
#         historical_data_mock.read_stock_time_count.return_value = 42 * 78 + 100
#         volatility_data_mock.read_stock_time_count.return_value = 42 * 78 + 100
#
#         with (
#             patch("your_module.async_historical_data_wrapper") as mock_hist_wrapper,
#             patch(
#                 "your_module.async_historical_volatility_data_wrapper"
#             ) as mock_vol_wrapper,
#             patch("your_module.datetime") as mock_datetime,
#             patch("your_module.pytz") as mock_pytz,
#             patch("your_module.timedelta") as mock_timedelta,
#         ):
#             # Setup wrapper decorators
#             async def wrapper_side_effect(func):
#                 async def inner(*args, **kwargs):
#                     return await func(
#                         volatility_data_mock, historical_data_mock, mock_broker_for_data
#                     )
#
#                 return inner
#
#             mock_hist_wrapper.side_effect = wrapper_side_effect
#             mock_vol_wrapper.side_effect = wrapper_side_effect
#
#             # Setup datetime mocks
#             mock_now = datetime(2023, 6, 15, 15, 0, 0)
#             mock_datetime.now.return_value = mock_now
#             mock_eastern = pytz.timezone("US/Eastern")
#             mock_pytz.timezone.return_value = mock_eastern
#             mock_timedelta.return_value = timedelta(days=60)
#
#             # Call the method
#             await ShortStraddle.update_historical_data_to_present()
#
#             # Verify data count checks were made
#             historical_data_mock.read_stock_time_count.assert_called_once()
#             volatility_data_mock.read_stock_time_count.assert_called_once()
#
#             # Should return early without requesting more data
#             mock_broker_for_data.ib.reqHistoricalDataAsync.assert_not_called()
#
#     @pytest.mark.asyncio
#     async def test_update_historical_data_to_present_insufficient_data(
#         self, mock_broker_for_data
#     ):
#         """Test update_historical_data_to_present when more data is needed."""
#         historical_data_mock = AsyncMock()
#         volatility_data_mock = AsyncMock()
#
#         # Mock insufficient data counts
#         historical_data_mock.read_stock_time_count.return_value = 10
#         volatility_data_mock.read_stock_time_count.return_value = 10
#
#         # Mock historical data response
#         mock_bars = [Mock(), Mock(), Mock()]
#         mock_broker_for_data.ib.reqHistoricalDataAsync.return_value = mock_bars
#
#         with (
#             patch("your_module.async_historical_data_wrapper") as mock_hist_wrapper,
#             patch(
#                 "your_module.async_historical_volatility_data_wrapper"
#             ) as mock_vol_wrapper,
#             patch("your_module.Stock") as mock_stock_class,
#             patch("your_module.datetime") as mock_datetime,
#             patch("your_module.pytz") as mock_pytz,
#             patch("your_module.timedelta") as mock_timedelta,
#             patch("builtins.print") as mock_print,
#         ):
#             # Setup wrapper decorators
#             async def wrapper_side_effect(func):
#                 async def inner(*args, **kwargs):
#                     return await func(
#                         volatility_data_mock, historical_data_mock, mock_broker_for_data
#                     )
#
#                 return inner
#
#             mock_hist_wrapper.side_effect = wrapper_side_effect
#             mock_vol_wrapper.side_effect = wrapper_side_effect
#
#             # Setup datetime mocks
#             mock_now = datetime(2023, 6, 15, 15, 0, 0)
#             mock_datetime.now.return_value = mock_now
#             mock_eastern = pytz.timezone("US/Eastern")
#             mock_pytz.timezone.return_value = mock_eastern
#             mock_timedelta.return_value = timedelta(days=60)
#
#             # Setup contract mock
#             mock_contract = Mock()
#             mock_stock_class.return_value = mock_contract
#
#             # Call the method
#             await ShortStraddle.update_historical_data_to_present()
#
#             # Verify contract qualification
#             mock_broker_for_data.ib.qualifyContractsAsync.assert_called_with(
#                 mock_contract
#             )
#
#             # Verify historical data request
#             mock_broker_for_data.ib.reqHistoricalDataAsync.assert_called()
#
#             # Verify print statements
#             assert mock_print.call_count >= 1
#
#
# class TestShortStraddleIntegration:
#     """Integration tests for the ShortStraddle class."""
#
#     def test_straddle_pricing_consistency(self):
#         """Test that straddle pricing is consistent across methods."""
#         S = 100.0
#         K = 100.0
#         T = 0.25
#         r = 0.04
#         atm_sigma = 0.2
#         position = -1
#
#         # Calculate straddle value using individual option prices
#         call_price = black_scholes_call(S, K, T, r, atm_sigma)
#         put_price = black_scholes_put(S, K, T, r, atm_sigma)
#         straddle_value = (call_price + put_price) * position
#
#         # Test estimate_updated_value with same parameters
#         estimated_value = ShortStraddle.estimate_updated_value(
#             straddle_value,  # previous_option_portfolio_value
#             S,  # previous_spot_open
#             atm_sigma,  # previous_implied_volatility
#             atm_sigma,  # current_implied_volatility (same)
#             S,  # current_spot_open (same)
#             K,
#             T,
#             position,
#         )
#
#         # Should be approximately equal when parameters are the same
#         assert abs(estimated_value - straddle_value) < 1e-6
#
#     def test_vol_skew_impact_on_pricing(self):
#         """Test that volatility skew affects option pricing appropriately."""
#         S = 100.0
#         T = 0.25
#         r = 0.04
#         atm_sigma = 0.2
#
#         # ATM options
#         atm_call = black_scholes_call(S, S, T, r, atm_sigma)
#         atm_put = black_scholes_put(S, S, T, r, atm_sigma)
#
#         # OTM call (higher strike, higher vol due to skew)
#         otm_call_strike = 110.0
#         otm_call = black_scholes_call(S, otm_call_strike, T, r, atm_sigma)
#
#         # OTM put (lower strike, higher vol due to skew)
#         otm_put_strike = 90.0
#         otm_put = black_scholes_put(S, otm_put_strike, T, r, atm_sigma)
#
#         # Due to vol skew, OTM options should have higher implied vol
#         # This affects the pricing, but the exact relationships depend on the skew parameters
#         assert otm_call > 0
#         assert otm_put > 0
#         assert atm_call > 0
#         assert atm_put > 0
#
#     @pytest.mark.asyncio
#     async def test_order_generation_integration(self):
#         """Test integration between order methods."""
#
#         mock_broker = AsyncMock()
#         mock_broker.get_current_price.side_effect = [2.50, 2.30]  # call, put prices
#
#         quantity_differences = {
#             ("SPY", "20230616", 450.0, 1.0, OptionType.C): -1.0,
#             ("SPY", "20230616", 450.0, 1.0, OptionType.P): -1.0,
#         }
#
#         with patch.object(ShortStraddle, "get_buy_order") as mock_get_buy_order:
#             mock_get_buy_order.return_value = [{"contract": Mock(), "order": Mock()}]
#
#             result = await ShortStraddle.get_orders_for_quantity_difference(
#                 mock_broker, quantity_differences
#             )
#
#             # Verify the integration works
#             assert len(result) == 1
#             mock_get_buy_order.assert_called_once_with("SPY", mock_broker, -1.0, 450.0)


# Fixtures for database testing (if you want to test with actual database)
@pytest.fixture
def mock_db_session():
    """Mock database session for testing."""
    session = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    return session
