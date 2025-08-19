import asyncio
from cachetools import TTLCache
import numpy as np
from datetime import datetime, timezone, timedelta
from typing import (
    Coroutine,
    Dict,
    List,
    Callable,
    TypeVar,
    Awaitable,
    Optional,
    cast,
    Tuple,
    Set,
    Any,
)
from ib_async import Client, Wrapper
from ib_async.ib import IB
from ib_async.contract import Contract, Option
from ib_async.order import Trade
from ib_async.objects import Fill, RealTimeBarList, RealTimeBar
from ib_async.ticker import Ticker
import pytz

from app.services.broker.DataBroker import FullOrder
from app.services.broker.Broker import Broker
from app.services.models.AsyncModelsCRUD import (
    AsyncCurrentOptionPositionsCRUD,
    AsyncCurrentStockPositionsCRUD,
    AsyncHistoricalDataCRUD,
    AsyncHistoricalVolatilityDataCRUD,
    AsyncOpenOptionOrdersCRUD,
    AsyncOpenStockOrdersCRUD,
    AsyncOptionTransactionsCRUD,
    AsyncStockTransactionsCRUD,
)
from app.utils.custom_logging import CustomLogger

from app.utils.db import (
    with_db_session_for_model_class_method,
    async_with_db_session_for_model_class_method,
)
from app.services.models.ModelsCRUD import (
    CurrentStockPositionsCRUD,
    CurrentOptionPositionsCRUD,
    StockTransactionsCRUD,
    OptionTransactionsCRUD,
    OpenStockOrdersCRUD,
    OpenOptionOrdersCRUD,
    HistoricalDataCRUD,
    HistoricalVolatilityDataCRUD,
)
from app.models import (
    CurrentStockPositions,
    CurrentOptionPositions,
    StockTransactions,
    OptionTransactions,
    OpenStockOrders,
    OpenOptionOrders,
    HistoricalData,
    HistoricalVolatilityData,
)
from app.models_types import (
    CurrentStockPositionsDict,
    CurrentOptionPositionsDict,
    StockTransactionsDict,
    OptionTransactionsDict,
    OptionType,
)
from app.tasks.execution_tasks import update_target_position_and_send_orders_for_broker
from app.services.strategy.StockStrategy import StockStrategy
from app.services.strategy.OptionStrategy import OptionStrategy


from app.services.IBC import full_restart_ibkr


T = TypeVar("T")  # Generic type variable


current_stock_position_wrapper = with_db_session_for_model_class_method(
    CurrentStockPositionsCRUD, CurrentStockPositions, "current_stock_positions"
)
async_current_stock_position_wrapper = async_with_db_session_for_model_class_method(
    AsyncCurrentStockPositionsCRUD,
    CurrentStockPositions,
    "async_current_stock_positions",
)
current_option_position_wrapper = with_db_session_for_model_class_method(
    CurrentOptionPositionsCRUD, CurrentOptionPositions, "current_option_positions"
)
async_current_option_position_wrapper = async_with_db_session_for_model_class_method(
    AsyncCurrentOptionPositionsCRUD,
    CurrentOptionPositions,
    "async_current_option_positions",
)
stock_transaction_wrapper = with_db_session_for_model_class_method(
    StockTransactionsCRUD, StockTransactions, "stock_transactions"
)
async_stock_transaction_wrapper = async_with_db_session_for_model_class_method(
    AsyncStockTransactionsCRUD, StockTransactions, "async_stock_transactions"
)
option_transaction_wrapper = with_db_session_for_model_class_method(
    OptionTransactionsCRUD, OptionTransactions, "option_transactions"
)
async_option_transaction_wrapper = async_with_db_session_for_model_class_method(
    AsyncOptionTransactionsCRUD, OptionTransactions, "async_option_transactions"
)
open_stock_orders_wrapper = with_db_session_for_model_class_method(
    OpenStockOrdersCRUD, OpenStockOrders, "open_stock_orders"
)
async_open_stock_orders_wrapper = async_with_db_session_for_model_class_method(
    AsyncOpenStockOrdersCRUD, OpenStockOrders, "async_open_stock_orders"
)
open_option_orders_wrapper = with_db_session_for_model_class_method(
    OpenOptionOrdersCRUD, OpenOptionOrders, "open_option_orders"
)
async_open_option_orders_wrapper = async_with_db_session_for_model_class_method(
    AsyncOpenOptionOrdersCRUD, OpenOptionOrders, "async_open_option_orders"
)
historical_data_wrapper = with_db_session_for_model_class_method(
    HistoricalDataCRUD, HistoricalData, "historical_data"
)
async_historical_data_wrapper = async_with_db_session_for_model_class_method(
    AsyncHistoricalDataCRUD, HistoricalData, "async_historical_data"
)
historical_volatility_data_wrapper = with_db_session_for_model_class_method(
    HistoricalVolatilityDataCRUD, HistoricalVolatilityData, "historical_volatility_data"
)
async_historical_volatility_data_wrapper = async_with_db_session_for_model_class_method(
    AsyncHistoricalVolatilityDataCRUD,
    HistoricalVolatilityData,
    "async_historical_volatility_data",
)


class IBKR(Wrapper, Client, Broker):
    price_events: Dict[Contract, Any] = {}

    def __init__(
        self,
        host: str,
        port: int,
        client_id: int,
        account: str,
        stock_strategy: Optional[StockStrategy] = None,
        option_strategy: Optional[OptionStrategy] = None,
    ):
        # super().__init__()
        self.ib: IB = IB()
        self.logger = CustomLogger(name="IBAsyncBroker")
        self.host = host
        self.port = port
        self.client_id = client_id
        self.account = account

        self.stock_strategy = stock_strategy
        self.option_strategy = option_strategy
        if self.stock_strategy is None and self.option_strategy is None:
            raise ValueError("Cannot supply no strategies to IBKR!")
        elif self.option_strategy is None:
            assert self.stock_strategy is not None
            self.strategy = self.stock_strategy.strategy
        else:
            assert self.option_strategy is not None
            self.strategy = self.option_strategy.strategy
        if (self.stock_strategy is not None and self.option_strategy is not None) and (
            self.stock_strategy.strategy != self.option_strategy.strategy
        ):
            raise ValueError(
                "stock strategy passed to IBKR should be the same as option strategy passed to IBKR!"
            )

        self.live_data: Dict[Contract, List[RealTimeBar]] = {}
        self.live_options_data: Dict[Contract, List[Tuple[datetime, float]]] = {}
        # self.past_data: Dict[Contract, Tuple[datetime, float]] = {}
        self.past_data: TTLCache[Contract, float] = TTLCache(maxsize=1000, ttl=20)
        self.past_data_vwap: TTLCache[Contract, float] = TTLCache(maxsize=1000, ttl=20)

        self.ib.newOrderEvent += self.newOrderEvent
        self.ib.execDetailsEvent += self.execDetailsEvent

        self.live_data_last_updated: Dict[
            Contract,
            Tuple[
                datetime,
                Callable[[RealTimeBarList, bool], Coroutine[Any, Any, None]],
            ],
        ] = {}

        self.current_stock_positions: CurrentStockPositionsCRUD
        self.async_current_stock_positions: AsyncCurrentStockPositionsCRUD
        self.current_option_positions: CurrentOptionPositionsCRUD
        self.async_current_option_positions: AsyncCurrentOptionPositionsCRUD
        self.stock_transactions: StockTransactionsCRUD
        self.async_stock_transactions: AsyncStockTransactionsCRUD
        self.option_transactions: OptionTransactionsCRUD
        self.async_option_transactions: AsyncOptionTransactionsCRUD
        self.open_stock_orders: OpenStockOrdersCRUD
        self.async_open_stock_orders: AsyncOpenStockOrdersCRUD
        self.open_option_orders: OpenOptionOrdersCRUD
        self.async_open_option_orders: AsyncOpenOptionOrdersCRUD
        self.historical_data: HistoricalDataCRUD
        self.async_historical_data: AsyncHistoricalDataCRUD
        self.historical_volatility_data: HistoricalVolatilityDataCRUD
        self.async_historical_volatility_data: AsyncHistoricalVolatilityDataCRUD

    async def set_up_strategies(self) -> None:
        if self.stock_strategy:
            await self.stock_strategy.create_strategy()
        if self.option_strategy:
            await self.option_strategy.create_strategy()

    async def connect_to_broker(self, timeout: int = 60) -> None:
        """
        Async method to connect to the IBKR API.
        """
        if self.ib.isConnected():
            return
        try:
            await self.ib.connectAsync(
                self.host,
                self.port,
                self.client_id,
                timeout=timeout,
                account=self.account,
            )
        except TimeoutError as e:
            self.logger.error(f"Connect failed 1: {e}, trying again")
            await self.ib.connectAsync(
                self.host,
                self.port,
                self.client_id,
                timeout=timeout,
                account=self.account,
            )
        except Exception as e:
            await self.ib.sleep(5)
            await self.ib.connectAsync(
                self.host,
                self.port,
                self.client_id,
                timeout=timeout,
                account=self.account,
            )

        self.ib.reqMarketDataType(1)
        self.logger.info("connect(): Successfully connected to IBKR.")
        await self.update_historical_data_till_today()
        await self.set_up_strategies()

    def disconnect_from_broker(self) -> None:
        """
        Async method to disconnect from the IBKR API.
        """
        self.ib.disconnect()
        self.logger.info("disconnect(): Successfully disconnected from IBKR.")

    def sleep(self, seconds: int) -> None:
        self.ib.sleep(seconds)

    async def _qualify_contracts_async(self, *contracts: Contract) -> None:
        """Runs qualifyContracts in a separate thread to prevent blocking."""
        await self.ib.qualifyContractsAsync(*contracts)

    async def _reset_IBKR(self) -> None:
        self.disconnect_from_broker()
        await full_restart_ibkr()
        await self.connect_to_broker()
        return

    async def _possibly_reset_once(self, func: Callable[[], Awaitable[T]]) -> T:
        """
        Runs an async function with a timeout. If it times out, resets IBKR and retries once.

        Args:
            func (Callable[[], Awaitable[T]]): An async function that returns a value of type T.

        Returns:
            T: The result of the async function.

        Raises:
            RuntimeError: If the function times out twice.
        """
        try:
            res = await asyncio.wait_for(func(), timeout=60)
        except asyncio.TimeoutError:
            try:
                res = await asyncio.wait_for(func(), timeout=60)
            except asyncio.TimeoutError:
                await self._reset_IBKR()
                try:
                    res = await asyncio.wait_for(func(), timeout=60)
                except asyncio.TimeoutError:
                    raise RuntimeError(f"TimeoutError for func: {func}")
        except Exception as e:
            self.logger.error(f"Error from _possibly_reset_once(): {e}")
        return res

    def _get_ticker_price(self, ticker: Ticker, vwap: bool) -> float:
        """
        Gets the best known price of the ticker from Ticker Object
        with the following priorities:
            1st: VWAP
            2nd: if thr is bid / ask info returned
                2.33: Last price if between bid / ask
                2.66: Midpoint of bid / ask
            3rd: Last price
            4th: Close
        """
        if vwap:
            if not np.isnan(ticker.vwap):
                return ticker.vwap
            return -1

        next_alt = ticker.marketPrice()
        if not np.isnan(next_alt):
            return next_alt

        final_alt = ticker.close
        if np.isnan(final_alt):
            return -1
        return final_alt

    async def get_current_price(
        self,
        # stock: str, exchange: str = "SMART", currency: str = "USD", tries: int = 1
        contract: Contract,
        vwap: bool = False,
        # tries: int = 1,
    ) -> float:
        """
        Async method to get the current price of the stock
        Rate Limitations: No more than one regulatory snapshot request per second
        """
        if not contract.conId:
            await self._possibly_reset_once(
                lambda: self._qualify_contracts_async(contract)
            )

        if vwap:
            if contract in self.past_data_vwap:
                return self.past_data_vwap[contract]
            # Request Delayed Market Data - Will return live data if have subscription, else delayed - FREE
            ticker = self.ib.reqMktData(contract, snapshot=True)

            while self._get_ticker_price(ticker, False) == -1:
                is_not_timed_out = self.ib.waitOnUpdate(timeout=5)
                if not is_not_timed_out:
                    if self._get_ticker_price(ticker, False) == -1:
                        self.logger.error(
                            f"reqMktData() in get_current_price() timed out - no data for ticker: {contract.symbol}"
                        )
                        return 0
                    break

            vwap_price = self._get_ticker_price(ticker, False)
            self.past_data_vwap[contract] = vwap_price
            self.logger.info(f"VWAP price updated for {contract} at {vwap_price}")
            return vwap_price

        if contract in self.live_data:
            return self.live_data[contract][-1].close
        if contract in self.live_options_data:
            latest_time = self.live_options_data[contract][-1][0].astimezone(
                pytz.timezone("US/Eastern")
            )
            if datetime.now(timezone.utc).astimezone(
                pytz.timezone("US/Eastern")
            ) - latest_time <= timedelta(seconds=20):
                return self.live_options_data[contract][-1][1]

        if contract in self.past_data:
            return self.past_data[contract]

        if contract.exchange != "SMART":
            self.logger.info(
                f"get_current_price(): Are you sure you have market data subscriptions for this exchange: {contract.exchange}"
            )

        # Request Delayed Market Data - Will return live data if have subscription, else delayed - FREE
        ticker = self.ib.reqMktData(contract, snapshot=True)

        while self._get_ticker_price(ticker, False) == -1:
            is_not_timed_out = self.ib.waitOnUpdate(timeout=5)
            if not is_not_timed_out:
                if self._get_ticker_price(ticker, False) == -1:
                    self.logger.error(
                        f"reqMktData() in get_current_price() timed out - no data for ticker: {contract.symbol}"
                    )
                    return 0
                break

        price = self._get_ticker_price(ticker, False)
        self.past_data[contract] = price
        self.logger.info(f"price updated for {contract} at {price}")
        return price

    # async def get_current_option_price(
    #     self,
    #     # stock: str,
    #     # expiry: str,
    #     # strike: float,
    #     # multiplier: str,
    #     # option_type: str,
    #     # exchange: str = 'SMART',
    #     # currency: str = 'USD'
    #     option: Contract,
    # ) -> float:
    #     # option = Option(
    #     #     stock,
    #     #     expiry,
    #     #     strike,
    #     #     option_type,
    #     #     exchange,
    #     #     multiplier,
    #     #     currency
    #     # )
    #     print(f"Requesting data for this contract: {option}")
    #     await self._qualify_contracts_async(option)
    #     if option in self.live_options_data:
    #         return sel
    #     ticker = self.ib.reqMktData(option, snapshot=True)
    #
    #     while self._get_ticker_price(ticker) == -1:
    #         is_not_timed_out = self.ib.waitOnUpdate(timeout=5)
    #         if not is_not_timed_out:
    #             if self._get_ticker_price(ticker) == -1:
    #                 self.logger.error(
    #                     f"reqMktData() in get_current_option_price() timed out - no data for ticker option: {option.symbol}"
    #                 )
    #                 return 0
    #             break
    #     self.past_data[contract] = (
    #         datetime.now().astimezone(pytz.timezone("US/Eastern")),
    #         price,
    #     )
    #     return self._get_ticker_price(ticker)

    def _get_stop_order_price(self, stock: str, trades: List[Trade]) -> float:
        """
        Private async function that filters the trades to return price of stop order associated with the stock
        NOTE: Assumes each stock has only one stop order associated with it
        """
        open_stop_orders = [
            i
            for i in trades
            if i.order.orderType == "STP" and i.contract.symbol == stock
        ]
        if len(open_stop_orders) == 0:
            return -1.0
        if len(open_stop_orders) > 1:
            self.logger.debug(
                f"get_stop_order_price() returns more than 1 stop orders: {open_stop_orders}"
            )
        return open_stop_orders[0].order.auxPrice

    # @Override
    async def get_current_positions(self) -> Dict[str, int]:
        positions = self.ib.positions()
        # positions = await self.localise_positions(self.ib.positions())
        # stocks = set([i['stock'] for i in positions])
        #
        # if len(stocks) != len(positions):
        #     self.logger.error('Error in get_current_positions(): more than one position containing the same stock')
        return {
            position.contract.symbol: int(position.position) for position in positions
        }

    # @Override
    def cancel_all_open_orders(self) -> None:
        self.ib.reqGlobalCancel()

    async def _new_execution_update(self, trade: Trade, fill: Fill) -> None:
        await self._new_execution_update_stock(trade, fill)
        await self._new_execution_update_option(trade, fill)

        if self.stock_strategy:
            self.stock_strategy.execDetailsEvent(trade, fill)
        if self.option_strategy:
            await self.option_strategy.execDetailsEvent(trade, fill)

    @async_current_stock_position_wrapper
    @async_stock_transaction_wrapper
    async def _new_execution_update_stock(self, trade: Trade, fill: Fill) -> None:
        if trade.contract.secType != "STK":
            return
        await self.async_stock_transactions.create(
            {
                "stock": trade.contract.symbol,
                "strategy": self.strategy,
                "time": fill.time,
                "price_transacted": fill.execution.price,
                "fees": fill.commissionReport.commission,
                "quantity": fill.execution.shares
                * (-1 if trade.order.action == "SELL" else 1),
            }
        )
        current_positions = await self.async_current_stock_positions.read(
            {
                "stock": trade.contract.symbol,
                "strategy": self.strategy,
            }
        )
        if not current_positions:
            await self.async_current_stock_positions.create(
                {
                    "stock": trade.contract.symbol,
                    "strategy": self.strategy,
                    "avg_price": fill.execution.price,
                    "quantity": fill.execution.shares
                    * (-1.0 if trade.order.action == "SELL" else 1.0),
                    "stop_limit": -1.0,
                }
            )
            return
        current_position = current_positions[0]
        new_quantity = current_position["quantity"] + (
            fill.execution.shares * (-1 if trade.order.action == "SELL" else 1)
        )
        if new_quantity == 0:
            await self.async_current_stock_positions.delete(
                {
                    "stock": trade.contract.symbol,
                    "strategy": self.strategy,
                }
            )
            self.logger.info(
                f"Position updated: {trade.contract.symbol} - {fill.execution.shares} units. Squared to Current Position of 0!"
            )
            return
        new_avg_price = current_position["avg_price"]
        if (current_position["quantity"] > 0 and trade.order.action == "BUY") or (
            current_position["quantity"] < 0 and trade.order.action == "SELL"
        ):
            current_abs_quantity = abs(current_position["quantity"])
            current_total_value = current_position["avg_price"] * current_abs_quantity
            additional_value = fill.execution.shares * fill.execution.price
            new_avg_price = (current_total_value + additional_value) / (
                fill.execution.shares + current_abs_quantity
            )
        current_position["quantity"] = new_quantity
        current_position["avg_price"] = new_avg_price

        await self.async_current_stock_positions.update(current_position)
        self.logger.info(
            f"Position updated: {trade.contract.symbol} - {trade.contract.secType} - {fill.execution.shares} units."
        )

    @async_current_option_position_wrapper
    @async_option_transaction_wrapper
    async def _new_execution_update_option(self, trade: Trade, fill: Fill) -> None:
        # ------------- ON PROBATION CODE, NOT SURE YET OF FULL VALIDITY --------
        if trade.contract.secType not in ("OPT", "BAG"):
            return
        if trade.contract.secType == "BAG":
            print(trade, fill)
            contract1 = trade.contract.comboLegs[0]
            contract2 = trade.contract.comboLegs[1]
            option_contract1 = Option(conId=contract1.conId)
            option_contract2 = Option(conId=contract2.conId)
            await self._qualify_contracts_async(option_contract1)
            await self._qualify_contracts_async(option_contract2)

            price = await self.get_current_price(option_contract1)
            fill1 = price
            fill2 = fill.execution.price - fill1

            trade.contract = option_contract1
            fill.execution.price = fill1
            fill.commissionReport.commission = fill.commissionReport.commission / 2
            await self._new_execution_update_option(trade, fill)

            trade.contract = option_contract2
            fill.execution.price = fill2
            await self._new_execution_update_option(trade, fill)
            # await self.async_option_transactions.create(
            #     {
            #         "stock": trade.contract.symbol,
            #         "strategy": self.strategy,
            #         "expiry": option_contract.lastTradeDateOrContractMonth,
            #         "strike": option_contract.strike,
            #         "multiplier": float(option_contract.multiplier),
            #         "option_type": right,
            #         "time": fill.time,
            #         "price_transacted": price,
            #         "fees": fill.commissionReport.commission / 2,
            #         "quantity": 100
            #         * (-1 if trade.order.action == "SELL" else 1),
            #     }
            # )
            # await self.async_option_transactions.create(
            #     {
            #         "stock": trade.contract.symbol,
            #         "strategy": self.strategy,
            #         "expiry": option_contract.lastTradeDateOrContractMonth,
            #         "strike": option_contract.strike,
            #         "multiplier": float(option_contract.multiplier),
            #         "option_type": other_right,
            #         "time": fill.time,
            #         "price_transacted": fill.execution.price - price,
            #         "fees": fill.commissionReport.commission / 2,
            #         "quantity": 100
            #         * (-1 if trade.order.action == "SELL" else 1),
            #     }
            # )

        await self.async_option_transactions.create(
            {
                "stock": trade.contract.symbol,
                "strategy": self.strategy,
                "expiry": trade.contract.lastTradeDateOrContractMonth,
                "strike": trade.contract.strike,
                "multiplier": float(trade.contract.multiplier),
                "option_type": cast(OptionType, trade.contract.right[0]),
                "time": fill.time,
                "price_transacted": fill.execution.price,
                "fees": fill.commissionReport.commission / 2,
                "quantity": fill.execution.shares
                * (-1 if trade.order.action == "SELL" else 1),
            }
        )
        current_positions = await self.async_current_option_positions.read(
            {
                "stock": trade.contract.symbol,
                "strategy": self.strategy,
                "expiry": trade.contract.lastTradeDateOrContractMonth,
                "strike": trade.contract.strike,
                "multiplier": float(trade.contract.multiplier),
                "option_type": cast(OptionType, trade.contract.right[0]),
            }
        )
        if not current_positions:
            await self.async_current_option_positions.create(
                {
                    "stock": trade.contract.symbol,
                    "strategy": self.strategy,
                    "expiry": trade.contract.lastTradeDateOrContractMonth,
                    "strike": trade.contract.strike,
                    "multiplier": float(trade.contract.multiplier),
                    "option_type": cast(OptionType, trade.contract.right[0]),
                    "avg_price": fill.execution.price,
                    "quantity": fill.execution.shares
                    * (-1.0 if trade.order.action == "SELL" else 1.0),
                }
            )
            return
        current_position = current_positions[0]
        new_quantity = current_position["quantity"] + (
            fill.execution.shares * (-1 if trade.order.action == "SELL" else 1)
        )
        if new_quantity == 0:
            await self.async_current_option_positions.delete(
                {
                    "stock": trade.contract.symbol,
                    "strategy": self.strategy,
                    "expiry": trade.contract.lastTradeDateOrContractMonth,
                    "strike": trade.contract.strike,
                    "multiplier": float(trade.contract.multiplier),
                    "option_type": cast(OptionType, trade.contract.right[0]),
                }
            )
            self.logger.info(
                f"Position updated: {trade.contract.symbol} - {fill.execution.shares} units. Squared to Current Position of 0!"
            )
            return
        new_avg_price = current_position["avg_price"]
        if (current_position["quantity"] > 0 and trade.order.action == "BUY") or (
            current_position["quantity"] < 0 and trade.order.action == "SELL"
        ):
            current_abs_quantity = abs(current_position["quantity"])
            current_total_value = current_position["avg_price"] * current_abs_quantity
            additional_value = fill.execution.shares * fill.execution.price
            new_avg_price = (current_total_value + additional_value) / (
                fill.execution.shares + current_abs_quantity
            )
        current_position["quantity"] = new_quantity
        current_position["avg_price"] = new_avg_price

        await self.async_current_option_positions.update(current_position)
        self.logger.info(
            f"Position updated: {trade.contract.symbol} - {trade.contract.secType} - {fill.execution.shares} units."
        )

    # FOR LIVE STRATEGIES #
    # @Override
    @async_open_stock_orders_wrapper
    @async_open_option_orders_wrapper
    @async_current_stock_position_wrapper
    @async_current_option_position_wrapper
    async def update_completed_orders(
        self, validate_current_position: bool = False
    ) -> bool:
        """
        Updates the Transaction and CurrentPosition DB for the completed orders since last session.

        validate_current_position: bool
            - Validates that CurrentPosition matches position fetched from Broker API
        """
        completed_trades: List[Trade] = await self.ib.reqCompletedOrdersAsync(
            True
        )  # True for api only
        for trade in completed_trades:
            if (
                trade.orderStatus.status in ("Cancelled", "Filled", "Inactive")
                and len(trade.log) == 0
            ):
                continue

            if trade.contract.secType == "STK":
                if not await self.async_open_stock_orders.read(
                    {
                        "order_id": trade.order.orderId,
                        "stock": trade.contract.symbol,
                        "strategy": self.strategy,
                        "time": trade.log[0].time,
                    }
                ):
                    continue
                if trade.orderStatus.status in ("Cancelled", "Filled", "Inactive"):
                    await self.async_open_stock_orders.delete(
                        {
                            "order_id": trade.order.orderId,
                            "stock": trade.contract.symbol,
                            "strategy": self.strategy,
                            "time": trade.log[0].time,
                        }
                    )
            elif trade.contract.secType == "OPT":
                if not await self.async_open_option_orders.read(
                    {
                        "order_id": trade.order.orderId,
                        "stock": trade.contract.symbol,
                        "strategy": self.strategy,
                        "expiry": trade.contract.lastTradeDateOrContractMonth,
                        "strike": trade.contract.strike,
                        "multiplier": float(trade.contract.multiplier),
                        "option_type": cast(OptionType, trade.contract.right[0]),
                        "time": trade.log[0].time,
                    }
                ):
                    continue
                if trade.orderStatus.status in ("Cancelled", "Filled", "Inactive"):
                    await self.async_open_option_orders.delete(
                        {
                            "order_id": trade.order.orderId,
                            "stock": trade.contract.symbol,
                            "strategy": self.strategy,
                            "expiry": trade.contract.lastTradeDateOrContractMonth,
                            "strike": trade.contract.strike,
                            "multiplier": float(trade.contract.multiplier),
                            "option_type": cast(OptionType, trade.contract.right[0]),
                            "time": trade.log[0].time,
                        }
                    )

            # NEED TO CHECK AND DEBUG THIS - MAYBE ALSO ADD VALIDATION FOR AVGCOST???
            for fill in trade.fills:
                fill_exists = False
                if trade.contract.secType == "STK":
                    fill_exists = (
                        len(
                            await self.async_stock_transactions.read(
                                {
                                    "stock": trade.contract.symbol,
                                    "strategy": self.strategy,
                                    "time": fill.time,
                                }
                            )
                        )
                        > 0
                    )
                elif trade.contract.secType == "OPT":
                    fill_exists = (
                        len(
                            await self.async_option_transactions.read(
                                {
                                    "stock": trade.contract.symbol,
                                    "strategy": self.strategy,
                                    "expiry": trade.contract.lastTradeDateOrContractMonth,
                                    "strike": trade.contract.strike,
                                    "multiplier": float(trade.contract.multiplier),
                                    "option_type": cast(
                                        OptionType, trade.contract.right[0]
                                    ),
                                    "time": fill.time,
                                }
                            )
                        )
                        > 0
                    )
                if fill_exists:
                    continue

                await self._new_execution_update(trade, fill)

        # BUGGY: DUE TO UPDATING OF PRIMARY KEYS
        if validate_current_position:
            ib_positions = self.ib.positions()
            local_positions = {
                position["stock"]: position
                for position in await self.async_current_stock_positions.read(None)
            }
            for position in ib_positions:
                if (
                    position.contract.symbol not in local_positions
                    or position.position
                    != local_positions[position.contract.symbol]["quantity"]
                ):
                    return False
        return True

    # @Override
    async def send_orders(self, orders: List[FullOrder]) -> None:
        """Async method to send multiple orders concurrently."""
        (
            await self._possibly_reset_once(
                lambda: self.ib.qualifyContractsAsync(
                    *[
                        order["contract"]
                        for order in orders
                        if order["contract"].secIdType != "BAG"
                    ]
                )
            )
        )
        for order in orders:
            trade = self.ib.placeOrder(order["contract"], order["order"])
            self.ib.waitOnUpdate()

            if trade.orderStatus.status == "Cancelled":
                self.logger.error(f"Order Submission Failed: {trade.log[-1].message}")
            else:
                self.logger.info(
                    f"Order submitted: {trade.order} for {trade.contract.symbol}"
                )

    @async_current_stock_position_wrapper
    @async_open_stock_orders_wrapper
    async def newOrderEvent(self, trade: Trade) -> None:
        """
        Update status of stop_price for position if it is STOP order
        """
        open_order = await self.async_open_stock_orders.read(
            {
                "order_id": trade.order.orderId,
                "stock": trade.contract.symbol,
                "strategy": self.strategy,
                "time": trade.log[0].time,
            }
        )
        if open_order:
            self.logger.error(f"Order alr exists: {trade.order.orderId}")
            return

        await self.async_open_stock_orders.create(
            {
                "order_id": trade.order.orderId,
                "stock": trade.contract.symbol,
                "strategy": self.strategy,
                "time": trade.log[0].time,
                "quantity": trade.order.totalQuantity
                * (-1 if trade.order.action == "SELL" else 1),
            }
        )

        if trade.order.orderType == "STP":
            stop_price = trade.order.auxPrice
            # quantity = trade.order.totalQuantity
            # BELOW BLOCK BUGGY FOR MULTIPLE ORDERS ON SINGLE STOCK
            # current_position_stock = self.current_position.read({'stock': trade.contract.symbol})
            # if not current_position_stock:
            #     self.logger.error(f"Error: No position found for stock {trade.contract.symbol}!")
            #     return
            # if (current_position_stock[0].quantity != quantity):
            #     self.logger.error('Error: Quantity listed for stop loss order does not align with current position!')
            await self.async_current_stock_positions.update(
                {
                    "stock": trade.contract.symbol,
                    "strategy": self.strategy,
                    "stop_limit": stop_price,
                }
            )
        self.logger.info(f"New order placed: {trade.order.orderId}")

    @async_current_stock_position_wrapper
    @async_stock_transaction_wrapper
    @async_open_stock_orders_wrapper
    async def execDetailsEvent(self, trade: Trade, fill: Fill) -> None:
        """
        Update DB when position updated
        """
        open_orders_exists = False
        if trade.contract.secType == "STK":
            open_orders_exists = (
                len(
                    await self.async_open_stock_orders.read(
                        {
                            "order_id": fill.execution.orderId,
                            "stock": trade.contract.symbol,
                            "strategy": self.strategy,
                            "time": trade.log[0].time,
                        }
                    )
                )
                > 0
            )
            if trade.orderStatus.status in ("Cancelled", "Filled", "Inactive"):
                await self.async_open_stock_orders.delete(
                    {
                        "order_id": fill.execution.orderId,
                        "stock": trade.contract.symbol,
                        "strategy": self.strategy,
                        "time": trade.log[0].time,
                    }
                )
        elif trade.contract.secType == "OPT":
            open_orders_exists = (
                len(
                    await self.async_open_option_orders.read(
                        {
                            "order_id": fill.execution.orderId,
                            "stock": trade.contract.symbol,
                            "strategy": self.strategy,
                            "expiry": trade.contract.lastTradeDateOrContractMonth,
                            "strike": trade.contract.strike,
                            "multiplier": float(trade.contract.multiplier),
                            "option_type": cast(OptionType, trade.contract.right[0]),
                            "time": trade.log[0].time,
                        }
                    )
                )
                > 0
            )
            if trade.orderStatus.status in ("Cancelled", "Filled", "Inactive"):
                await self.async_open_option_orders.delete(
                    {
                        "order_id": fill.execution.orderId,
                        "stock": trade.contract.symbol,
                        "strategy": self.strategy,
                        "expiry": trade.contract.lastTradeDateOrContractMonth,
                        "strike": trade.contract.strike,
                        "multiplier": float(trade.contract.multiplier),
                        "option_type": cast(OptionType, trade.contract.right[0]),
                        "time": trade.log[0].time,
                    }
                )
        if not open_orders_exists:
            self.logger.error(
                "Error: Received Transaction for Missing Entry in OpenOrder"
            )

        await self._new_execution_update(trade, fill)

    async def run_live_strategies(self) -> None:
        if self.stock_strategy is not None:
            await self.run_live_strategies_for_stocks()
        if self.option_strategy is not None:
            await self.run_live_strategies_for_options()

    async def check_live_subs(self) -> None:
        time_now = datetime.now()
        for contract, last_updated_and_fn in self.live_data_last_updated.items():
            if time_now - last_updated_and_fn[0] >= timedelta(minutes=5, seconds=10):
                bars = self.ib.reqRealTimeBars(contract, 5, "TRADES", True)
                bar_updater_fn = last_updated_and_fn[1]
                self.live_data_last_updated[contract] = (datetime.now(), bar_updater_fn)
                bars.updateEvent += bar_updater_fn
                IBKR.price_events[contract] = bars.updateEvent

    # FOR LIVE STRATEGIES #
    async def run_live_strategies_for_stocks(self) -> None:
        assert self.stock_strategy is not None

        for contract in await self.stock_strategy.get_stocks(self):
            await self._qualify_contracts_async(contract)
            if contract not in self.live_data:
                self.live_data[contract] = []
            if contract in IBKR.price_events:
                IBKR.price_events[contract] += (
                    lambda bars, hasNewBar: self.stockBarUpdateEvent(
                        contract, contract.symbol, bars, hasNewBar
                    )
                )
                continue

            self.logger.info(f"Requesting live data for {contract}")
            bars = self.ib.reqRealTimeBars(contract, 5, "TRADES", True)
            IBKR.price_events[contract] = bars.updateEvent

            async def bar_updater_fn(bars: RealTimeBarList, hasNewBar: bool) -> None:
                await self.stockBarUpdateEvent(
                    contract, contract.symbol, bars, hasNewBar
                )

            self.live_data_last_updated[contract] = (datetime.now(), bar_updater_fn)
            bars.updateEvent += bar_updater_fn

    @async_historical_data_wrapper
    async def stockBarUpdateEvent(
        self, contract: Contract, stock: str, bars: RealTimeBarList, hasNewBar: bool
    ) -> None:
        self.live_data_last_updated[contract] = (
            datetime.now(),
            self.live_data_last_updated[contract][1],
        )

        for bar in bars:
            self.live_data[contract].append(bar)
            if (bar.time.minute + 1) % 5 == 0 and bar.time.second == 55:
                self.live_data[contract].pop()
                open, high, low, close = bar.open_, bar.high, bar.low, bar.close
                volume = bar.volume
                while self.live_data[contract]:
                    earlier_bar = self.live_data[contract].pop()
                    high, low = max(high, earlier_bar.high), min(low, earlier_bar.low)
                    volume += earlier_bar.volume
                    if (
                        earlier_bar.time.minute % 5 == 0
                        and earlier_bar.time.second == 0
                    ):
                        open = earlier_bar.open_
                        await self.async_historical_data.create_or_update(
                            {
                                "stock": stock,
                                "time": earlier_bar.time,
                                "open": open,
                                "high": high,
                                "low": low,
                                "close": close,
                                "volume": int(volume),
                            }
                        )
                        self.logger.info(
                            f"Updating orders for {self.strategy} for stocks"
                        )
                        await update_target_position_and_send_orders_for_broker(self)
                        break
            bars.clear()

    async def run_live_strategies_for_options(self) -> None:
        assert self.option_strategy is not None

        for contract in await self.option_strategy.get_stocks(self):
            if contract not in self.live_options_data:
                self.live_options_data[contract] = []
            if contract not in self.live_data:
                self.live_data[contract] = []

            if contract in IBKR.price_events:
                IBKR.price_events[contract] += (
                    lambda bars, hasNewBar: self.stockBarUpdateEvent(
                        contract, contract.symbol, bars, hasNewBar
                    )
                )
                continue

            self.logger.info(
                f"Requesting live data for implied volatility of: {contract}"
            )
            # ticker_live = self.ib.reqMktData(
            #     contract,
            #     genericTickList="106",
            #     snapshot=False,
            #     regulatorySnapshot=False,
            # )
            # ticker_live.updateEvent += lambda ticker: self.optionBarUpdateEvent(
            #     contract, ticker
            # )
            bars = self.ib.reqRealTimeBars(contract, 5, "TRADES", True)
            IBKR.price_events[contract] = bars.updateEvent

            async def bar_updater_fn(bars: RealTimeBarList, hasNewBar: bool) -> None:
                await self.stockBarUpdateEvent(
                    contract, contract.symbol, bars, hasNewBar
                )

            self.live_data_last_updated[contract] = (datetime.now(), bar_updater_fn)
            bars.updateEvent += bar_updater_fn

    @async_historical_volatility_data_wrapper
    async def optionBarUpdateEvent(self, contract: Contract, ticker: Ticker) -> None:
        if ticker.time is None:
            return
        self.live_data_last_updated[contract] = (
            datetime.now(),
            self.live_data_last_updated[contract][1],
        )

        eastern = pytz.timezone("US/Eastern")
        if not np.isnan(ticker.impliedVolatility):
            self.live_options_data[contract].append(
                (ticker.time.astimezone(eastern), ticker.impliedVolatility)
            )
        if len(self.live_options_data[contract]) == 1:
            return
        if len(self.live_options_data[contract]) == 0:
            print(f"WTF, {ticker}")
            return

        initial_interval = self.live_options_data[contract][0][0]
        assert initial_interval is not None
        initial_interval = initial_interval.replace(second=0, microsecond=0).astimezone(
            eastern
        ) - timedelta(minutes=initial_interval.minute % 5)
        current_interval = self.live_options_data[contract][-1][0]
        assert current_interval is not None
        current_interval = current_interval.replace(second=0, microsecond=0).astimezone(
            eastern
        ) - timedelta(minutes=current_interval.minute % 5)

        interval_to_update = initial_interval
        collected_data: List[float] = []
        while initial_interval != current_interval:
            collected_data.append(self.live_options_data[contract].pop(0)[1])
            initial_interval = self.live_options_data[contract][0][0]
            assert initial_interval is not None
            initial_interval = initial_interval.replace(
                second=0, microsecond=0
            ).astimezone(eastern) - timedelta(minutes=initial_interval.minute % 5)

        if len(collected_data) > 0:
            open = collected_data[0]
            high = max(collected_data)
            low = min(collected_data)
            close = collected_data[-1]

            if (
                eastern.localize(
                    datetime(
                        interval_to_update.year,
                        interval_to_update.month,
                        interval_to_update.day,
                        16,
                        0,
                        0,
                    )
                )
                >= interval_to_update
                >= eastern.localize(
                    datetime(
                        interval_to_update.year,
                        interval_to_update.month,
                        interval_to_update.day,
                        9,
                        30,
                        0,
                    )
                )
            ):
                await self.async_historical_volatility_data.create_or_update(
                    {
                        "stock": contract.symbol,
                        "time": interval_to_update,
                        "open": open,
                        "high": high,
                        "low": low,
                        "close": close,
                    }
                )
                self.logger.info(f"Updating orders for {self.strategy} for options")
                await update_target_position_and_send_orders_for_broker(self)

    async def update_historical_data_till_today(self) -> None:
        if self.stock_strategy:
            await self.stock_strategy.update_historical_data_to_present(self)
        if self.option_strategy:
            await self.option_strategy.update_historical_data_to_present(self)
