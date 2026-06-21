from app.services.models.AsyncModelsCRUD import (
    AsyncCurrentStockPositionsCRUD,
    AsyncHistoricalDataCRUD,
    AsyncStrategyCRUD,
    AsyncHistoricalThresholdRebalancingCRUD,
    AsyncHistoricalCalendarRebalancingCRUD,
)
from app.services.strategy.StockStrategy import StockStrategy as StrategyClass
from app.utils.custom_logging import CustomLogger
from app.utils.db import (
    async_with_db_session_for_model,
)
from app.models import (
    CurrentStockPositions,
    HistoricalCalendarRebalancing,
    HistoricalData,
    Strategy,
    HistoricalThresholdRebalancing,
)
from app.models_types import (
    HistoricalCalendarRebalancingDict,
    HistoricalThresholdRebalancingDict,
    Status,
    TargetStockPositionsDict,
)
from typing import List, Dict, TypedDict, cast, Any
from app.services.broker.DataBroker import DataBroker, FullOrder
from ib_async.contract import ContFuture, Contract, Stock
from ib_async.order import MarketOrder, StopOrder
from datetime import date, datetime, timedelta, timezone
import pytz
import pandas_market_calendars as mcal

async_historical_threshold_rebalancing_wrapper = async_with_db_session_for_model(
    AsyncHistoricalThresholdRebalancingCRUD, HistoricalThresholdRebalancing
)
async_historical_calendar_rebalancing_wrapper = async_with_db_session_for_model(
    AsyncHistoricalCalendarRebalancingCRUD, HistoricalCalendarRebalancing
)
async_historical_data_wrapper = async_with_db_session_for_model(
    AsyncHistoricalDataCRUD, HistoricalData
)
async_strategy_wrapper = async_with_db_session_for_model(AsyncStrategyCRUD, Strategy)
async_current_stock_positions_wrapper = async_with_db_session_for_model(
    AsyncCurrentStockPositionsCRUD, CurrentStockPositions
)


class ValTime(TypedDict):
    value: float
    time: datetime


class CalendarRebalancingFrontRunner(StrategyClass):
    strategy = "calendar_rebalancing_front_runner"
    eastern = pytz.timezone("US/Eastern")
    initial_equity_weight = 0.60
    logger = CustomLogger("CalendarRebalancingFrontRunner")

    @async_strategy_wrapper
    @staticmethod
    async def create_strategy(strategy: AsyncStrategyCRUD) -> None:
        strategy_exists = await strategy.read(
            {"strategy": CalendarRebalancingFrontRunner.strategy}
        )
        if len(strategy_exists) > 0:
            return

        await strategy.create(
            {
                "strategy": CalendarRebalancingFrontRunner.strategy,
                "capital": 100000,
                "initial_capital": 100000,
                "status": Status.active,
            }
        )

    # Override
    @async_current_stock_positions_wrapper
    @async_historical_data_wrapper
    @async_strategy_wrapper
    @async_historical_calendar_rebalancing_wrapper
    @staticmethod
    async def get_weights(
        historical_calendar_rebalancing: AsyncHistoricalCalendarRebalancingCRUD,
        strategy: AsyncStrategyCRUD,
        historical_data: AsyncHistoricalDataCRUD,
        current_stock_positions: AsyncCurrentStockPositionsCRUD,
        broker: DataBroker,
    ) -> List[TargetStockPositionsDict]:
        """ """
        await CalendarRebalancingFrontRunner.update_historical_data_to_present(broker)
        strategy_row = await strategy.read(
            {"strategy": CalendarRebalancingFrontRunner.strategy}
        )
        assert len(strategy_row) > 0
        capital = strategy_row[0]["capital"]

        time_now = datetime.now(timezone.utc).astimezone(
            CalendarRebalancingFrontRunner.eastern
        )
        if time_now.day != 1:
            es_last_bar = await historical_data.read_stock("FUT:ES")
            zn_last_bar = await historical_data.read_stock("FUT:ZN")
            return [
                {
                    "stock": "FUT:ES",
                    "strategy": CalendarRebalancingFrontRunner.strategy,
                    "stop_limit": 0,
                    "avg_price": es_last_bar[0]["close"],
                    "quantity": 0,
                },
                {
                    "stock": "FUT:ZN",
                    "strategy": CalendarRebalancingFrontRunner.strategy,
                    "stop_limit": 0,
                    "avg_price": zn_last_bar[0]["close"],
                    "quantity": 0,
                },
            ]

        if time_now.month == 1:
            prev_4 = date(time_now.year - 1, 12, 28)
        else:
            # THIS IS CURRENTLY WRONG?????
            prev_4 = date(time_now.year, time_now.month, 1) - timedelta(days=4)

        bar_to_retrieve = CalendarRebalancingFrontRunner.eastern.localize(
            datetime(prev_4.year, prev_4.month, prev_4.day, 15, 55)
        )
        bar = await historical_calendar_rebalancing.read_exact_time(bar_to_retrieve)
        if bar is None:
            es_last_bar = await historical_data.read_stock("FUT:ES")
            zn_last_bar = await historical_data.read_stock("FUT:ZN")
            return [
                {
                    "stock": "FUT:ES",
                    "strategy": CalendarRebalancingFrontRunner.strategy,
                    "stop_limit": 0,
                    "avg_price": es_last_bar[0]["close"],
                    "quantity": 0,
                },
                {
                    "stock": "FUT:ZN",
                    "strategy": CalendarRebalancingFrontRunner.strategy,
                    "stop_limit": 0,
                    "avg_price": zn_last_bar[0]["close"],
                    "quantity": 0,
                },
            ]

        implied_weight = bar["calendar_equity_prop"] - 0.6
        es_last_bar = await historical_data.read_stock("FUT:ES")
        zn_last_bar = await historical_data.read_stock("FUT:ZN")
        return [
            {
                "stock": "FUT:ES",
                "strategy": CalendarRebalancingFrontRunner.strategy,
                "stop_limit": es_last_bar[0]["close"] * 0.5,
                "avg_price": es_last_bar[0]["close"],
                "quantity": int(capital * implied_weight),
            },
            {
                "stock": "FUT:ZN",
                "strategy": CalendarRebalancingFrontRunner.strategy,
                "stop_limit": zn_last_bar[0]["close"] * 1.5,
                "avg_price": zn_last_bar[0]["close"],
                "quantity": -int(capital * implied_weight),
            },
        ]

    @staticmethod
    async def get_buy_order(
        stock: str,
        broker: DataBroker,
        quantity: int,
        quantity_to_insure: int,
        avg_price: float,
    ) -> List[FullOrder]:
        contract_symbol = stock[4:]
        if contract_symbol == "ES":
            contract = ContFuture("ES", "CME", "USD")
        else:
            contract = ContFuture("ZN", "CBOT", "USD")
        orders: List[FullOrder] = []

        order = MarketOrder("BUY", quantity)
        order.transmit = False

        attached_stop_limit = StopOrder(
            "SELL", quantity, round(0.9 * avg_price * 20) / 20
        )
        attached_stop_limit.transmit = True
        attached_stop_limit.parentId = order.orderId

        orders.append({"contract": contract, "order": order})
        orders.append({"contract": contract, "order": attached_stop_limit})

        stop_limit = StopOrder(
            "SELL", quantity_to_insure, round(0.9 * avg_price * 20) / 20
        )
        # Stop Limit for Current Order
        orders.append({"contract": contract, "order": stop_limit})
        return orders

    @staticmethod
    async def get_sell_order(
        stock: str, broker: DataBroker, quantity: int, avg_price: float
    ) -> List[FullOrder]:
        contract_symbol = stock[4:]
        if contract_symbol == "ES":
            contract = ContFuture("ES", "CME", "USD")
        else:
            contract = ContFuture("ZN", "CBOT", "USD")

        orders: List[FullOrder] = []
        order = MarketOrder("SELL", quantity)
        orders.append({"contract": contract, "order": order})
        return orders

    @staticmethod
    async def get_stocks(broker: DataBroker) -> List[Contract]:
        contracts = [
            ContFuture(symbol="ES", exchange="CME", currency="USD"),
            ContFuture(symbol="ZN", exchange="CBOT", currency="USD"),
        ]
        qualified_contracts = await broker.ib.qualifyContractsAsync(*contracts)
        return qualified_contracts

    @async_historical_data_wrapper
    @async_historical_calendar_rebalancing_wrapper
    @staticmethod
    async def update_historical_data_to_present(
        historical_calendar_rebalancing: AsyncHistoricalCalendarRebalancingCRUD,
        historical_data: AsyncHistoricalDataCRUD,
        broker: DataBroker,
    ) -> None:
        # From backtests, longest running time where there was no rebalancing of portfolio is 11610 5 min bars / ~129 days
        # enough_es_data = await historical_data.has_at_least_n_rows(
        #     "FUT:ES", int(252 / 4 * 90)
        # )

        # Get number of bars expected from last trading day and check if in db alr
        nyse = mcal.get_calendar("NYSE")
        trading_days = nyse.valid_days(
            datetime.now() - timedelta(days=10),
            datetime.now(),
            tz=pytz.timezone("US/Eastern"),
        )
        last_trading_day = trading_days[-1]
        time_tdy = datetime.now(timezone.utc).astimezone(
            CalendarRebalancingFrontRunner.eastern
        )
        if last_trading_day.date() == time_tdy.date():
            if (time_tdy.hour == 9 and time_tdy.minute < 35) or time_tdy.hour < 9:
                last_trading_day = trading_days[-2]
                expected_bars = 90
            elif time_tdy.hour == 9:
                expected_bars = (time_tdy.minute - 30) // 5 + 1
            else:
                expected_bars = 7 + (time_tdy.hour - 10) * 12 + time_tdy.minute // 5
        else:
            last_trading_day = trading_days[-2]
            expected_bars = 90
        es_data_count = await historical_data.read_stock_time_count(
            "FUT:ES", last_trading_day
        )
        enough_es_data = es_data_count >= expected_bars

        if not enough_es_data:
            CalendarRebalancingFrontRunner.logger.info("Requesting data for ES")
            bars = await broker.ib.reqHistoricalDataAsync(
                ContFuture("ES", "CME", "USD"),
                endDateTime="",
                durationStr="1 Y" if es_data_count == 0 else "1 D",
                barSizeSetting="5 mins",
                whatToShow="TRADES",
                useRTH=True,
                formatDate=2,
                keepUpToDate=False,
                timeout=600,
            )
            if not bars:
                CalendarRebalancingFrontRunner.logger.error(
                    "No data available for ES up till now"
                )
            else:
                await historical_data.create_or_update_all(
                    [
                        {
                            "stock": "FUT:ES",
                            "time": cast(datetime, bar.date),
                            "open": bar.open,
                            "high": bar.high,
                            "low": bar.low,
                            "close": bar.close,
                            "volume": int(bar.volume),
                        }
                        for bar in bars
                    ]
                )

        zn_data_count = await historical_data.read_stock_time_count(
            "FUT:ZN", last_trading_day
        )
        enough_zn_data = zn_data_count >= expected_bars
        if not enough_zn_data:
            CalendarRebalancingFrontRunner.logger.info("Requesting data for ZN")
            bars = await broker.ib.reqHistoricalDataAsync(
                ContFuture("ZN", "CBOT", "USD"),
                endDateTime="",
                durationStr="1 Y" if zn_data_count == 0 else "1 D",
                barSizeSetting="5 mins",
                whatToShow="TRADES",
                useRTH=True,
                formatDate=2,
                keepUpToDate=False,
                timeout=600,
            )
            if not bars:
                CalendarRebalancingFrontRunner.logger.error(
                    "No data available for ZN up till now"
                )
            else:
                await historical_data.create_or_update_all(
                    [
                        {
                            "stock": "FUT:ZN",
                            "time": cast(datetime, bar.date),
                            "open": bar.open,
                            "high": bar.high,
                            "low": bar.low,
                            "close": bar.close,
                            "volume": int(bar.volume),
                        }
                        for bar in bars
                    ]
                )

        # Check if rebalancing portfolios are updated to present
        enough_rebalancing_data = (
            await historical_calendar_rebalancing.read_portfolio_time_count(
                last_trading_day
            )
        ) >= expected_bars
        if not enough_rebalancing_data:
            CalendarRebalancingFrontRunner.logger.info(
                "Updating data for Rebalancing Portfolios"
            )
            last_bar = await historical_calendar_rebalancing.get_last_entry()
            if last_bar is None:
                # Buffer for max no. of days when proportion did not reset to 0.6 for threshold strategy
                last_time = datetime.now(timezone.utc).astimezone(
                    CalendarRebalancingFrontRunner.eastern
                ) - timedelta(days=200)
            else:
                last_time = last_bar["time"] - timedelta(days=5)

            es = await historical_data.read_stock_time("FUT:ES", last_time)
            zn = await historical_data.read_stock_time("FUT:ZN", last_time)
            es_timesteps = set([i["time"] for i in es])
            zn_timesteps = set([i["time"] for i in zn])
            common_timesteps = es_timesteps & zn_timesteps
            es = [i for i in es if i["time"] in common_timesteps]
            zn = [i for i in zn if i["time"] in common_timesteps]
            assert len(es) == len(zn)
            es_vals: List[ValTime] = [
                {"time": i["time"], "value": i["close"]} for i in es
            ]
            zn_vals: List[ValTime] = [
                {"time": i["time"], "value": i["close"]} for i in zn
            ]

            new_calendar_values = (
                CalendarRebalancingFrontRunner.get_new_calendar_values(
                    CalendarRebalancingFrontRunner.initial_equity_weight
                    if last_bar is None
                    else last_bar["calendar_equity_prop"],
                    es_vals,
                    zn_vals,
                    last_time if last_bar is None else last_bar["time"],
                )
            )

            await historical_calendar_rebalancing.create_or_update_all(
                [
                    {"calendar_equity_prop": i["value"], "time": i["time"]}
                    for i in new_calendar_values
                ]
            )

    @staticmethod
    def get_new_calendar_values(
        initial_val: float,
        equity_val: List[ValTime],
        bond_val: List[ValTime],
        first_time_to_ignore: datetime,
    ) -> List[ValTime]:
        """
        returns the list of new equity values over time, exclusive of the first_time_to_ignore
        """
        assert len(equity_val) == len(bond_val)

        new_values: List[ValTime] = []
        previous_threshold_value = initial_val

        # for ind, (es_val, zn_val) in enumerate(zip(equity_val, bond_val)):
        for ind in range(len(equity_val)):
            if equity_val[ind]["time"] <= first_time_to_ignore:
                continue

            if CalendarRebalancingFrontRunner.is_last_us_business_day_of_month(
                equity_val[ind]["time"]
            ):
                new_values.append(
                    {
                        "time": equity_val[ind]["time"],
                        "value": CalendarRebalancingFrontRunner.initial_equity_weight,
                    }
                )
                continue

            equity_ret, bond_ret = (
                equity_val[ind]["value"] / equity_val[ind - 1]["value"],
                bond_val[ind]["value"] / bond_val[ind - 1]["value"],
            )
            current_equity_value, current_bond_value = (
                previous_threshold_value * equity_ret,
                (1 - previous_threshold_value) * bond_ret,
            )
            current_equity_weight = current_equity_value / (
                current_equity_value + current_bond_value
            )
            new_values.append(
                {"time": equity_val[ind]["time"], "value": current_equity_weight}
            )

            previous_threshold_value = current_equity_weight

        return new_values

    @staticmethod
    def is_last_us_business_day_of_month(time: datetime) -> bool:
        """
        Checks if a given datetime falls on the last
        business day of the month when the US market is open, based on Eastern Time.
        """
        eastern_time = time.astimezone(CalendarRebalancingFrontRunner.eastern)
        year, month = eastern_time.year, eastern_time.month

        last_day = 31
        if month != 12:
            last_day = (date(year, month + 1, 1) - timedelta(days=1)).day

        return time.day == last_day

    @staticmethod
    def is_first_month_of_quarter(time: datetime) -> bool:
        return time.astimezone(CalendarRebalancingFrontRunner.eastern).month in (
            1,
            4,
            7,
            10,
        )

    # EXAMPLES OF MORE COMPLEX ORDERS
    # contract: Contract = Stock(local_order['stock'], 'SMART', 'USD')
    # contract = (await broker._possibly_reset_once(lambda: self.ib.qualifyContractsAsync(contract)))[0]
    #
    # order: Order = LimitOrder('BUY', local_order['quantity'], local_order['price'])
    # if local_order['order_type'] == 'LIMIT':
    #     (
    #         max_pct_vol, start_time, end_time,
    #         allow_past_end_time, no_take_liq
    #     ) = (
    #         local_order['order_details']['max_pct_vol'], local_order['order_details']['start_time'], local_order['order_details']['end_time'],
    #         local_order['order_details']['allow_past_end_time'], local_order['order_details']['no_take_liq']
    #     )
    #     order.algoStrategy = "Vwap"
    #     order.algoParams = []
    #     order.algoParams.append(TagValue("maxPctVol", max_pct_vol))
    #     order.algoParams.append(TagValue("startTime", start_time))
    #     order.algoParams.append(TagValue("endTime", end_time))
    #     order.algoParams.append(TagValue("allowPastEndTime", allow_past_end_time))
    #     order.algoParams.append(TagValue("noTakeLiq", no_take_liq))
    # elif local_order['order_type'] == 'MKT':
    #     order = MarketOrder('BUY' if local_order['quantity'] > 0 else 'SELL', abs(local_order['quantity']))
    # elif local_order['order_type'] == 'STOP':
    #     order = StopOrder('SELL', local_order['quantity'], local_order['price'], )
    # elif local_order['order_type'] == 'SELL':
    #     order = LimitOrder('SELL', local_order['quantity'], local_order['price'])
    # else:
    #     self.logger.error('Order_type not recognised: only LIMIT and STOP recognised currently!')
    #
    # trade = self.ib.placeOrder(contract, order)
    # self.ib.waitOnUpdate()
    #
    # if trade.orderStatus.status == 'Cancelled':
    #     self.logger.error(f"Order Submission Failed: {trade.log[-1].message}")
    #     return {"order_id": None, "status": "Cancelled"}
    # else:
    #     self.logger.info(f"Order submitted: {trade.order.orderId} for {trade.contract.symbol}")
    #
    # return {"order_id": trade.order.orderId, "status": "submitted"}
    #
