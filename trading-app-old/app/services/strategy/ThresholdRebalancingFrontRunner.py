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
    HistoricalThresholdRebalancingDict,
    Status,
    TargetStockPositionsDict,
)
from typing import List, Dict, Literal, TypedDict, cast, Any
from app.services.broker.DataBroker import DataBroker, FullOrder
from ib_async.contract import ContFuture, Contract, Stock
from ib_async.order import MarketOrder, StopOrder
from datetime import date, datetime, timedelta, timezone
import pytz
import pandas_market_calendars as mcal
from decimal import Decimal, ROUND_HALF_UP

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


ThresholdColumnType = Literal[
    "threshold_equity_prop_000",
    "threshold_equity_prop_001",
    "threshold_equity_prop_002",
    "threshold_equity_prop_003",
    "threshold_equity_prop_004",
    "threshold_equity_prop_005",
    "threshold_equity_prop_006",
    "threshold_equity_prop_007",
    "threshold_equity_prop_008",
    "threshold_equity_prop_009",
    "threshold_equity_prop_010",
    "threshold_equity_prop_011",
    "threshold_equity_prop_012",
    "threshold_equity_prop_013",
    "threshold_equity_prop_014",
    "threshold_equity_prop_015",
    "threshold_equity_prop_016",
    "threshold_equity_prop_017",
    "threshold_equity_prop_018",
    "threshold_equity_prop_019",
    "threshold_equity_prop_020",
    "threshold_equity_prop_021",
    "threshold_equity_prop_022",
    "threshold_equity_prop_023",
    "threshold_equity_prop_024",
    "threshold_equity_prop_025",
]


class ThresholdRebalancingFrontRunner(StrategyClass):
    logger = CustomLogger("ThresholdRebalancingFrontRunner")
    strategy = "threshold_rebalancing_front_runner"
    eastern = pytz.timezone("US/Eastern")
    initial_equity_weight = 0.60

    @async_strategy_wrapper
    @staticmethod
    async def create_strategy(strategy: AsyncStrategyCRUD) -> None:
        strategy_exists = await strategy.read(
            {"strategy": ThresholdRebalancingFrontRunner.strategy}
        )
        if len(strategy_exists) > 0:
            return

        await strategy.create(
            {
                "strategy": ThresholdRebalancingFrontRunner.strategy,
                "capital": 100000,
                "initial_capital": 100000,
                "status": Status.active,
            }
        )

    # Override
    @async_historical_data_wrapper
    @async_strategy_wrapper
    @async_historical_threshold_rebalancing_wrapper
    @staticmethod
    async def get_weights(
        historical_threshold_rebalancing: AsyncHistoricalThresholdRebalancingCRUD,
        strategy: AsyncStrategyCRUD,
        historical_data: AsyncHistoricalDataCRUD,
        broker: DataBroker,
    ) -> List[TargetStockPositionsDict]:
        """ """
        await ThresholdRebalancingFrontRunner.update_historical_data_to_present(broker)
        strategy_row = await strategy.read(
            {"strategy": ThresholdRebalancingFrontRunner.strategy}
        )
        assert len(strategy_row) > 0
        capital = strategy_row[0]["capital"]

        if ThresholdRebalancingFrontRunner.is_first_month_of_quarter(
            datetime.now(timezone.utc)
        ):
            es_last_bar = await historical_data.read_stock("FUT:ES")
            zn_last_bar = await historical_data.read_stock("FUT:ZN")
            return [
                {
                    "stock": "FUT:ES",
                    "strategy": ThresholdRebalancingFrontRunner.strategy,
                    "stop_limit": 0,
                    "avg_price": es_last_bar[0]["close"],
                    "quantity": 0,
                },
                {
                    "stock": "FUT:ZN",
                    "strategy": ThresholdRebalancingFrontRunner.strategy,
                    "stop_limit": 0,
                    "avg_price": zn_last_bar[0]["close"],
                    "quantity": 0,
                },
            ]

        last_bar = await historical_threshold_rebalancing.get_last_equity_weights()
        implied_weight = -(
            last_bar["threshold_equity_prop_000"]
            + last_bar["threshold_equity_prop_001"]
            + last_bar["threshold_equity_prop_002"]
            + last_bar["threshold_equity_prop_003"]
        ) / (4 * 0.015)
        last_es = await historical_data.read_stock("FUT:ES", 1)
        last_zn = await historical_data.read_stock("FUT:ZN", 1)
        assert len(last_es) > 0
        assert len(last_zn) > 0

        return [
            {
                "stock": "FUT:ES",
                "strategy": ThresholdRebalancingFrontRunner.strategy,
                "stop_limit": last_es[0]["close"] * 0.5,
                "avg_price": last_es[0]["close"],
                "quantity": int(capital * implied_weight),
            },
            {
                "stock": "FUT:ZN",
                "strategy": ThresholdRebalancingFrontRunner.strategy,
                "stop_limit": last_zn[0]["close"] * 1.5,
                "avg_price": last_zn[0]["close"],
                "quantity": -int(capital * implied_weight),
            },
        ]

    @staticmethod
    def get_buy_price(current_prices: Dict[str, float]) -> Dict[str, float]:
        return {"SPY": current_prices["SPY"]}

    @staticmethod
    def get_sell_price(current_prices: Dict[str, float]) -> Dict[str, float]:
        return {"SPY": current_prices["SPY"] * 1.01}

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

        order_id = broker.ib.client.getReqId()
        order = MarketOrder("BUY", quantity, orderId=order_id)
        order.transmit = False

        attached_stop_limit = StopOrder(
            "SELL", quantity, round(0.9 * avg_price * 20) / 20
        )
        attached_stop_limit.transmit = True
        attached_stop_limit.parentId = order.orderId

        orders.append({"contract": contract, "order": order})
        orders.append({"contract": contract, "order": attached_stop_limit})

        if quantity_to_insure != 0:
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
    @async_historical_threshold_rebalancing_wrapper
    @staticmethod
    async def update_historical_data_to_present(
        historical_portfolio_rebalancing: AsyncHistoricalThresholdRebalancingCRUD,
        historical_data: AsyncHistoricalDataCRUD,
        broker: DataBroker,
    ) -> None:
        # From backtests, longest running time where there was no rebalancing of portfolio is 11610 5 min bars / ~129 days
        # enough_es_data = await historical_data.has_at_least_n_rows(
        #     "FUT:ES", int(252 / 4 * 90)
        # )
        def get_column_name(x: float) -> ThresholdColumnType:
            dec = Decimal(str(x))
            return cast(
                ThresholdColumnType,
                (
                    "threshold_equity_prop_"
                    + format(
                        dec.quantize(Decimal("0.001"), rounding=ROUND_HALF_UP), "f"
                    )[2:]
                ),
            )

        # Get number of bars expected from last trading day and check if in db alr
        nyse = mcal.get_calendar("NYSE")
        trading_days = nyse.valid_days(
            datetime.now() - timedelta(days=10),
            datetime.now(),
            tz=pytz.timezone("US/Eastern"),
        )
        last_trading_day = trading_days[-1]
        time_tdy = datetime.now(timezone.utc).astimezone(
            ThresholdRebalancingFrontRunner.eastern
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
            ThresholdRebalancingFrontRunner.logger.info("Requesting data for ES")
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
                ThresholdRebalancingFrontRunner.logger.error(
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
            ThresholdRebalancingFrontRunner.logger.info("Requesting data for ZN")
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
                ThresholdRebalancingFrontRunner.logger.error(
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
            await historical_portfolio_rebalancing.read_portfolio_time_count(
                last_trading_day
            )
        ) >= expected_bars
        if not enough_rebalancing_data:
            ThresholdRebalancingFrontRunner.logger.info(
                "Updating data for Rebalancing Portfolios"
            )
            last_bar = await historical_portfolio_rebalancing.get_last_entry()
            if last_bar is None:
                # Buffer for max no. of days when proportion did not reset to 0.6 for threshold strategy
                last_time = datetime.now(timezone.utc).astimezone(
                    ThresholdRebalancingFrontRunner.eastern
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

            new_threshold_values: Dict[float, List[ValTime]] = {}
            for threshold_int in range(0, 26):
                threshold_val = threshold_int / 1000
                new_threshold_values[threshold_val] = (
                    ThresholdRebalancingFrontRunner.get_new_threshold_values(
                        ThresholdRebalancingFrontRunner.initial_equity_weight
                        if last_bar is None
                        else last_bar[get_column_name(threshold_val)],
                        es_vals,
                        zn_vals,
                        last_time if last_bar is None else last_bar["time"],
                        threshold_val,
                    )
                )
            # new_calendar_values = ThresholdRebalancingFrontRunner.get_new_calendar_values(
            #     ThresholdRebalancingFrontRunner.initial_equity_weight
            #     if last_bar is None
            #     else last_bar["calendar_equity_prop"],
            #     es_vals,
            #     zn_vals,
            #     last_time if last_bar is None else last_bar["time"],
            # )
            assert all(
                [
                    len(new_threshold_values[i]) == len(new_threshold_values[0.01])
                    for i in new_threshold_values
                ]
            )
            # assert len(new_threshold_values[0.01]) == len(new_calendar_values)

            new_bars: List[HistoricalThresholdRebalancingDict] = []
            for ind in range(len(new_threshold_values[0.01])):
                next_val: Dict[str, Any] = {
                    "time": new_threshold_values[0.01][ind]["time"]
                }
                for threshold_int in range(0, 26):
                    threshold_val = threshold_int / 1000
                    next_val[get_column_name(threshold_val)] = new_threshold_values[
                        threshold_val
                    ][ind]["value"]

                # next_val["calendar_equity_prop"] = new_calendar_values[ind]["value"]
                new_bars.append(cast(HistoricalThresholdRebalancingDict, next_val))

            await historical_portfolio_rebalancing.create_or_update_all(new_bars)

    @staticmethod
    def get_new_threshold_values(
        initial_val: float,
        equity_val: List[ValTime],
        bond_val: List[ValTime],
        first_time_to_ignore: datetime,
        threshold_val: float,
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

            if abs(previous_threshold_value - 0.6) > threshold_val:
                new_values.append(
                    {
                        "time": equity_val[ind]["time"],
                        "value": ThresholdRebalancingFrontRunner.initial_equity_weight,
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

            if ThresholdRebalancingFrontRunner.is_last_us_business_day_of_month(
                equity_val[ind]["time"]
            ):
                new_values.append(
                    {
                        "time": equity_val[ind]["time"],
                        "value": ThresholdRebalancingFrontRunner.initial_equity_weight,
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
        eastern_time = time.astimezone(ThresholdRebalancingFrontRunner.eastern)
        year, month = eastern_time.year, eastern_time.month

        last_day = 31
        if month != 12:
            last_day = (date(year, month + 1, 1) - timedelta(days=1)).day

        return time.day == last_day

    @staticmethod
    def is_first_month_of_quarter(time: datetime) -> bool:
        return time.astimezone(ThresholdRebalancingFrontRunner.eastern).month in (
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
