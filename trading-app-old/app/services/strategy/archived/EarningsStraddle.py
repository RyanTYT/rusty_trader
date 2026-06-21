from operator import pos
import numpy as np
from datetime import datetime, timedelta, timezone
import pytz
import aiohttp
import pandas_market_calendars as mcal
from app.services.models.AsyncModelsCRUD import (
    AsyncCurrentOptionPositionsCRUD,
    AsyncHistoricalDataCRUD,
    AsyncHistoricalOptionsDataCRUD,
    AsyncHistoricalVolatilityDataCRUD,
    AsyncOptionTransactionsCRUD,
    AsyncPhantomPortfolioValueCRUD,
    AsyncStrategyCRUD,
)
from app.services.strategy.OptionStrategy import OptionStrategy
from typing import Dict, List, Set, Tuple, cast, Any
from app.services.broker.DataBroker import DataBroker, FullOrder

from app.models import (
    HistoricalData,
    HistoricalVolatilityData,
    OptionTransactions,
    Strategy,
    CurrentOptionPositions,
    HistoricalOptionsData,
    PhantomPortfolioValue,
)
from app.services.models.ModelsCRUD import (
    HistoricalDataCRUD,
    HistoricalVolatilityDataCRUD,
    OptionTransactionsCRUD,
    StrategyCRUD,
    CurrentOptionPositionsCRUD,
    HistoricalOptionsDataCRUD,
    PhantomPortfolioValueCRUD,
)
from app.models_types import TargetOptionPositionsDict, OptionType, Status
from app.utils.custom_logging import CustomLogger
from app.utils.db import (
    with_db_session_for_model,
    async_with_db_session_for_model,
)
from ib_async.contract import Option, ComboLeg, Contract, TagValue
from ib_async.order import LimitOrder
import ssl
import asyncio
import certifi
import re


historical_data_wrapper = with_db_session_for_model(HistoricalDataCRUD, HistoricalData)
async_historical_data_wrapper = async_with_db_session_for_model(
    AsyncHistoricalDataCRUD, HistoricalData
)
historical_volatility_data_wrapper = with_db_session_for_model(
    HistoricalVolatilityDataCRUD, HistoricalVolatilityData
)
async_historical_volatility_data_wrapper = async_with_db_session_for_model(
    AsyncHistoricalVolatilityDataCRUD, HistoricalVolatilityData
)
option_transactions_wrapper = with_db_session_for_model(
    OptionTransactionsCRUD, OptionTransactions
)
async_option_transactions_wrapper = async_with_db_session_for_model(
    AsyncOptionTransactionsCRUD, OptionTransactions
)
strategy_wrapper = with_db_session_for_model(StrategyCRUD, Strategy)
async_strategy_wrapper = async_with_db_session_for_model(AsyncStrategyCRUD, Strategy)
current_option_positions_wrapper = with_db_session_for_model(
    CurrentOptionPositionsCRUD, CurrentOptionPositions
)
async_current_option_positions_wrapper = async_with_db_session_for_model(
    AsyncCurrentOptionPositionsCRUD, CurrentOptionPositions
)
historical_options_data_wrapper = with_db_session_for_model(
    HistoricalOptionsDataCRUD, HistoricalOptionsData
)
async_historical_options_data_wrapper = async_with_db_session_for_model(
    AsyncHistoricalOptionsDataCRUD, HistoricalOptionsData
)
phantom_portfolio_value_wrapper = with_db_session_for_model(
    PhantomPortfolioValueCRUD, PhantomPortfolioValue
)
async_phantom_portfolio_value_wrapper = async_with_db_session_for_model(
    AsyncPhantomPortfolioValueCRUD, PhantomPortfolioValue
)
# historical_data_crud_type = TypeVar("historical_data_crud_type", bound=HistoricalDataCRUD)
# current_position_crud_type = TypeVar("current_position_crud_type", bound=CurrentStockPositionsCRUD)


class EarningsStraddle(OptionStrategy):
    """
    Short if price is 1.1 * past 78 5min bars (i.e. past day)
    Close if after day of earnings
    """

    strategy: str = "earnings_straddle"
    to_clear_before_sending: bool = True
    # stocks: List[Dict[str, Any]] = [] # Does not include already open positions
    today_earnings_stocks: Set[str]
    # future_earnings_stocks: List[
    #     Dict[str, Any]
    # ] = []  # Does not include already open positions
    future_earnings_stocks: List[Contract] = []
    possible_options: Dict[Contract, List[Contract]] = {}
    current_options: List[Contract] = []
    calendar = mcal.get_calendar("NYSE")
    eastern = pytz.timezone("US/Eastern")

    @async_strategy_wrapper
    @staticmethod
    async def create_strategy(strategy: AsyncStrategyCRUD) -> None:
        strategy_exists = await strategy.read({"strategy": EarningsStraddle.strategy})
        if len(strategy_exists) > 0:
            return

        await strategy.create(
            {
                "strategy": EarningsStraddle.strategy,
                "capital": 100000,
                "initial_capital": 100000,
                "status": Status.active,
            }
        )

    @async_historical_options_data_wrapper
    @async_historical_data_wrapper
    @async_current_option_positions_wrapper
    @staticmethod
    async def update_bars_till_present(
        current_option_positions: AsyncCurrentOptionPositionsCRUD,
        historical_data: AsyncHistoricalDataCRUD,
        historical_options_data: AsyncHistoricalOptionsDataCRUD,
        broker: DataBroker,
    ) -> bool:
        """
        Updates all data that may be required up till time now

        ----- NOTE -----
        Only need to keep track of new opportunities,
        already opened positions will be trivially separately checked in get_weights()
        """
        if len(EarningsStraddle.future_earnings_stocks) == 0:
            await EarningsStraddle.get_stocks(broker)

        # Update Historical data bars first, then other bars
        expected_bars = []

        end = datetime.now(timezone.utc)
        start = end - timedelta(days=10)
        most_recent_valid_days = EarningsStraddle.calendar.valid_days(
            start, end, tz=EarningsStraddle.eastern
        ).to_pydatetime()[-2:]
        time_now = datetime.now(timezone.utc).astimezone(EarningsStraddle.eastern)
        for day in most_recent_valid_days:
            time = day.replace(hour=9, minute=30)
            for _ in range(79):
                if time_now >= time:
                    expected_bars.append(time)
                time += timedelta(minutes=5)

        expected_bars = expected_bars[-79:]  # Get most recent 78 bars

        is_updated = False

        # Update for current positions
        current_positions = (
            await current_option_positions.get_current_positions_for_strategy(
                EarningsStraddle.strategy
            )
        )
        contracts = [
            Contract(
                symbol=contract["stock"],
                secType="OPT",
                lastTradeDateOrContractMonth=contract["expiry"].replace("-", ""),
                multiplier=str(contract["multiplier"]),
                strike=contract["strike"],
                right=contract["option_type"].value,
                exchange="SMART",
            )
            for contract in current_positions
        ]
        await broker.ib.qualifyContractsAsync(*contracts)

        for local_contract, contract in zip(current_positions, contracts):
            past_prices = await historical_options_data.read_stock(
                local_contract["stock"],
                local_contract["expiry"],
                local_contract["strike"],
                local_contract["multiplier"],
                local_contract["option_type"],
                79,
            )
            if (
                len(past_prices) == 0
                or past_prices[-1]["time"].astimezone(EarningsStraddle.eastern)
                != expected_bars[0]
            ):
                ib_bars = await broker.ib.reqHistoricalDataAsync(
                    contract,
                    endDateTime=datetime.now(timezone.utc),
                    durationStr="4 D",
                    barSizeSetting="5 mins",
                    whatToShow="TRADES",
                    useRTH=True,
                    formatDate=2,
                    keepUpToDate=False,
                )
                await historical_options_data.create_or_update_all(
                    [
                        {
                            "stock": contract.symbol,
                            "expiry": contract.lastTradeDateOrContractMonth,
                            "strike": contract.strike,
                            "multiplier": float(contract.multiplier),
                            "option_type": cast(OptionType, contract.right[0]),
                            "time": cast(datetime, bar.date),
                            "open": bar.open,
                            "high": bar.high,
                            "low": bar.low,
                            "close": bar.close,
                            "volume": bar.volume,
                        }
                        for bar in ib_bars
                    ]
                )

        to_del: List[Contract] = []
        to_del_precise: List[Tuple[Contract, Contract]] = []
        # Update for future opportunities
        for contract in EarningsStraddle.future_earnings_stocks:
            historical_stock = await historical_data.read_stock(contract.symbol, 79)
            if (
                len(historical_stock) == 0
                or historical_stock[-1]["time"].astimezone(EarningsStraddle.eastern)
                != expected_bars[0]
            ):
                ib_bars = await broker.ib.reqHistoricalDataAsync(
                    contract,
                    endDateTime=datetime.now(timezone.utc),
                    durationStr="4 D",
                    barSizeSetting="5 mins",
                    whatToShow="TRADES",
                    useRTH=True,
                    formatDate=2,
                    keepUpToDate=False,
                )
                if len(ib_bars) == 0:
                    to_del.append(contract)
                    continue
                await historical_data.create_or_update_all(
                    [
                        {
                            "stock": contract.symbol,
                            "time": cast(datetime, bar.date),
                            "open": bar.open,
                            "high": bar.high,
                            "low": bar.low,
                            "close": bar.close,
                            "volume": int(bar.volume),
                        }
                        for bar in ib_bars
                    ]
                )
                historical_stock = await historical_data.read_stock(contract.symbol, 79)

            # min_price, max_price = (
            #     min([i["low"] for i in historical_stock]),
            #     max([i["high"] for i in historical_stock]),
            # )
            # print(contract.symbol, min_price, max_price)
            # MAY want to get strike just above and below as well

            for inner_contract in EarningsStraddle.possible_options[contract]:
                # if not (min_price <= float(contract.strike) <= max_price):
                #     continue
                bars = await historical_options_data.read_stock(
                    inner_contract.symbol,
                    inner_contract.lastTradeDateOrContractMonth,
                    inner_contract.strike,
                    float(inner_contract.multiplier),
                    cast(OptionType, inner_contract.right[0]),
                    79,
                )
                if (
                    len(bars) == 0
                    or bars[-1]["time"].astimezone(EarningsStraddle.eastern)
                    != expected_bars[0]
                ):
                    is_updated = True
                    ib_bars = await broker.ib.reqHistoricalDataAsync(
                        contract,
                        endDateTime=datetime.now(timezone.utc),
                        durationStr="4 D",
                        barSizeSetting="5 mins",
                        whatToShow="TRADES",
                        useRTH=True,
                        formatDate=2,
                        keepUpToDate=False,
                    )
                    if len(ib_bars) == 0:
                        to_del_precise.append((contract, inner_contract))
                    await historical_options_data.create_or_update_all(
                        [
                            {
                                "stock": inner_contract.symbol,
                                "expiry": inner_contract.lastTradeDateOrContractMonth,
                                "strike": inner_contract.strike,
                                "multiplier": float(inner_contract.multiplier),
                                "option_type": cast(
                                    OptionType, inner_contract.right[0]
                                ),
                                "time": cast(datetime, bar.date),
                                "open": bar.open,
                                "high": bar.high,
                                "low": bar.low,
                                "close": bar.close,
                                "volume": bar.volume,
                            }
                            for bar in ib_bars
                        ]
                    )
        for i in to_del:
            EarningsStraddle.future_earnings_stocks.remove(i)
        for j in to_del_precise:
            EarningsStraddle.possible_options[j[0]].remove(j[1])

        return is_updated

    @async_historical_options_data_wrapper
    @async_historical_data_wrapper
    @async_current_option_positions_wrapper
    @staticmethod
    async def get_weights(
        current_option_positions: AsyncCurrentOptionPositionsCRUD,
        historical_data: AsyncHistoricalDataCRUD,
        historical_options_data: AsyncHistoricalOptionsDataCRUD,
        broker: DataBroker,
    ) -> List[TargetOptionPositionsDict]:
        # realised boolean doesn't matter because i'm alse updating via real time bars which isn't taken into account for in the function
        await EarningsStraddle.update_bars_till_present(broker)

        target_positions: List[TargetOptionPositionsDict] = []
        current_positions = (
            await current_option_positions.get_current_positions_for_strategy(
                EarningsStraddle.strategy
            )
        )
        to_offload = [
            i
            for i in current_positions
            if i["stock"]
            not in [j.symbol for j in EarningsStraddle.future_earnings_stocks]
            and i["stock"] not in EarningsStraddle.today_earnings_stocks
        ]
        for position in to_offload:
            target_positions.append(
                {
                    "stock": position["stock"],
                    "strategy": EarningsStraddle.strategy,
                    "expiry": position["expiry"],
                    "strike": position["strike"],
                    "multiplier": position["multiplier"],
                    "option_type": position["option_type"],
                    "avg_price": position["strike"],
                    "quantity": 0,
                }
            )

        current_positions_to_track = [
            i
            for i in current_positions
            if i["stock"] in EarningsStraddle.today_earnings_stocks
            or i["stock"] in [i.symbol for i in EarningsStraddle.future_earnings_stocks]
        ]
        for position in current_positions_to_track:
            recent_price = await historical_options_data.read_stock(
                position["stock"],
                position["expiry"],
                position["strike"],
                position["multiplier"],
                position["option_type"],
                1,
            )
            assert len(recent_price) == 1
            if (
                recent_price[0]["close"] < 0.9 * position["avg_price"]
                or recent_price[0]["close"] > 1.05 * position["avg_price"]
            ):
                target_positions.append(
                    {
                        "stock": position["stock"],
                        "strategy": EarningsStraddle.strategy,
                        "expiry": position["expiry"],
                        "strike": position["strike"],
                        "multiplier": position["multiplier"],
                        "option_type": position["option_type"],
                        "avg_price": position["strike"],
                        "quantity": 0,
                    }
                )

        done_straddles: Set[Tuple[str, str, float, str]] = set()
        for contract in EarningsStraddle.future_earnings_stocks:
            if (
                contract.symbol,
                contract.lastTradeDateOrContractMonth,
                contract.strike,
                contract.multiplier,
            ) in done_straddles:
                continue
            # FEELS LIKE THIS COULD POSSIBLY BE OPTIMISED BUT FOR NOW NAIVE ITERATION
            historical_stock = await historical_data.read_stock(contract.symbol, 1)
            assert len(historical_stock) > 0
            price_now = historical_stock[0]["close"]
            possible_contracts = EarningsStraddle.possible_options[contract]
            closest_contract = min(
                possible_contracts, key=lambda x: abs(price_now - x.strike)
            )
            print(possible_contracts, closest_contract)
            call_bars = await historical_options_data.read_stock(
                closest_contract.symbol,
                closest_contract.lastTradeDateOrContractMonth,
                closest_contract.strike,
                float(closest_contract.multiplier),
                OptionType.C,
                79,
            )
            put_bars = await historical_options_data.read_stock(
                closest_contract.symbol,
                closest_contract.lastTradeDateOrContractMonth,
                closest_contract.strike,
                float(closest_contract.multiplier),
                OptionType.P,
                79,
            )
            mean_previous_prices = np.mean(
                [i["close"] + j["close"] for i, j in zip(call_bars[1:], put_bars[1:])]
            )
            # TO UPDATE TO USE OHLC bars
            most_recent_price, most_recent_volume = (
                call_bars[0]["close"] + put_bars[0]["close"],
                call_bars[0]["volume"] + put_bars[0]["volume"],
            )
            if (
                most_recent_price > 1.1 * mean_previous_prices
                and most_recent_volume > 100
            ):
                done_straddles.add(
                    (
                        contract.symbol,
                        contract.lastTradeDateOrContractMonth,
                        contract.strike,
                        contract.multiplier,
                    )
                )
                target_positions.append(
                    {
                        "stock": contract.symbol,
                        "strategy": EarningsStraddle.strategy,
                        "expiry": closest_contract.lastTradeDateOrContractMonth,
                        "strike": closest_contract.strike,
                        "multiplier": float(closest_contract.multiplier),
                        "option_type": OptionType.C,
                        "avg_price": price_now,
                        "quantity": -1,
                    }
                )
                target_positions.append(
                    {
                        "stock": contract.symbol,
                        "strategy": EarningsStraddle.strategy,
                        "expiry": closest_contract.lastTradeDateOrContractMonth,
                        "strike": closest_contract.strike,
                        "multiplier": float(closest_contract.multiplier),
                        "option_type": OptionType.P,
                        "avg_price": price_now,
                        "quantity": -1,
                    }
                )

        return target_positions

    @staticmethod
    async def get_buy_order(
        broker: DataBroker,
        stock: str,
        expiry: str,
        strike: float,
        multiplier: str,
        quantity: int,
    ) -> List[FullOrder]:
        call_option = Option(stock, expiry, strike, "C", "SMART", multiplier, "USD")
        put_option = Option(stock, expiry, strike, "P", "SMART", multiplier, "USD")
        call_price = await broker.get_current_price(call_option)
        put_price = await broker.get_current_price(put_option)

        full_contract = Contract(
            symbol=stock,
            secType="BAG",
            currency="USD",
            exchange="SMART",
            comboLegs=[
                ComboLeg(call_option.conId, action="BUY", ratio=1, exchange="SMART"),
                ComboLeg(put_option.conId, action="BUY", ratio=1, exchange="SMART"),
            ],
        )

        order = LimitOrder(
            "SELL",
            abs(quantity),
            round(1.005 * (call_price + put_price), 2),
            tif="GTC",
            # allOrNone=True,
        )
        order.smartComboRoutingParams.append(TagValue("NonGuaranteed", "1"))
        return [{"contract": full_contract, "order": order}]

    @staticmethod
    async def get_sell_order(
        broker: DataBroker,
        stock: str,
        expiry: str,
        strike: float,
        multiplier: str,
        quantity: int,
    ) -> List[FullOrder]:
        call_option = Option(stock, expiry, strike, "C", "SMART", multiplier, "USD")
        put_option = Option(stock, expiry, strike, "P", "SMART", multiplier, "USD")
        call_price = await broker.get_current_price(call_option)
        put_price = await broker.get_current_price(put_option)

        full_contract = Contract(
            symbol=stock,
            secType="BAG",
            currency="USD",
            exchange="SMART",
            comboLegs=[
                ComboLeg(call_option.conId, action="BUY", ratio=1, exchange="SMART"),
                ComboLeg(put_option.conId, action="BUY", ratio=1, exchange="SMART"),
            ],
        )

        order = LimitOrder(
            "BUY",
            abs(quantity),
            round(0.995 * (call_price + put_price), 2),
            tif="GTC",
            # allOrNone=True,
        )
        order.smartComboRoutingParams.append(TagValue("NonGuaranteed", "1"))
        return [{"contract": full_contract, "order": order}]

    @staticmethod
    async def get_orders_for_quantity_difference(
        broker: DataBroker,
        quantity_differences: Dict[Tuple[str, str, float, float, OptionType], float],
    ) -> List[FullOrder]:
        if len(quantity_differences) == 0:
            return []

        unique_straddles = {}
        unique_straddles_counter = {}
        for quantity_diff, position in quantity_differences.items():
            if quantity_diff[0] not in unique_straddles:
                unique_straddles[quantity_diff[0]] = (
                    quantity_diff[1],
                    quantity_diff[2],
                    quantity_diff[3],
                    position,
                )
                unique_straddles_counter[quantity_diff[0]] = 1
            else:
                if position != unique_straddles[quantity_diff[0]][3]:
                    CustomLogger(
                        "EarningsStraddle.get_orders_for_quantity_difference()"
                    ).error(
                        f"Target Position for {quantity_diff[0]} does not match for multiple positions."
                    )
                unique_straddles_counter[quantity_diff[0]] += 1

        assert all([i == 2 for i in unique_straddles_counter.values()])

        orders = []
        for straddle, details in unique_straddles.items():
            if details[3] < 0:
                orders.extend(
                    await EarningsStraddle.get_buy_order(
                        broker,
                        straddle,
                        details[0],
                        details[1],
                        str(details[2]),
                        int(details[3]),
                    )
                )
            else:
                orders.extend(
                    await EarningsStraddle.get_sell_order(
                        broker,
                        straddle,
                        details[0],
                        details[1],
                        str(details[2]),
                        int(details[3]),
                    )
                )
        return orders

    @staticmethod
    async def get_dates_until_fri() -> List[datetime]:
        """
        Returns list of Datetime objects with hour, minute, second, microsecond = 0
        for all days until the next friday from today
        """
        today = pytz.timezone("US/Eastern").localize(
            datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        )
        dates: List[datetime] = []
        while today.weekday() < 4:
            dates.append(today)
            today += timedelta(days=1)
        return dates

    @staticmethod
    @async_current_option_positions_wrapper
    async def get_stocks(
        current_option_positions: AsyncCurrentOptionPositionsCRUD, broker: DataBroker
    ) -> List[Contract]:
        """
        Gets all stocks for which earnings is any date from today until the next fri
        data from api.nasdaq.com,
        then updates EarningsStraddle.stocks with the open opportunities and possible_options with the IBKR contracts possible
        """
        # url = "https://api.nasdaq.com/api/calendar/earnings?date=2025-06-09"
        if len(EarningsStraddle.future_earnings_stocks) > 0:
            return EarningsStraddle.future_earnings_stocks

        ssl_context = ssl.create_default_context(cafile=certifi.where())
        async with aiohttp.ClientSession() as session:
            # Update for future earnings
            dates = await EarningsStraddle.get_dates_until_fri()
            opportunities: List[Contract] = []
            for ind, date in enumerate(dates):
                url = f"https://api.nasdaq.com/api/calendar/earnings?date={date.strftime('%Y-%m-%d')}"
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36",
                    "Accept": "application/json, text/plain, */*",
                    "Accept-Encoding": "gzip, deflate, br",  # Add this
                    "Accept-Language": "en-US,en;q=0.9",  # Add this
                    "Connection": "keep-alive",  # Add this
                }

                async with session.get(url, headers=headers, ssl=ssl_context) as resp:
                    if resp.status == 200:
                        res = await resp.json()
                        res = res["data"]["rows"]
                        if ind == 0:
                            EarningsStraddle.today_earnings_stocks = set(
                                [i["symbol"] for i in res]
                            )
                        possible_contracts = [
                            {
                                "symbol": i["symbol"],
                                "exchange": "SMART",
                                "currency": "USD",
                                "secType": "STK",
                            }
                            for i in res
                            if (
                                i["time"] == "time-not-supplied"
                                or (i["time"] == "time-pre-market" and ind > 0)
                                or (
                                    i["time"] == "time-after-hours"
                                    and ind < len(dates) - 1
                                )
                            )
                        ]
                        qualified_contracts = [
                            i
                            for i in await broker.ib.qualifyContractsAsync(
                                *[
                                    Contract(**contract)
                                    for contract in possible_contracts
                                ]
                            )
                            if i.conId
                        ]
                        opportunities.extend(qualified_contracts)
                    else:
                        CustomLogger(f"{EarningsStraddle.strategy} get_stocks()").error(
                            "error in get request to nasdaq"
                        )

            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            def get_url(symbol: str) -> str:
                return f"https://paper-api.alpaca.markets/v2/options/contracts?underlying_symbols={symbol}&status=active&expiration_date_gte={dates[-1].strftime('%Y-%m-%d')}&limit=1000"

            headers = {
                "APCA-API-KEY-ID": "<FILL THIS IN>",
                "APCA-API-SECRET-KEY": "<FILL THIS IN>",
                "accept": "application/json",
            }
            excluded_opportunities: Set[Contract] = set()
            for contract in opportunities:
                async with session.get(
                    get_url(contract.symbol), headers=headers, ssl=ssl_context
                ) as resp:
                    if resp.status == 200:
                        res = await resp.json()
                        if len(res["option_contracts"]) == 0:
                            CustomLogger("EarningsStraddle.get_stocks()").warning(
                                f"Error when getting option contracts for stock: No data: {contract}"
                            )
                            excluded_opportunities.add(contract)
                            continue
                        expiration_date = res["option_contracts"][0][
                            "expiration_date"
                        ].replace("-", "")

                        opt_contract = Contract(
                            symbol=contract.symbol,
                            secType="OPT",
                            lastTradeDateOrContractMonth=expiration_date,
                            multiplier="100",
                            exchange="SMART",
                            currency="USD",
                        )
                        contracts = await broker.ib.reqContractDetailsAsync(
                            opt_contract
                        )
                        if len(contracts) == 0:
                            excluded_opportunities.add(contract)
                            continue
                        EarningsStraddle.possible_options[contract] = [
                            cast(Contract, i.contract) for i in contracts
                        ]
                    else:
                        CustomLogger("EarningsStraddle.get_stocks").error(
                            "get_stocks failed at retrieving contract details"
                        )

        EarningsStraddle.future_earnings_stocks.extend(
            [i for i in opportunities if i not in excluded_opportunities]
        )
        assert all(
            [
                len(EarningsStraddle.possible_options[i]) > 0
                for i in EarningsStraddle.future_earnings_stocks
            ]
        )
        return EarningsStraddle.future_earnings_stocks

    @staticmethod
    async def update_historical_data_to_present(
        broker: DataBroker,
    ) -> None:
        await EarningsStraddle.update_bars_till_present(broker)
