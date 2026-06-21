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
from typing import Dict, List, Set, Tuple, cast
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
from app.models_types import (
    CurrentOptionPositionsDict,
    TargetOptionPositionsDict,
    OptionType,
    Status,
)
from app.utils.custom_logging import CustomLogger
from app.utils.db import (
    with_db_session_for_model,
    async_with_db_session_for_model,
)
from ib_async.contract import Option, ComboLeg, Contract, TagValue
from ib_async.order import LimitOrder
import ssl
import certifi


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


class PreEarningsStraddle(OptionStrategy):
    """
    Short if 3 day before earnings
    Take profit when straddle has lost 20% of value
    OR if day before earnings
    """

    strategy: str = "pre_earnings_straddle"
    to_clear_before_sending: bool = True
    earnings_stocks_3_days_away: List[Contract]
    earnings_stocks_1_day_away: List[Contract]
    possible_options: Dict[Contract, List[Contract]] = {}
    calendar = mcal.get_calendar("NYSE")
    eastern = pytz.timezone("US/Eastern")

    @async_strategy_wrapper
    @staticmethod
    async def create_strategy(strategy: AsyncStrategyCRUD) -> None:
        strategy_exists = await strategy.read(
            {"strategy": PreEarningsStraddle.strategy}
        )
        if len(strategy_exists) > 0:
            return

        await strategy.create(
            {
                "strategy": PreEarningsStraddle.strategy,
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

        # ----- NOTE -----
        # Only need to keep track of new opportunities,
        # already opened positions will be trivially separately checked in get_weights()
        """
        if len(PreEarningsStraddle.today_earnings_stocks) == 0:
            await PreEarningsStraddle.get_stocks(broker)

        # Update Historical data bars first, then other bars
        expected_bars = []

        # Get the most recent bar
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=10)
        most_recent_valid_days = PreEarningsStraddle.calendar.valid_days(
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

        # Update data for current positions
        is_updated = False
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

        # Update bars for opportunities
        to_del: List[Contract] = []
        to_del_precise: List[Tuple[Contract, Contract]] = []
        # Update for future opportunities
        for contract in EarningsStraddle.today_earnings_stocks:
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
            last_price = historical_stock[-1]["close"]

            # min_price, max_price = (
            #     min([i["low"] for i in historical_stock]),
            #     max([i["high"] for i in historical_stock]),
            # )
            # print(contract.symbol, min_price, max_price)
            # MAY want to get strike just above and below as well

            closest_contract = min(
                EarningsStraddle.possible_options[contract],
                key=lambda x: abs(x.strike - last_price),
            )
            # for inner_contract in EarningsStraddle.possible_options[contract]:
            #     if not (min_price <= float(contract.strike) <= max_price):
            #         continue
            bars = await historical_options_data.read_stock(
                closest_contract.symbol,
                closest_contract.lastTradeDateOrContractMonth,
                closest_contract.strike,
                float(closest_contract.multiplier),
                cast(OptionType, closest_contract.right[0]),
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
                    to_del_precise.append((contract, closest_contract))
                await historical_options_data.create_or_update_all(
                    [
                        {
                            "stock": closest_contract.symbol,
                            "expiry": closest_contract.lastTradeDateOrContractMonth,
                            "strike": closest_contract.strike,
                            "multiplier": float(closest_contract.multiplier),
                            "option_type": cast(OptionType, closest_contract.right[0]),
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
            EarningsStraddle.today_earnings_stocks.remove(i)
        for j in to_del_precise:
            EarningsStraddle.possible_options[j[0]].remove(j[1])

        return is_updated

    @async_historical_options_data_wrapper
    @async_historical_data_wrapper
    @async_current_option_positions_wrapper
    @async_option_transactions_wrapper
    @staticmethod
    async def get_weights(
        option_transactions: AsyncOptionTransactionsCRUD,
        current_option_positions: AsyncCurrentOptionPositionsCRUD,
        historical_data: AsyncHistoricalDataCRUD,
        historical_options_data: AsyncHistoricalOptionsDataCRUD,
        broker: DataBroker,
    ) -> List[TargetOptionPositionsDict]:
        # realised boolean doesn't matter because i'm alse updating via real time bars which isn't taken into account for in the function
        await EarningsStraddle.update_bars_till_present(broker)

        # Extract positions to readable format
        target_positions: List[TargetOptionPositionsDict] = []
        current_positions = (
            await current_option_positions.get_current_positions_for_strategy(
                EarningsStraddle.strategy
            )
        )
        straddle_dict: Dict[str, List[CurrentOptionPositionsDict]] = {}
        for current_position in current_positions:
            if current_position["stock"] not in straddle_dict:
                straddle_dict[current_position["stock"]] = []
            straddle_dict[current_position["stock"]].append(current_position)
        to_ignore = set()
        for stock in straddle_dict:
            if len(straddle_dict[stock]) == 1:
                CustomLogger("EarningsStraddle").critical(
                    f"Only one leg of strategy filled for the time being for stock: {stock}"
                )
                to_ignore.add(stock)
                continue
            if len(straddle_dict[stock]) > 2:
                CustomLogger("EarningsStraddle").error(
                    f"More than one straddle for stock: {stock}"
                )
        for stock in to_ignore:
            del straddle_dict[stock]

        # Determine if take profit hit
        to_offload = []
        for stock, straddle in straddle_dict.items():
            contract1, contract2 = straddle[0], straddle[1]
            last_bar = await historical_options_data.read_stock(
                contract1["stock"],
                contract1["expiry"],
                contract1["strike"],
                contract1["multiplier"],
                contract1["option_type"],
                limit=1,
            )
            last_bar2 = await historical_options_data.read_stock(
                contract2["stock"],
                contract2["expiry"],
                contract2["strike"],
                contract2["multiplier"],
                contract2["option_type"],
                limit=1,
            )
            last_close = last_bar[0]["close"] + last_bar2[0]["close"]
            if last_close < (contract1["avg_price"] + contract2["avg_price"]) * 0.8:
                to_offload.extend(straddle)
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

        # look at the stocks for today
        for contract in EarningsStraddle.today_earnings_stocks:
            in_curr_positions = (
                len(
                    await current_option_positions.get_current_positions_for_stock(
                        contract.symbol
                    )
                )
                > 0
            )
            in_tdy_transactions = (
                len(
                    await option_transactions.read_stock_day(
                        contract.symbol,
                        datetime.now(timezone.utc).astimezone(EarningsStraddle.eastern),
                    )
                )
                > 0
            )
            if not in_curr_positions and not in_tdy_transactions:
                last_stock_bar = await historical_data.read_stock(contract.symbol, 1)
                if len(last_stock_bar) == 0:
                    CustomLogger("EarningsStraddle").error(
                        f"Error with contract: {contract}, no last bar of data"
                    )
                    continue
                possible_contracts = EarningsStraddle.possible_options[contract]
                closest_contract = min(
                    possible_contracts,
                    key=lambda x: abs(x.strike - last_stock_bar[0]["close"]),
                )
                call_contract = [
                    i
                    for i in possible_contracts
                    if i.symbol == closest_contract.symbol
                    and i.strike == closest_contract.strike
                    and i.lastTradeDateOrContractMonth
                    == closest_contract.lastTradeDateOrContractMonth
                    and i.multiplier == closest_contract.multiplier
                    and i.right == "C"
                ][0]
                put_contract = [
                    i
                    for i in possible_contracts
                    if i.symbol == closest_contract.symbol
                    and i.strike == closest_contract.strike
                    and i.lastTradeDateOrContractMonth
                    == closest_contract.lastTradeDateOrContractMonth
                    and i.multiplier == closest_contract.multiplier
                    and i.right == "P"
                ][0]
                call_contract_bars = await broker.ib.reqHistoricalDataAsync(
                    call_contract,
                    endDateTime="",
                    durationStr="1 D",
                    barSizeSetting="5 mins",
                    whatToShow="TRADES",
                    useRTH=True,
                )
                put_contract_bars = await broker.ib.reqHistoricalDataAsync(
                    put_contract,
                    endDateTime="",
                    durationStr="1 D",
                    barSizeSetting="5 Min",
                    whatToShow="TRADES",
                    useRTH=True,
                )
                last_vol = call_contract_bars[-1].volume + put_contract_bars[-1].volume

                if last_vol > 100:
                    # Avg price here doesn't matter since actual orders are constructed below
                    target_positions.append(
                        {
                            "stock": contract.symbol,
                            "strategy": EarningsStraddle.strategy,
                            "expiry": closest_contract.lastTradeDateOrContractMonth,
                            "strike": closest_contract.strike,
                            "multiplier": float(closest_contract.multiplier),
                            "option_type": OptionType.C,
                            "avg_price": 0,
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
                            "avg_price": 0,
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
    async def get_stocks(broker: DataBroker) -> List[Contract]:
        """
        Gets all stocks for which earnings is any date from today until the next fri
        data from api.nasdaq.com,
        then updates EarningsStraddle.stocks with the open opportunities and possible_options with the IBKR contracts possible
        """
        # url = "https://api.nasdaq.com/api/calendar/earnings?date=2025-06-09"
        if len(PreEarningsStraddle.today_earnings_stocks) > 0:
            return EarningsStraddle.today_earnings_stocks

        # Update stocks to monitor for those with earnings today
        date = datetime.now(timezone.utc).astimezone(
            pytz.timezone("US/Eastern")
        ).replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=3)
        opportunities: List[Contract] = []
        ssl_context = ssl.create_default_context(cafile=certifi.where())

        async with aiohttp.ClientSession() as session:
            # Update for future earnings
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
                    possible_contracts = [
                        {
                            "symbol": i["symbol"],
                            "exchange": "SMART",
                            "currency": "USD",
                            "secType": "STK",
                        }
                        for i in res
                        if (i["time"] != "time-pre-market")
                    ]
                    qualified_contracts = [
                        i
                        for i in await broker.ib.qualifyContractsAsync(
                            *[Contract(**contract) for contract in possible_contracts]
                        )
                        if i.conId
                    ]
                    opportunities.extend(qualified_contracts)
                else:
                    CustomLogger(f"{EarningsStraddle.strategy} get_stocks()").error(
                        "error in get request to nasdaq"
                    )

            # Update possible options related to stock
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            def get_url(symbol: str) -> str:
                return f"https://paper-api.alpaca.markets/v2/options/contracts?underlying_symbols={symbol}&status=active&expiration_date_gte={date.strftime('%Y-%m-%d')}&limit=10"

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

        EarningsStraddle.today_earnings_stocks.extend(
            [i for i in opportunities if i not in excluded_opportunities]
        )
        assert all(
            [
                len(EarningsStraddle.possible_options[i]) > 0
                for i in EarningsStraddle.today_earnings_stocks
            ]
        )
        return EarningsStraddle.today_earnings_stocks

    @staticmethod
    async def update_historical_data_to_present(
        broker: DataBroker,
    ) -> None:
        await EarningsStraddle.update_bars_till_present(broker)
