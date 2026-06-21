from sortedcontainers import SortedList
from collections import deque
import numpy as np
from datetime import datetime, timedelta, timezone
import pytz
import pandas_market_calendars as mcal
from scipy.stats import norm
from sqlalchemy.ext.asyncio.engine import AsyncConnection
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
from typing import Dict, List, Tuple, cast, Any
from app.services.broker.DataBroker import DataBroker, FullOrder

from ib_async.order import Trade
from ib_async.objects import Fill
from ib_async.contract import Stock
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
    async_with_engine,
)
from sqlalchemy import text
from sqlalchemy.engine import Connection
from ib_async.contract import Option, ComboLeg, Contract, TagValue
from ib_async.order import LimitOrder


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


def implied_vol_skew(S: float, K: float, atm_vol: float) -> float:
    log_moneyness = np.log(K / S)
    assert type(log_moneyness) is np.float64
    skew_slope = 0.2
    vol_adj = skew_slope * log_moneyness
    return max(atm_vol + vol_adj, 0.01)


def black_scholes_call(
    S: float, K: float, T: float, r: float, atm_sigma: float
) -> float:
    if T <= 0:
        return max(S - K, 0)
    sigma = implied_vol_skew(S, K, atm_sigma)
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T) + 1e-8)
    d2 = d1 - sigma * np.sqrt(T)
    return float(S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2))


def black_scholes_put(
    S: float, K: float, T: float, r: float, atm_sigma: float
) -> float:
    if T <= 0:
        return max(K - S, 0)
    sigma = implied_vol_skew(S, K, atm_sigma)
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T) + 1e-8)
    d2 = d1 - sigma * np.sqrt(T)
    return float(K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1))


class ShortStraddle(OptionStrategy):
    strategy: str = "short_straddle"
    to_clear_before_sending: bool = True

    @phantom_portfolio_value_wrapper
    @strategy_wrapper
    @staticmethod
    async def create_strategy(
        strategy: StrategyCRUD, phantom_portfolio_value: PhantomPortfolioValueCRUD
    ) -> None:
        strategy_exists = strategy.read({"strategy": ShortStraddle.strategy})
        if len(strategy_exists) > 0:
            return

        strategy.create(
            {
                "strategy": ShortStraddle.strategy,
                "capital": 100000,
                "initial_capital": 100000,
                "status": Status.active,
            }
        )
        phantom_portfolio_value.create(
            {
                "time": datetime.now(),
                "cash_portfolio_value": 100000,
                "option_portfolio_value": 0,
                "bought_price": 0,
                "strike": 0,
                "peak": 0,
                "paused": False,
                "resume_trades": 0,
            }
        )

    @staticmethod
    def get_percentile(sorted_list: SortedList[float], percentile: float) -> float:
        n = len(sorted_list)
        rank = percentile * (n - 1)
        lower_index = int(rank)
        upper_index = lower_index + 1
        fraction = rank - lower_index

        if upper_index >= n:
            return sorted_list[-1]
        return (
            sorted_list[lower_index]
            + (sorted_list[upper_index] - sorted_list[lower_index]) * fraction
        )

    @staticmethod
    def round_down_to_nearest_5_minutes(dt: datetime) -> datetime:
        """Rounds a datetime.datetime object down to the nearest 5-minute interval."""
        minutes = dt.minute // 5 * 5
        return dt.replace(minute=minutes, second=0, microsecond=0)

    @staticmethod
    def estimate_updated_value(
        previous_option_portfolio_value: float,
        previous_spot_open: float,
        previous_implied_volatility: float,
        current_implied_volatility: float,
        current_spot_open: float,
        K: float,
        T: float,
        position: float,
    ) -> float:
        call_price = black_scholes_call(
            previous_spot_open, K, T, 0.04, previous_implied_volatility
        )
        put_price = black_scholes_put(
            previous_spot_open, K, T, 0.04, previous_implied_volatility
        )
        previous_estimated_value = (call_price + put_price) * position
        offset = previous_option_portfolio_value - previous_estimated_value

        # Estimate new straddle value using current spot and IV
        call_price = black_scholes_call(
            current_spot_open, K, T, 0.04, current_implied_volatility
        )
        put_price = black_scholes_put(
            current_spot_open, K, T, 0.04, current_implied_volatility
        )

        return (call_price + put_price) * position + offset

    @staticmethod
    @async_historical_options_data_wrapper
    @async_historical_volatility_data_wrapper
    @async_phantom_portfolio_value_wrapper
    async def update_portfolio_value_to_now(
        phantom_portfolio_value: AsyncPhantomPortfolioValueCRUD,
        historical_volatility_data: AsyncHistoricalVolatilityDataCRUD,
        historical_options_data: AsyncHistoricalOptionsDataCRUD,
    ) -> None:
        last_holding_time_str = "15:45"
        last_consideration_for_buying_time_str = "15:35"
        longest_holding_period = 25 * 60  # in seconds
        drawdown_threshold = 0.0
        position = 1

        eastern = pytz.timezone("US/Eastern")

        # Assume today's date in US/Eastern timezone for initialization
        today_eastern = eastern.localize(datetime.now()).date()

        # Combine today's date with the given times and set the timezone
        last_holding_time = datetime.combine(
            today_eastern, datetime.strptime(last_holding_time_str, "%H:%M").time()
        ).replace(tzinfo=eastern)
        last_consideration_for_buying_time = datetime.combine(
            today_eastern,
            datetime.strptime(last_consideration_for_buying_time_str, "%H:%M").time(),
        ).replace(tzinfo=eastern)

        last_portfolio_entries = await phantom_portfolio_value.get_last_entry()
        last_portfolio_entry = last_portfolio_entries[0]
        bought_time = datetime(1970, 1, 1, tzinfo=timezone.utc).astimezone(eastern)
        if last_portfolio_entry["option_portfolio_value"] != 0:
            portfolio_entries_iterator = last_portfolio_entries[1:]
            prev_time = portfolio_entries_iterator[0]
            for i in portfolio_entries_iterator:
                if i["option_portfolio_value"] == 0:
                    bought_time = prev_time["time"].astimezone(eastern)
                prev_time = i
        (
            previous_time,
            previous_cash_portfolio_value,
            previous_option_portfolio_value,
            previous_bought,
            K,
            peak,
            paused,
            resume_trades,
        ) = (
            last_portfolio_entry["time"],
            last_portfolio_entry["cash_portfolio_value"],
            last_portfolio_entry["option_portfolio_value"],
            last_portfolio_entry["bought_price"],
            last_portfolio_entry["strike"],
            last_portfolio_entry["peak"],
            last_portfolio_entry["paused"],
            last_portfolio_entry["resume_trades"],
        )
        # ----------------------- TO UPDATE SO THAT PREVIOUS_TIME IS TZ AWARE ------------------
        if (eastern.localize(datetime.now()) - previous_time).total_seconds() < (
            5 * 60
        ):
            return

        volatility_data = await historical_volatility_data.read_for_stock_past(
            "SPY", previous_time
        )
        options_data = await historical_options_data.read_for_stock_past(
            "SPY", previous_time
        )
        implied_volatility_window_deque = deque(volatility_data[:78], maxlen=78)
        implied_volatility_window = SortedList(
            [i["open"] for i in volatility_data[:78]]
        )
        volatility_data = volatility_data[78:]
        options_index = 0
        print(len(volatility_data))
        for implied_volatility in volatility_data:
            print(implied_volatility["time"])
            # while vol_index < len(volatility_data) or options_index < len(options_data):
            value_to_remove = implied_volatility_window_deque.popleft()
            implied_volatility_window.remove(value_to_remove["open"])
            implied_volatility_window.add(implied_volatility["open"])
            implied_volatility_window_deque.append(implied_volatility)

            current_time = implied_volatility["time"]
            end_of_session_today = datetime(
                implied_volatility["time"].year,
                implied_volatility["time"].month,
                implied_volatility["time"].day,
                16,
                0,
                0,
            ).astimezone(pytz.timezone("US/Eastern"))

            rolling75 = ShortStraddle.get_percentile(implied_volatility_window, 0.75)
            rolling90 = ShortStraddle.get_percentile(implied_volatility_window, 0.90)

            while (
                options_index < len(options_data)
                and options_data[options_index]["time"] < implied_volatility["time"]
            ):
                options_index += 1

            if previous_option_portfolio_value < 0:
                previous_option_portfolio_value = ShortStraddle.estimate_updated_value(
                    previous_option_portfolio_value,
                    implied_volatility_window_deque[-2]["spot_open"],
                    implied_volatility_window_deque[-2]["open"],
                    implied_volatility_window_deque[-1]["open"],
                    implied_volatility_window_deque[-1]["spot_open"],
                    K,
                    (
                        end_of_session_today
                        - implied_volatility["time"].astimezone(
                            pytz.timezone("Asia/Singapore")
                        )
                    ).total_seconds()
                    / (60 * 5 * 78 * 365),
                    position,
                )
                if (
                    (implied_volatility["open"] < rolling75)
                    or (current_time >= last_holding_time)
                    or (
                        (current_time - bought_time).total_seconds()
                        > longest_holding_period
                    )
                ):
                    # sell
                    previous_cash_portfolio_value += previous_option_portfolio_value
                    is_gain = previous_bought + previous_option_portfolio_value
                    previous_option_portfolio_value = 0

                    if paused:
                        if is_gain:
                            resume_trades += 1
                            if resume_trades >= 3:
                                resume_trades = 0
                                paused = False
                        else:
                            resume_trades = 0
                    else:
                        peak = max(
                            peak,
                            previous_option_portfolio_value
                            + previous_cash_portfolio_value,
                        )
                        drawdown = (
                            1
                            - (
                                previous_option_portfolio_value
                                + previous_cash_portfolio_value
                            )
                            / peak
                        )
                        if drawdown > drawdown_threshold:
                            paused = True
                await phantom_portfolio_value.create(
                    {
                        "time": implied_volatility["time"],
                        "cash_portfolio_value": previous_cash_portfolio_value,
                        "option_portfolio_value": previous_option_portfolio_value,
                        "bought_price": previous_bought,
                        "strike": K,
                        "peak": max(
                            peak,
                            previous_option_portfolio_value
                            + previous_cash_portfolio_value,
                        ),
                        "paused": paused,
                        "resume_trades": resume_trades,
                    }
                )
            else:
                if current_time >= last_consideration_for_buying_time:
                    await phantom_portfolio_value.create(
                        {
                            "time": implied_volatility["time"],
                            "cash_portfolio_value": previous_cash_portfolio_value,
                            "option_portfolio_value": previous_option_portfolio_value,
                            "bought_price": previous_bought,
                            "strike": K,
                            "peak": peak,
                            "paused": paused,
                            "resume_trades": resume_trades,
                        }
                    )
                    continue
                if implied_volatility["open"] > rolling90:
                    if (
                        options_index < len(options_data)
                        and ShortStraddle.round_down_to_nearest_5_minutes(
                            options_data[options_index]["time"]
                        )
                        == implied_volatility["time"]
                    ):
                        K = options_data[options_index]["strike"]
                        first_option_price = options_data[options_index]["close"]
                        second_option_price = options_data[options_index]["close"]
                        if (
                            ShortStraddle.round_down_to_nearest_5_minutes(
                                options_data[options_index + 1]["time"]
                            )
                            == implied_volatility["time"]
                        ):
                            second_option_price = options_data[options_index + 1][
                                "close"
                            ]
                            options_index += 1
                        previous_bought = first_option_price + second_option_price
                        options_index += 1
                    else:
                        K = implied_volatility["spot_open"]
                        price = black_scholes_call(
                            implied_volatility["spot_open"],
                            implied_volatility["spot_open"],
                            (
                                end_of_session_today
                                - implied_volatility["time"].astimezone(
                                    pytz.timezone("Asia/Singapore")
                                )
                            ).total_seconds()
                            / (60 * 5 * 78 * 365),
                            0.04,
                            implied_volatility["open"],
                        )
                        previous_bought = price * 2
                    previous_option_portfolio_value -= (previous_bought) * position
                    previous_cash_portfolio_value -= previous_option_portfolio_value
                await phantom_portfolio_value.create(
                    {
                        "time": implied_volatility["time"],
                        "cash_portfolio_value": previous_cash_portfolio_value,
                        "option_portfolio_value": previous_option_portfolio_value,
                        "bought_price": previous_bought,
                        "strike": K,
                        "peak": peak,
                        "paused": paused,
                        "resume_trades": resume_trades,
                    }
                )

    @async_historical_options_data_wrapper
    @staticmethod
    async def update_historical_options(
        historical_options_data: AsyncHistoricalOptionsDataCRUD,
        broker: DataBroker,
        expiry: str,
        strike: float,
        time: datetime,
    ) -> Tuple[float, float]:
        call_option, put_option = (
            Option("SPY", expiry, strike, "C", "SMART", "100", "USD"),
            Option("SPY", expiry, strike, "P", "SMART", "100", "USD"),
        )
        call_price, put_price = (
            await broker.get_current_price(call_option),
            await broker.get_current_price(put_option),
        )
        await historical_options_data.create_or_update(
            {
                "stock": "SPY",
                "expiry": expiry,
                "strike": strike,
                "multiplier": 100,
                "option_type": OptionType.C,
                "time": time,
                "open": call_price,
                "high": call_price,
                "low": call_price,
                "close": call_price,
                "volume": 0,
            }
        )
        await historical_options_data.create_or_update(
            {
                "stock": "SPY",
                "expiry": expiry,
                "strike": strike,
                "multiplier": 100,
                "option_type": OptionType.P,
                "time": time,
                "open": put_price,
                "high": put_price,
                "low": put_price,
                "close": put_price,
                "volume": 0,
            }
        )
        return call_price, put_price

    @staticmethod
    @async_historical_volatility_data_wrapper
    @async_with_engine
    @async_current_option_positions_wrapper
    @async_phantom_portfolio_value_wrapper
    async def get_weights(
        phantom_portfolio_value: AsyncPhantomPortfolioValueCRUD,
        current_option_positions: AsyncCurrentOptionPositionsCRUD,
        conn: AsyncConnection,
        historical_volatility_data: AsyncHistoricalVolatilityDataCRUD,
        broker: DataBroker,
    ) -> List[TargetOptionPositionsDict]:
        last_holding_time_str = "15:45"
        last_consideration_for_buying_time_str = "15:35"
        longest_holding_period = 25 * 60  # in minutes
        drawdown_threshold = 0.0
        position = 1

        eastern = pytz.timezone("US/Eastern")

        # Assume today's date in US/Eastern timezone for initialization
        today_eastern = eastern.localize(datetime.now()).date()

        # Combine today's date with the given times and set the timezone
        last_holding_time = datetime.combine(
            today_eastern, datetime.strptime(last_holding_time_str, "%H:%M").time()
        ).replace(tzinfo=eastern)
        last_consideration_for_buying_time = datetime.combine(
            today_eastern,
            datetime.strptime(last_consideration_for_buying_time_str, "%H:%M").time(),
        ).replace(tzinfo=eastern)

        CustomLogger(f"{ShortStraddle.strategy} get_weights()").info(
            "getting weights now"
        )
        await ShortStraddle.update_portfolio_value_to_now()

        last_portfolio_entries = await phantom_portfolio_value.get_last_entry()
        last_portfolio_entry = last_portfolio_entries[0]
        (
            previous_time,
            previous_cash_portfolio_value,
            previous_option_portfolio_value,
            previous_bought,
            K,
            peak,
            paused,
            resume_trades,
        ) = (
            last_portfolio_entry["time"],
            last_portfolio_entry["cash_portfolio_value"],
            last_portfolio_entry["option_portfolio_value"],
            last_portfolio_entry["bought_price"],
            last_portfolio_entry["strike"],
            last_portfolio_entry["peak"],
            last_portfolio_entry["paused"],
            last_portfolio_entry["resume_trades"],
        )
        bought_time = datetime(1970, 1, 1, tzinfo=timezone.utc).astimezone(eastern)
        if last_portfolio_entry["option_portfolio_value"] != 0:
            portfolio_entries_iterator = last_portfolio_entries[1:]
            prev_time = portfolio_entries_iterator[0]
            for i in portfolio_entries_iterator:
                if i["option_portfolio_value"] == 0:
                    bought_time = prev_time["time"]
                prev_time = i

        volatility_data = await historical_volatility_data.read_for_stock_past(
            "SPY", previous_time
        )

        if len(volatility_data) == 0:
            return []
        implied_volatility_window_deque = deque(volatility_data[:78], maxlen=78)
        implied_volatility_window = SortedList(
            [i["open"] for i in volatility_data[:78]]
        )

        rolling75 = ShortStraddle.get_percentile(implied_volatility_window, 0.75)
        rolling90 = ShortStraddle.get_percentile(implied_volatility_window, 0.90)

        current_spy_positions = (
            await current_option_positions.get_current_positions_for_strategy(
                ShortStraddle.strategy
            )
        )

        current_time = datetime.now().astimezone(pytz.timezone("US/Eastern"))
        end_of_session_today = datetime(
            current_time.year, current_time.month, current_time.day, 16, 0, 0
        ).astimezone(pytz.timezone("US/Eastern"))

        if paused:
            if previous_option_portfolio_value < 0:
                previous_option_portfolio_value = ShortStraddle.estimate_updated_value(
                    previous_option_portfolio_value,
                    implied_volatility_window_deque[-2]["spot_open"],
                    implied_volatility_window_deque[-2]["open"],
                    implied_volatility_window_deque[-1]["open"],
                    implied_volatility_window_deque[-1]["spot_open"],
                    K,
                    (
                        end_of_session_today
                        - implied_volatility_window_deque[-1]["time"].astimezone(
                            pytz.timezone("Asia/Singapore")
                        )
                    ).total_seconds()
                    / (60 * 5 * 78 * 365),
                    position,
                )
                if (
                    (implied_volatility_window_deque[-1]["open"] < rolling75)
                    or (current_time >= last_holding_time)
                    or (
                        (current_time - bought_time).total_seconds()
                        > longest_holding_period
                    )
                ):
                    await ShortStraddle.update_historical_options(
                        broker,
                        end_of_session_today.strftime("%Y%m%d"),
                        round(implied_volatility_window_deque[-1]["spot_open"]),
                        implied_volatility_window_deque[-1]["time"],
                    )
                    # sell
                    previous_cash_portfolio_value += previous_option_portfolio_value
                    is_gain = previous_bought + previous_option_portfolio_value
                    previous_option_portfolio_value = 0

                    if paused:
                        if is_gain:
                            resume_trades += 1
                            if resume_trades >= 3:
                                resume_trades = 0
                                paused = False
                        else:
                            resume_trades = 0
                    else:
                        peak = max(
                            peak,
                            previous_option_portfolio_value
                            + previous_cash_portfolio_value,
                        )
                        drawdown = (
                            1
                            - (
                                previous_option_portfolio_value
                                + previous_cash_portfolio_value
                            )
                            / peak
                        )
                        if drawdown > drawdown_threshold:
                            paused = True
                await phantom_portfolio_value.create(
                    {
                        "time": implied_volatility_window_deque[-1]["time"],
                        "cash_portfolio_value": previous_cash_portfolio_value,
                        "option_portfolio_value": previous_option_portfolio_value,
                        "bought_price": previous_bought,
                        "strike": round(
                            implied_volatility_window_deque[-1]["spot_open"]
                        ),
                        "peak": max(
                            peak,
                            previous_option_portfolio_value
                            + previous_cash_portfolio_value,
                        ),
                        "paused": paused,
                        "resume_trades": resume_trades,
                    }
                )

            if implied_volatility_window_deque[-1]["open"] > rolling90:
                call_price, put_price = await ShortStraddle.update_historical_options(
                    broker,
                    end_of_session_today.strftime("%Y%m%d"),
                    round(implied_volatility_window_deque[-1]["spot_open"]),
                    implied_volatility_window_deque[-1]["time"],
                )
                # call_price = black_scholes_call(
                #     implied_volatility_window_deque[-1]['spot_open'],
                #     round(implied_volatility_window_deque[-1]['spot_open']),
                #     (end_of_session_today - implied_volatility_window_deque[-1]['time'].astimezone(pytz.timezone('Asia/Singapore'))).total_seconds() / (60 * 5 * 78 * 365),
                #     0.04,
                #     implied_volatility_window_deque[-1]['open']
                # )
                # put_price = black_scholes_put(
                #     implied_volatility_window_deque[-1]['spot_open'],
                #     round(implied_volatility_window_deque[-1]['spot_open']),
                #     (end_of_session_today - implied_volatility_window_deque[-1]['time'].astimezone(pytz.timezone('Asia/Singapore'))).total_seconds() / (60 * 5 * 78 * 365),
                #     0.04,
                #     implied_volatility_window_deque[-1]['open']
                # )
                bought_price = call_price + put_price
                await phantom_portfolio_value.create(
                    {
                        "time": implied_volatility_window_deque[-1]["time"],
                        "cash_portfolio_value": previous_cash_portfolio_value
                        + bought_price,
                        "option_portfolio_value": -bought_price,
                        "bought_price": bought_price,
                        "strike": round(
                            implied_volatility_window_deque[-1]["spot_open"]
                        ),
                        "peak": peak,
                        "paused": paused,
                        "resume_trades": resume_trades,
                    }
                )
            return []

        if len(current_spy_positions) > 0:
            target: List[TargetOptionPositionsDict] = []
            if (
                implied_volatility_window_deque[-1]["open"] < rolling75
                or current_time >= last_holding_time
            ):
                for spy_position in current_spy_positions:
                    target.append(
                        {
                            "stock": "SPY",
                            "strategy": ShortStraddle.strategy,
                            "expiry": spy_position["expiry"],
                            "strike": spy_position["strike"],
                            "multiplier": spy_position["multiplier"],
                            "option_type": spy_position["option_type"],
                            "avg_price": spy_position["strike"],
                            "quantity": 0,
                        }
                    )
                return target
                # Buy back short straddle
                # 'sell'
            for spy_position in current_spy_positions:
                query = text(f"""
                    SELECT
                        time
                    FROM trading.option_transactions
                    WHERE stock = 'SPY'
                        AND strategy = {spy_position["strategy"]}
                        AND expiry = {spy_position["expiry"]}
                        AND strike = {spy_position["strike"]}
                        AND multiplier = {spy_position["multiplier"]}
                        AND option_type = {spy_position["option_type"]}
                    ORDER BY time DESC
                    limit 1;
                """)  # Get all transactions of the last day
                query_res = await conn.execute(query)
                last_transaction = query_res.fetchone()
                assert last_transaction is not None
                time_bought = cast(datetime, last_transaction)
                if (
                    current_time
                    - time_bought.astimezone(pytz.timezone("Asia/Singapore"))
                ).total_seconds() >= longest_holding_period:
                    # Buy back Short Straddle
                    for spy_position in current_spy_positions:
                        target.append(
                            {
                                "stock": "SPY",
                                "strategy": ShortStraddle.strategy,
                                "expiry": spy_position["expiry"],
                                "strike": spy_position["strike"],
                                "multiplier": spy_position["multiplier"],
                                "option_type": spy_position["option_type"],
                                "avg_price": spy_position["strike"],
                                "quantity": 0,
                            }
                        )
            return target
            # 'sell'
        else:
            if current_time >= last_consideration_for_buying_time:
                return []  # i.e. no change - 'target_positions (i.e. no change)'
            if implied_volatility_window_deque[-1]["open"] > rolling90:
                # Buy into Short Straddle
                return [
                    {
                        "stock": "SPY",
                        "strategy": ShortStraddle.strategy,
                        "expiry": current_time.strftime("%Y%m%d"),
                        "strike": round(
                            implied_volatility_window_deque[-1]["spot_open"]
                        ),
                        "multiplier": 100,
                        "option_type": OptionType.C,
                        "avg_price": round(
                            implied_volatility_window_deque[-1]["spot_open"]
                        ),
                        "quantity": -position,
                    },
                    {
                        "stock": "SPY",
                        "strategy": ShortStraddle.strategy,
                        "expiry": current_time.strftime("%Y%m%d"),
                        "strike": round(
                            implied_volatility_window_deque[-1]["spot_open"]
                        ),
                        "multiplier": 100,
                        "option_type": OptionType.P,
                        "avg_price": round(
                            implied_volatility_window_deque[-1]["spot_open"]
                        ),
                        "quantity": -position,
                    },
                ]
        return []

    # @staticmethod
    # def get_buy_price(current_prices: Dict[str, float]) -> Dict[str, float]:
    #     pass
    #
    # @staticmethod
    # def get_sell_price(current_prices: Dict[str, float]) -> Dict[str, float]:
    #     pass

    @staticmethod
    # def get_buy_order(current_prices: Optional[Dict[str, float]], quantity: int) -> List[LocalOrder]:
    async def get_buy_order(
        stock: str, broker: DataBroker, quantity: float, strike: float
    ) -> List[FullOrder]:
        time_now = pytz.timezone("US/Eastern").localize(datetime.now())
        end_of_session_today = (
            datetime(time_now.year, time_now.month, time_now.day, 16, 0, 0)
            .astimezone(pytz.timezone("US/Eastern"))
            .strftime("%Y%m%d")
        )
        call_option = Option(
            "SPY", end_of_session_today, strike, "C", "SMART", "100", "USD"
        )
        put_option = Option(
            "SPY", end_of_session_today, strike, "P", "SMART", "100", "USD"
        )
        # await broker._qualify_contracts_async(call_option, put_option)
        call_price = await broker.get_current_price(call_option)
        put_price = await broker.get_current_price(put_option)

        full_contract = Contract(
            symbol="SPY",
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
            round(0.995 * (call_price + put_price), 2),
            tif="GTC",
            allOrNone=True,
        )
        order.smartComboRoutingParams.append(TagValue("NonGuaranteed", "1"))
        return [{"contract": full_contract, "order": order}]
        # Order order;
        # order.action = action;
        # order.orderType = "LMT";
        # order.totalQuantity = quantity;
        # order.lmtPrice = limitPrice;
        # if(nonGuaranteed){
        #     TagValueSPtr tag1(new TagValue("NonGuaranteed", "1"));
        #     order.smartComboRoutingParams.reset(new TagValueList());
        #     order.smartComboRoutingParams->push_back(tag1);
        # }

    @staticmethod
    # def get_sell_order(current_prices: Optional[Dict[str, float]], quantity: int) -> List[LocalOrder]:
    async def get_sell_order(
        stock: str, broker: DataBroker, quantity: float, strike: float
    ) -> List[FullOrder]:
        time_now = pytz.timezone("US/Eastern").localize(datetime.now())
        end_of_session_today = (
            datetime(time_now.year, time_now.month, time_now.day, 16, 0, 0)
            .astimezone(pytz.timezone("US/Eastern"))
            .strftime("%Y%m%d")
        )
        call_option = Option(
            "SPY", end_of_session_today, strike, "C", "SMART", "100", "USD"
        )
        put_option = Option(
            "SPY", end_of_session_today, strike, "P", "SMART", "100", "USD"
        )
        call_price = await broker.get_current_price(call_option)
        put_price = await broker.get_current_price(put_option)

        full_contract = Contract(
            symbol="SPY",
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
            round(1.005 * (call_price + put_price), 2),
            tif="GTC",
            allOrNone=True,
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
        assert len(quantity_differences) == 2
        indices = list(quantity_differences.keys())
        index1, index2 = indices[0], indices[1]
        assert (
            index1[0] == index2[0]
            and index1[1] == index2[1]
            and index1[2] == index2[2]
            and index1[3] == index2[3]
            and (
                (index1[4] == OptionType.C and index2[4] == OptionType.P)
                or (index1[4] == OptionType.P and index2[4] == OptionType.C)
            )
        )
        call_index = index1 if index1[4] == OptionType.C else index2

        if quantity_differences[call_index] < 0:
            return await ShortStraddle.get_buy_order(
                call_index[0], broker, quantity_differences[call_index], call_index[2]
            )
        else:
            return await ShortStraddle.get_sell_order(
                call_index[0], broker, quantity_differences[call_index], call_index[2]
            )

    @staticmethod
    @async_historical_options_data_wrapper
    @async_current_option_positions_wrapper
    @async_phantom_portfolio_value_wrapper
    async def execDetailsEvent(
        phantom_portfolio_value: AsyncPhantomPortfolioValueCRUD,
        current_option_positions: AsyncCurrentOptionPositionsCRUD,
        historical_options_data: AsyncHistoricalOptionsDataCRUD,
        trade: Trade,
        fill: Fill,
    ) -> None:
        await historical_options_data.create(
            {
                "stock": trade.contract.symbol,
                "expiry": trade.contract.lastTradeDateOrContractMonth,
                "strike": trade.contract.strike,
                "multiplier": float(trade.contract.multiplier),
                "option_type": cast(OptionType, trade.contract.right[0]),
                "time": fill.time,
                "open": fill.execution.price,
                "high": fill.execution.price,
                "low": fill.execution.price,
                "close": fill.execution.price,
                "volume": 0,
            }
        )

        current_positions = (
            await current_option_positions.get_current_positions_for_strategy(
                ShortStraddle.strategy
            )
        )

        if len(current_positions) == 1:
            previous_phantom_position = (
                await phantom_portfolio_value.get_actual_last_entry()
            )
            if previous_phantom_position["option_portfolio_value"] != 0:
                await phantom_portfolio_value.create_or_update(
                    {
                        "time": ShortStraddle.round_down_to_nearest_5_minutes(
                            datetime.now(pytz.timezone("US/Eastern"))
                        ),
                        "cash_portfolio_value": previous_phantom_position[
                            "cash_portfolio_value"
                        ]
                        - fill.execution.price * 100,
                        "option_portfolio_value": previous_phantom_position[
                            "option_portfolio_value"
                        ]
                        + fill.execution.price * 100,
                        "bought_price": previous_phantom_position["bought_price"],
                        "strike": previous_phantom_position["strike"],
                        "peak": previous_phantom_position["peak"],
                        "paused": previous_phantom_position["paused"],
                        "resume_trades": previous_phantom_position["resume_trades"],
                    }
                )
        if len(current_positions) == 0:
            # Assumes phantom_portfolio_value fully updated
            previous_phantom_position = (
                await phantom_portfolio_value.get_actual_last_entry()
            )
            if previous_phantom_position["option_portfolio_value"] != 0:
                await phantom_portfolio_value.create_or_update(
                    {
                        "time": ShortStraddle.round_down_to_nearest_5_minutes(
                            pytz.timezone("US/Eastern").localize(datetime.now())
                        ),
                        "cash_portfolio_value": previous_phantom_position[
                            "cash_portfolio_value"
                        ]
                        - fill.execution.price * 100,
                        "option_portfolio_value": 0,
                        "bought_price": previous_phantom_position["bought_price"],
                        "strike": previous_phantom_position["strike"],
                        "peak": max(
                            previous_phantom_position["peak"],
                            previous_phantom_position["cash_portfolio_value"]
                            - fill.execution.price * 100,
                        ),
                        "paused": previous_phantom_position["paused"],
                        "resume_trades": previous_phantom_position["resume_trades"],
                    }
                )

        if len(current_positions) > 2:
            print(
                f"What the fuck i have more than 2 option positions open for ShortStraddle: {trade},\n {fill}"
            )
            return

        previous_phantom_position = (
            await phantom_portfolio_value.get_actual_last_entry()
        )
        bought_price = sum(
            [current_position["avg_price"] for current_position in current_positions]
        )
        now = pytz.timezone("US/Eastern").localize(datetime.now())
        current_interval = pytz.timezone("US/Eastern").localize(
            datetime(
                now.year,
                now.month,
                now.day,
                now.hour,
                now.minute // 5,
                0,
            )
        )
        await phantom_portfolio_value.create(
            {
                "time": current_interval,
                "cash_portfolio_value": previous_phantom_position[
                    "cash_portfolio_value"
                ]
                + bought_price * 100,
                "option_portfolio_value": -bought_price * 100,
                "bought_price": bought_price,
                "strike": current_positions[0]["strike"],
                "peak": previous_phantom_position["peak"],
                "paused": previous_phantom_position["paused"],
                "resume_trades": previous_phantom_position["resume_trades"],
            }
        )

        # --------------- CHECK IF CURRENT POSITIONS HAS 2 POSITIONS OPEN NOW -------
        # --------------- IF 2 positions open, get avg price n update phantom portfolio ------
        # ---------------- IF not do nth --------

    @staticmethod
    async def get_stocks(broker: DataBroker) -> List[Contract]:
        contract = Stock("SPY", "SMART", "USD")
        await broker.ib.qualifyContractsAsync(contract)
        return [contract]

    @async_historical_data_wrapper
    @async_historical_volatility_data_wrapper
    @staticmethod
    async def update_historical_data_to_present(
        historical_volatility_data: AsyncHistoricalVolatilityDataCRUD,
        historical_data: AsyncHistoricalDataCRUD,
        broker: DataBroker,
    ) -> None:
        # def get_last_trading_day_start_time() -> datetime:
        #     eastern = pytz.timezone('US/Eastern')
        #     nyse = mcal.get_calendar('NYSE')
        #
        #     now = datetime.now(eastern)
        #     schedule = nyse.valid_days(end_date=now.strftime('%Y-%m-%d'), count=2)
        #
        #     # Get the most recent valid trading day before today
        #     last_trading_day = schedule[-1].to_pydatetime()
        #     return eastern.localize(datetime.combine(last_trading_day.date(), datetime.min.time()))

        time_from_required = pytz.timezone("US/Eastern").localize(
            datetime.now()
        ) - timedelta(days=60)
        # time_from_required = get_last_trading_day_start_time()

        historical_data_count = await historical_data.read_stock_time_count(
            "SPY", time_from_required
        )
        historical_volatility_data_count = (
            await historical_volatility_data.read_stock_time_count(
                "SPY", time_from_required
            )
        )
        print(
            f"Historical Data Count: {historical_data_count} rows, {historical_data_count / 78} days in rows"
        )

        if (
            historical_data_count >= 42 * 78
            and historical_volatility_data_count >= 42 * 78
        ):
            return

        contract = Stock("SPY", "SMART", "USD")
        await broker.ib.qualifyContractsAsync(contract)

        current_end_time = pytz.timezone("US/Eastern").localize(datetime.now())

        while current_end_time > time_from_required:
            print(f"Requesting data up to {current_end_time.isoformat()} for {'SPY'}")
            bars = await broker.ib.reqHistoricalDataAsync(
                contract,
                endDateTime=time_from_required + timedelta(days=30),
                durationStr="21 D",
                barSizeSetting="5 mins",
                whatToShow="TRADES",
                useRTH=True,
                formatDate=2,
                keepUpToDate=False,
            )

            if not bars:
                print(f"No more data available for {'SPY'} at {current_end_time}")
                break

            for bar in bars:
                assert isinstance(bar.date, datetime), (
                    f"bar.date must be datetime, got {type(bar.date)}"
                )

                await historical_data.create_or_update(
                    {
                        "stock": "SPY",
                        "time": bar.date,
                        "open": bar.open,
                        "high": bar.high,
                        "low": bar.low,
                        "close": bar.close,
                        "volume": int(bar.volume),
                    }
                )

            bars = await broker.ib.reqHistoricalDataAsync(
                contract,
                endDateTime=time_from_required + timedelta(days=30),
                durationStr="21 D",
                barSizeSetting="5 mins",
                # whatToShow='TRADES',
                whatToShow="OPTION_IMPLIED_VOLATILITY",
                useRTH=True,
                formatDate=2,
                keepUpToDate=False,
            )

            if not bars:
                print(f"No more data available for {'SPY'} at {current_end_time}")
                break

            for bar in bars:
                assert isinstance(bar.date, datetime), (
                    f"bar.date must be datetime, got {type(bar.date)}"
                )
                await historical_volatility_data.create_or_update(
                    {
                        "stock": "SPY",
                        "time": bar.date,
                        "open": bar.open,
                        "high": bar.high,
                        "low": bar.low,
                        "close": bar.close,
                    }
                )

            time_from_required += timedelta(days=21)
