from app.services.strategy.Strategy import Strategy
from app.utils.db import with_db_session_for_model
from app.services.models.ModelsCRUD import HistoricalDataCRUD, CurrentPositionCRUD
from app.models import HistoricalData, CurrentPosition
from ib_async.contract import Stock
from app.models_types import TargetPositionDict
from typing import TypeVar, List, Dict, cast
from app.services.broker.DataBroker import DataBroker
import pandas as pd
import numpy as np

historical_data_wrapper = with_db_session_for_model(
    HistoricalDataCRUD, HistoricalData, "historical_data"
)
current_position_wrapper = with_db_session_for_model(
    CurrentPositionCRUD, CurrentPosition, "current_position"
)
historical_data_crud_type = TypeVar(
    "historical_data_crud_type", bound=HistoricalDataCRUD
)
current_position_crud_type = TypeVar(
    "current_position_crud_type", bound=CurrentPositionCRUD
)


class LinReg(Strategy):
    strategy = "LinReg"

    # Override
    @current_position_wrapper
    @historical_data_wrapper
    @staticmethod
    async def get_weights(
        historical_data: historical_data_crud_type,
        current_position: current_position_crud_type,
        broker: DataBroker,
        scaling: float = 1,
    ) -> List[TargetPositionDict]:
        """
        Use scaling to change quantity of stock to buy
        """
        buy_sell = 0
        # From past experience, only max 10 lookback period required
        spy_past_data_list = historical_data.read_stock("SPY", 50)

        spy_past_data = pd.DataFrame(spy_past_data_list)
        spy_past_data["timestamp_seconds"] = (
            spy_past_data["time"].astype(np.int64) // 10**9
        )

        def pred_next(ser: pd.Series[float], full_df: pd.DataFrame) -> float:
            """
            df: should include all data points including the point of prediction
            """
            df = full_df.loc[ser.index]

            x = df["timestamp_seconds"][:-1]
            y = df["close"][:-1]
            slope, intercept = np.polyfit(x, y, 1)
            next_y = cast(float, slope * df["timestamp_seconds"].iloc[-1] + intercept)

            return next_y

        window = 25
        spy_past_data["close_reg_20"] = (
            spy_past_data["close"]
            .rolling(window=window + 1)
            .apply(pred_next, args=(spy_past_data,))
        )

        # Drop data with NA values cos of predictions
        spy_past_data = spy_past_data[window:]

        current = spy_past_data.iloc[0]
        if current["close"] < current["close_reg_20"]:
            # if currently increasing, go next
            if current["close"] >= spy_past_data[1]["close"]:
                buy_sell = 0

            # look back to check whether it is second downturn in downturn
            is_second_down = False
            for j in range(1, 9):
                if (
                    spy_past_data.iloc[j]["close"]
                    >= spy_past_data.iloc[j]["close_reg_20"]
                ):
                    break
                is_second_down = (
                    spy_past_data.iloc[j]["close"] > spy_past_data.iloc[j + 1]["close"]
                )
                if is_second_down:
                    buy_sell = 1
                    break
        elif not current["close"] == current["close_reg_20"]:
            buy_sell = -1

        spy_current_position = current_position.read(
            {"stock": "SPY", "strategy": LinReg.strategy}
        )
        contract = Stock("SPY", "SMART", "USD")
        current_price = await broker.get_current_price(contract)
        # current_price = 400

        target_positions = [
            cast(
                TargetPositionDict,
                {
                    "stock": "SPY",
                    "avg_price_bought": LinReg.get_buy_price({"SPY": current_price})[
                        "SPY"
                    ],
                    "current_price": current_price,
                    "quantity": 1.0 * scaling,
                    "stop_limit": 0.9 * current_price,
                    "strategy": LinReg.strategy,
                },
            )
        ]
        if (not spy_current_position and buy_sell != 1) or buy_sell == -1:
            target_positions[0]["quantity"] = 0
        return target_positions

    @staticmethod
    def get_buy_price(current_prices: Dict[str, float]) -> Dict[str, float]:
        return {"SPY": current_prices["SPY"] - 2}

    @staticmethod
    def get_sell_price(current_prices: Dict[str, float]) -> Dict[str, float]:
        return {"SPY": current_prices["SPY"] * 1.01}
