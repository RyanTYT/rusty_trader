from app.services.models.AsyncModelsCRUD import (
    AsyncCurrentStockPositionsCRUD,
    AsyncHistoricalDataCRUD,
    AsyncStrategyCRUD,
)
from app.services.strategy.StockStrategy import StockStrategy as StrategyClass
from app.utils.db import (
    async_with_db_session_for_model,
)
from app.models import (
    CurrentStockPositions,
    HistoricalData,
    Strategy,
)
from app.models_types import Status, TargetStockPositionsDict
from typing import List, Dict, cast
from app.services.broker.DataBroker import DataBroker, FullOrder
from ib_async.contract import Contract, Stock
from ib_async.order import MarketOrder, StopOrder
from datetime import datetime, timezone
import pytz

async_historical_data_wrapper = async_with_db_session_for_model(
    AsyncHistoricalDataCRUD, HistoricalData
)
async_strategy_wrapper = async_with_db_session_for_model(AsyncStrategyCRUD, Strategy)
async_current_stock_positions_wrapper = async_with_db_session_for_model(
    AsyncCurrentStockPositionsCRUD, CurrentStockPositions
)


class Noise(StrategyClass):
    strategy = "Noise"

    @async_strategy_wrapper
    @staticmethod
    async def create_strategy(strategy: AsyncStrategyCRUD) -> None:
        strategy_exists = await strategy.read({"strategy": Noise.strategy})
        if len(strategy_exists) > 0:
            return

        await strategy.create(
            {
                "strategy": Noise.strategy,
                "capital": 100000,
                "initial_capital": 100000,
                "status": Status.active,
            }
        )

    # Override
    @async_current_stock_positions_wrapper
    @async_historical_data_wrapper
    @async_strategy_wrapper
    @staticmethod
    async def get_weights(
        strategy: AsyncStrategyCRUD,
        historical_data: AsyncHistoricalDataCRUD,
        current_stock_positions: AsyncCurrentStockPositionsCRUD,
        broker: DataBroker,
    ) -> List[TargetStockPositionsDict]:
        """ """
        quantity = 10
        noise_val = await historical_data.avg_move_since_open("QQQ")
        assert noise_val
        last_max_open = await historical_data.get_last_max_open("QQQ")
        upper_noise, lower_noise = (
            (1 + noise_val) * last_max_open,
            (1 - noise_val) * last_max_open,
        )
        # TO update properly
        vwap = await broker.get_current_price(Stock("QQQ", "SMART", "USD"), vwap=True)
        current_prices = await historical_data.read_stock("QQQ", limit=1)
        assert len(current_prices) > 0
        current_price = current_prices[0]
        daily_vol = await historical_data.get_daily_vol("QQQ")
        if daily_vol > 0.04:
            quantity = 5
        elif daily_vol < 0.01:
            quantity = 20

        current_position = (
            await current_stock_positions.get_current_positions_for_strategy(
                Noise.strategy
            )
        )
        if len(current_position) > 0:
            if current_price["close"] < upper_noise or current_price["close"] <= vwap:
                return [
                    {
                        "stock": "QQQ",
                        "strategy": Noise.strategy,
                        "stop_limit": current_price["close"] - 50,
                        "avg_price": current_price["close"],
                        "quantity": 0,
                    }
                ]

        if current_price["close"] > upper_noise and (
            current_price["time"].minute == 0 or current_price["time"].minute == 30
        ):
            return [
                {
                    "stock": "QQQ",
                    "strategy": Noise.strategy,
                    "stop_limit": current_price["close"] - 50,
                    "avg_price": current_price["close"],
                    "quantity": quantity,
                }
            ]
        return []

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
        contract = Stock(stock, "SMART", "USD")
        await broker.ib.qualifyContractsAsync(contract)
        current_price = await broker.get_current_price(contract)
        orders: List[FullOrder] = []

        order_id = broker.ib.client.getReqId()
        order = MarketOrder("BUY", quantity, orderId=order_id)
        order.transmit = False

        attached_stop_limit = StopOrder(
            "SELL", quantity, round(0.9 * current_price * 20) / 20
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
        contract = Stock(stock, "SMART", "USD")

        orders: List[FullOrder] = []
        order = MarketOrder("SELL", quantity)
        orders.append({"contract": contract, "order": order})
        return orders

    @staticmethod
    async def get_stocks(broker: DataBroker) -> List[Contract]:
        contract = Stock(symbol="QQQ", exchange="SMART", currency="USD")
        await broker.ib.qualifyContractsAsync(contract)
        return [contract]

    @async_historical_data_wrapper
    @staticmethod
    async def update_historical_data_to_present(
        historical_data: AsyncHistoricalDataCRUD, broker: DataBroker
    ) -> None:
        if await historical_data.has_minimum_daily_ohlcv("QQQ"):
            return

        print("Requesting data for QQQ")
        end_time = datetime.now(timezone.utc).astimezone(pytz.timezone("US/Eastern"))
        bars = await broker.ib.reqHistoricalDataAsync(
            Stock("QQQ", "SMART", "USD"),
            endDateTime=end_time,
            durationStr="50 D",
            barSizeSetting="5 mins",
            whatToShow="TRADES",
            useRTH=True,
            formatDate=2,
            keepUpToDate=False,
        )

        if not bars:
            print(f"No data available for QQQ at {end_time}")
            return

        await historical_data.create_or_update_all(
            [
                {
                    "stock": "QQQ",
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

        await historical_data.refresh_daily_data()

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
