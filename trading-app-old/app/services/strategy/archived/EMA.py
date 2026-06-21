from app.services.strategy.StockStrategy import StockStrategy as StrategyClass
from app.utils.db import with_db_session_for_model, with_engine
from app.services.models.ModelsCRUD import StrategyCRUD, HistoricalDataCRUD, CurrentStockPositionsCRUD
from app.models import HistoricalData, CurrentStockPositions, Strategy
from app.models_types import TargetStockPositionsDict, StrategyDictPrimaryKeys
from typing import TypeVar, List, Dict, Tuple, cast, Any
from app.services.broker.DataBroker import DataBroker, FullOrder
import numpy as np
from sqlalchemy import text
from sqlalchemy.engine import Connection
from ib_async.contract import Stock
from ib_async.order import MarketOrder, StopOrder

historical_data_wrapper = with_db_session_for_model(HistoricalDataCRUD, HistoricalData, 'historical_data')
current_stock_position_wrapper = with_db_session_for_model(CurrentStockPositionsCRUD, CurrentStockPositions, 'current_stock_positions')
strategy_wrapper = with_db_session_for_model(StrategyCRUD, Strategy, 'strategy')
historical_data_crud_type = TypeVar("historical_data_crud_type", bound=HistoricalDataCRUD)
current_position_crud_type = TypeVar("current_position_crud_type", bound=CurrentStockPositionsCRUD)


class EMA(StrategyClass):
    strategy = 'EMA'

    # Override
    @current_stock_position_wrapper
    @with_engine
    @strategy_wrapper
    @staticmethod
    async def get_weights(
        strategy: StrategyCRUD,
        conn: Connection,
        current_stock_positions: CurrentStockPositionsCRUD,
        broker: DataBroker
    ) -> List[TargetStockPositionsDict]:
        """
        """
        leverage = 4
        strategy_amount = strategy.read(cast(StrategyDictPrimaryKeys, {"strategy": EMA.strategy}))[0]["capital"]
        # current_position_strat = current_position.read({"stock": "SPY", "strategy": EMA.strategy})

        contract = Stock("SPY", "SMART", "USD")
        current_price = await broker.get_current_price(contract)

        # From past experience, only max 10 lookback period required
        query = text("""
            SELECT
                time_bucket('5 minutes', time) AS bucket_time,
                stock,
                first(open, time) AS open,
                MAX(high) AS high,
                MIN(low) AS low,
                last(close, time) AS close,
                SUM(volume) AS total_volume
            FROM market_data.historical_data
            WHERE
                (time AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::TIME
                BETWEEN '09:30:00' AND '16:00:00'
            GROUP BY bucket_time, stock
            ORDER BY bucket_time DESC
            LIMIT 62;
        """)
        # For precision up to 0.001, i need at least 61 previous points for span=10
        recent_5_min_res = conn.execute(query)
        recent_5_min: List[float] = [row.open for row in recent_5_min_res][::-1]
        ewm_span = 10

        alpha = 2 / (ewm_span + 1)  # Smoothing factor
        ewm_values_5min = np.zeros(62)  # Array to store results

        ewm_values_5min[0] = recent_5_min[0]  # First value is same as the input
        for i in range(1, 62):
            ewm_values_5min[i] = alpha * recent_5_min[i] + (1 - alpha) * ewm_values_5min[i - 1]  # EWM formula
        ewm_5min_buy_signal = recent_5_min[-1] > ewm_values_5min[61]

        query = text("""
            SELECT
                time_bucket('1 day', time) AS bucket_time,
                stock,
                first(open, time) AS open,
                MAX(high) AS high,
                MIN(low) AS low,
                last(close, time) AS close,
                SUM(volume) AS total_volume
            FROM market_data.historical_data
            GROUP BY bucket_time, stock
            ORDER BY bucket_time DESC
            LIMIT 20;
        """)
        # For precision up to 0.001, i need at least 19 previous points for span=4
        recent_daily_res = conn.execute(query)
        recent_daily = [row.open for row in recent_daily_res][::-1]
        ewm_span = 4

        alpha = 2 / (ewm_span + 1)  # Smoothing factor
        ewm_values_daily = np.zeros(20)  # Array to store results

        ewm_values_daily[0] = recent_daily[0]  # First value is same as the input
        for i in range(1, 20):
            ewm_values_daily[i] = alpha * recent_daily[i] + (1 - alpha) * ewm_values_daily[i - 1]  # EWM formula
        ewm_daily_buy_signal = recent_daily[-1] > ewm_values_daily[19]

        increasing_buy_signal = recent_5_min[-1] > recent_5_min[-2]

        combined_buy_signal = ewm_5min_buy_signal * ewm_daily_buy_signal * increasing_buy_signal
        sell_signal = recent_5_min[-1] < ewm_values_5min[61]

        # if combined_buy_signal and len(current_position_strat) == 0:
        if combined_buy_signal:
            capital_allocation = leverage * strategy_amount
            target_position = int(capital_allocation / current_price)  # to round down

            return [{
                'stock': 'SPY',
                'avg_price': EMA.get_buy_price({'SPY': current_price})['SPY'],
                'quantity': target_position,
                'stop_limit': 0.9 * current_price,
                'strategy': EMA.strategy
            }]
        # if sell_signal and current_position_strat:
        elif sell_signal:
            return [{
                'stock': 'SPY',
                'avg_price': EMA.get_sell_price({'SPY': current_price})['SPY'],
                'quantity': 0,
                'stop_limit': 0.9 * current_price,
                'strategy': EMA.strategy
            }]
        return []

    @staticmethod
    def get_buy_price(current_prices: Dict[str, float]) -> Dict[str, float]:
        return {'SPY': current_prices['SPY']}

    @staticmethod
    def get_sell_price(current_prices: Dict[str, float]) -> Dict[str, float]:
        return {'SPY': current_prices['SPY'] * 1.01}

    @staticmethod
    async def get_buy_order(stock: str, broker: DataBroker, quantity: int, quantity_to_insure: int, avg_price_bought: float) -> List[FullOrder]:
        contract = Stock(stock, "SMART", "USD")
        current_price = await broker.get_current_price(contract)
        contract = Stock(stock, 'SMART', 'USD')
        orders: List[FullOrder] = []

        order = MarketOrder('BUY', quantity)
        order.transmit = False

        attached_stop_limit = StopOrder('SELL', quantity, round(0.9 * current_price * 20) / 20)
        attached_stop_limit.transmit = True
        attached_stop_limit.parentId = order.orderId

        orders.append({
            "contract": contract,
            "order": order
        })
        orders.append({
            "contract": contract,
            "order": attached_stop_limit
        })

        stop_limit = StopOrder('SELL', quantity_to_insure, round(0.9 * avg_price_bought * 20) / 20)
        # Stop Limit for Current Order
        orders.append({
            "contract": contract,
            "order": stop_limit
        })
        return orders

    @staticmethod
    async def get_sell_order(stock: str, broker: DataBroker, quantity: int, avg_price: float) -> List[FullOrder]:
        # current_price = await broker.get_current_price(stock, 'SMART', 'USD')
        contract = Stock(stock, 'SMART', 'USD')

        orders: List[FullOrder] = []
        # order = LimitOrder('SELL', quantity, EMA.get_sell_price({"SPY": current_price})['SPY'])
        order = MarketOrder('SELL', quantity)
        orders.append({
            "contract": contract,
            "order": order
        })
        return orders

    @staticmethod
    async def get_stocks() -> List[Dict[str, Any]]:
        return [
            {
                "symbol": "SPY",
                "exchange": "SMART",
                "currency": "USD"
            }
        ]
        # return [("Stock", "SPY", "SMART", "USD")]

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
