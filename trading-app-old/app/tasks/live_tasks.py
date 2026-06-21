#
# from typing import List, Dict, Any, cast
#
# # from app.utils.custom_logging import CustomLogger
# from app.services.broker.Broker import Broker, LocalOrder
# from app.models_types import CurrentPositionDict, TargetPositionDict
# from app.services.strategy.Strategy import Strategy
# from app.services.models.ModelsCRUD import CurrentPositionCRUD, TargetPositionCRUD
# from app.models import CurrentPosition, TargetPosition
# from app.utils.db import with_db_session_for_model
#
# from datetime import datetime
# import pytz
#
#
# class TargetPositionWithStrategy(TargetPositionDict):
#     actual_strategy: Strategy
#
#
# current_position_wrapper_async = with_db_session_for_model(CurrentPositionCRUD, CurrentPosition, 'current_position')
# target_position_wrapper_async = with_db_session_for_model(TargetPositionCRUD, TargetPosition, 'target_position')
#
# # GRAND NOTE: I realised after a bit that this should be atomic
# #    - i.e. One Atom - One Strategy, One Broker - such that if strategies have overlapping stocks, they should be considered in a separate atom
# #    - as such, some functions may be less useful and weirder than expected
# # Quirks:
# #    - update_target_positions_from_strategies(): one-liner function - useless
# #    - get_orders_needed(): Assumes stock appears uniquely for from_position and to_position
#
# # NOTE:
# # Set threshold of strategy to be when predicted worst drawdown could potentially wipe out at most 25% of portfolio
# # TBD (TO BE DEFINED)
# # StrategyDistribution
# #   get_weights(stocks: List[str]) -> {'AAPL': {'weight': <>, 'strategy_to_buy_sell': <VWAP, ...>}}
#
#
# @target_position_wrapper_async
# async def update_target_positions_from_strategies(target_position: TargetPositionCRUD, broker: Broker, strategies: List[Strategy]) -> List[TargetPositionWithStrategy]:
#     """
#     """
#     target_positions = []
#     result: List[TargetPositionWithStrategy] = []
#     for strategy in strategies:
#         for target_position_strat in await strategy.get_weights(broker, 1):
#             target_positions.append(target_position_strat)
#
#             any_target_position = cast(Dict[str, Any], target_position_strat.copy())
#             any_target_position['actual_strategy'] = strategy
#             target_position_with_strat: TargetPositionWithStrategy = cast(TargetPositionWithStrategy, any_target_position)
#             result.append(target_position_with_strat)
#
#     target_position.create_or_update_all(target_positions)
#
#     return result
#
#
# @current_position_wrapper_async
# async def update_current_positions_from_broker(current_position: CurrentPositionCRUD, broker: Broker) -> List[CurrentPositionDict]:
#     current_positions: List[CurrentPositionDict] = await broker.get_current_positions()
#     current_positions_stocks = set([i['stock'] for i in current_positions])
#
#     # Reduce all current positions that should not exist to 0
#     local_current_positions = current_position.read(None)
#     for local_current_position in local_current_positions:
#         if local_current_position['stock'] not in current_positions_stocks:
#             current_position.delete(local_current_position)
#
#     # Update all current positions
#     current_position.create_or_update_all(current_positions)
#
#     return local_current_positions
#
#
# async def get_orders_needed(broker: Broker, from_position: List[CurrentPositionDict], to_position: List[TargetPositionWithStrategy]) -> List[LocalOrder]:
#     from_position_dict = {
#         position['stock']: position
#         for position in from_position
#     }
#     to_position_dict = {
#         position['stock']: position
#         for position in to_position
#     }
#
#     filler_actual_strategy = to_position[0]['actual_strategy']
#
#     for stock in from_position_dict:
#         if stock not in to_position_dict:
#             updated_to_position_stock: Dict[str, Any] = cast(Dict[str, Any], from_position_dict[stock].copy())
#
#             updated_to_position_stock['quantity'] = 0
#             # VERYYRYRYRYRYRY BUGGYYGYGYGYGYG
#             updated_to_position_stock['actual_strategy'] = filler_actual_strategy
#             to_position_dict[stock] = cast(TargetPositionWithStrategy, updated_to_position_stock)
#
#     current_date = datetime.now(pytz.timezone('US/Eastern')).date().strftime("%Y%m%d")
#     orders: List[LocalOrder] = []
#     for stock, target_position in to_position_dict.items():
#         target_qty = target_position['quantity']
#         current_qty = 0 if stock not in from_position_dict else from_position_dict[stock]['quantity']
#         if current_qty == target_qty:
#             continue
#         current_price = await broker.get_current_price(stock)
#         if target_qty < current_qty:
#             # sell
#             qty = current_qty - target_qty
#             target_order = target_position.copy()
#             target_order['quantity'] = qty
#             orders.append({
#                 "stock": stock,
#                 "quantity": qty,
#                 "price": round(target_position['actual_strategy'].get_sell_price({'SPY': current_price})['SPY'] * 20) / 20,
#                 "order_type": "SELL",
#                 # --- BELOW not NEEDED ---
#                 "order_details": {
#                     "max_pct_vol": "0.1",
#                     "start_time": f"{current_date} 09:30:00 US/Eastern",
#                     "end_time": f"{current_date} 16:00:00 US/Eastern",
#                     "allow_past_end_time": "0",
#                     "no_take_liq": "True"
#                 }
#             })
#
#         else:
#             qty = target_qty - current_qty
#             orders.append({
#                 "stock": stock,
#                 "quantity": qty,
#                 "price": round(target_position['avg_price_bought'] * 20) / 20,
#                 "order_type": "LIMIT",
#                 "order_details": {
#                     "max_pct_vol": "0.1",
#                     "start_time": f"{current_date} 09:30:00 US/Eastern",
#                     "end_time": f"{current_date} 16:00:00 US/Eastern",
#                     "allow_past_end_time": "0",
#                     "no_take_liq": "True"
#                 }
#             })
#             orders.append({
#                 "stock": stock,
#                 "quantity": target_qty,
#                 "price": round(0.9 * (current_price) * 20) / 20,
#                 "order_type": "STOP",
#                 # --- Below NOT NEEDED ---
#                 "order_details": {
#                     "max_pct_vol": "0.1",
#                     "start_time": f"{current_date} 09:30:00 US/Eastern",
#                     "end_time": f"{current_date} 16:00:00 US/Eastern",
#                     "allow_past_end_time": "0",
#                     "no_take_liq": "True"
#                 }
#             })
#     return orders
#
# async def on_price_update():
#     update_target_positions_from_strategies()
#     get_orders_needed()
