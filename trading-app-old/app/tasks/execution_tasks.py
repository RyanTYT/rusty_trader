import os
from typing import List, cast, Dict, Tuple
import requests

# from app.utils.custom_logging import CustomLogger
from app.services.models.AsyncModelsCRUD import (
    AsyncCurrentOptionPositionsCRUD,
    AsyncCurrentStockPositionsCRUD,
    AsyncStrategyCRUD,
    AsyncTargetOptionPositionsCRUD,
    AsyncTargetStockPositionsCRUD,
)
from app.services.strategy.StockStrategy import StockStrategy
from app.services.broker.DataBroker import DataBroker, FullOrder
from app.services.broker.Broker import Broker
from app.services.models.ModelsCRUD import (
    CurrentStockPositionsCRUD,
    CurrentOptionPositionsCRUD,
    TargetStockPositionsCRUD,
    TargetOptionPositionsCRUD,
    StrategyCRUD,
)
from app.models import (
    CurrentStockPositions,
    CurrentOptionPositions,
    TargetStockPositions,
    TargetOptionPositions,
    Strategy as StrategyModel,
)
from app.utils.custom_logging import CustomLogger
from app.utils.db import async_with_db_session_for_model
from ib_async.contract import Stock, Option, Contract
from ib_async.order import MarketOrder
from app.models_types import (
    Status,
    OptionType,
    TargetOptionPositionsDict,
    TargetStockPositionsDict,
)


RUST_BACKEND_URL = os.getenv("RUST_BACKEND_URL")
BEARER_TOKEN = os.getenv("BEARER_TOKEN")

async_current_stock_positions_wrapper = async_with_db_session_for_model(
    AsyncCurrentStockPositionsCRUD, CurrentStockPositions
)
async_current_option_positions_wrapper = async_with_db_session_for_model(
    AsyncCurrentOptionPositionsCRUD, CurrentOptionPositions
)
async_target_stock_positions_wrapper = async_with_db_session_for_model(
    AsyncTargetStockPositionsCRUD, TargetStockPositions
)
async_target_option_positions_wrapper = async_with_db_session_for_model(
    AsyncTargetOptionPositionsCRUD, TargetOptionPositions
)
async_strategy_wrapper = async_with_db_session_for_model(
    AsyncStrategyCRUD, StrategyModel
)

# GRAND NOTE: I realised after a bit that this should be atomic
#    - i.e. One Atom - One Strategy, One Broker - such that if strategies have overlapping stocks, they should be considered in a separate atom
#    - as such, some functions may be less useful and weirder than expected
# Quirks:
#    - update_target_positions_from_strategies(): one-liner function - useless
#    - get_orders_needed(): Assumes stock appears uniquely for from_position and to_position

# NOTE:
# Set threshold of strategy to be when predicted worst drawdown could potentially wipe out at most 25% of portfolio
# TBD (TO BE DEFINED)
# StrategyDistribution
#   get_weights(stocks: List[str]) -> {'AAPL': {'weight': <>, 'strategy_to_buy_sell': <VWAP, ...>}}


# async def update_target_position_and_send_orders_for_broker_with_live_data(
#     broker: Broker, live_data: Dict[Contract, float]
# ) -> None:
#     orders_required = []
#     if broker.stock_strategy is not None:
#         orders_required.extend(
#             await update_target_position_and_send_orders_for_broker_stocks(broker)
#         )
#     if broker.option_strategy is not None:
#         orders_required.extend(
#             await update_target_position_and_send_orders_for_broker_options(broker)
#         )
#
#     broker.cancel_all_open_orders()
#     await broker.send_orders(orders_required)


async def update_target_position_and_send_orders_for_broker(broker: Broker) -> None:
    orders_required = []
    if broker.stock_strategy is not None:
        orders_required.extend(
            await update_target_position_and_send_orders_for_broker_stocks(broker)
        )
    if broker.option_strategy is not None:
        orders_required.extend(
            await update_target_position_and_send_orders_for_broker_options(broker)
        )

    broker.cancel_all_open_orders()
    await broker.send_orders(orders_required)


@async_target_stock_positions_wrapper
@async_current_stock_positions_wrapper
@async_strategy_wrapper
async def update_target_position_and_send_orders_for_broker_stocks(
    strategy_crud: AsyncStrategyCRUD,
    current_stock_positions: AsyncCurrentStockPositionsCRUD,
    target_stock_positions: AsyncTargetStockPositionsCRUD,
    broker: Broker,
) -> List[FullOrder]:
    assert broker.stock_strategy is not None
    """
    """
    strategy_details = (await strategy_crud.read({"strategy": broker.strategy}))[0]
    target_positions: List[TargetStockPositionsDict] = []
    target_positions = await broker.stock_strategy.get_weights(cast(DataBroker, broker))

    if strategy_details["status"] != Status.active.value:
        target_positions = [
            {
                "stock": stock["stock"],
                "strategy": broker.strategy,
                "stop_limit": 0.0,
                "quantity": 0.0,
                "avg_price": 0.0,
            }
            for stock in await current_stock_positions.get_current_positions_for_strategy(
                broker.strategy
            )
        ]
    if broker.stock_strategy.to_clear_before_sending:
        for stock in await broker.stock_strategy.get_stocks(broker):
            await target_stock_positions.clear_positions(broker.strategy, stock.symbol)
    await target_stock_positions.create_or_update_all(target_positions)

    orders_required: List[FullOrder] = []
    for order_details in await target_stock_positions.get_order_quantities_required(
        broker.stock_strategy
    ):
        CustomLogger("update_target_positions_from_strategies").info(
            f"Dets: {order_details}"
        )
        if order_details["quantity_difference"] == 0:
            continue
        if strategy_details["status"] == Status.inactive.value:
            # Make market order sells
            contract = Stock(order_details["stock"], "SMART", "USD")
            order = MarketOrder(
                "SELL" if order_details["quantity_difference"] < 0 else "BUY",
                abs(order_details["quantity_difference"]),
            )
            orders_required.append({"contract": contract, "order": order})
            continue

        if order_details["quantity_difference"] < 0:
            orders_required.extend(
                await broker.stock_strategy.get_sell_order(
                    order_details["stock"],
                    cast(DataBroker, broker),
                    -order_details["quantity_difference"],
                    order_details["avg_price"],
                )
            )
        else:
            orders_required.extend(
                await broker.stock_strategy.get_buy_order(
                    order_details["stock"],
                    cast(DataBroker, broker),
                    order_details["quantity_difference"],
                    order_details["quantity"],
                    order_details["avg_price"],
                )
            )
    return orders_required


@async_target_option_positions_wrapper
@async_current_option_positions_wrapper
@async_strategy_wrapper
async def update_target_position_and_send_orders_for_broker_options(
    strategy_crud: AsyncStrategyCRUD,
    current_options_positions: AsyncCurrentOptionPositionsCRUD,
    target_option_positions: AsyncTargetOptionPositionsCRUD,
    broker: Broker,
) -> List[FullOrder]:
    """ """
    assert broker.option_strategy is not None
    strategy_details = (await strategy_crud.read({"strategy": broker.strategy}))[0]
    target_positions: List[TargetOptionPositionsDict] = []
    target_positions = await broker.option_strategy.get_weights(
        cast(DataBroker, broker)
    )

    if strategy_details["status"] != Status.active.value:
        target_positions = [
            {
                "stock": stock["stock"],
                "strategy": broker.strategy,
                "expiry": stock["expiry"],
                "strike": stock["strike"],
                "multiplier": stock["multiplier"],
                "option_type": stock["option_type"],
                "quantity": 0.0,
                "avg_price": 0.0,
            }
            for stock in await current_options_positions.get_current_positions_for_strategy(
                broker.strategy
            )
        ]
    if broker.option_strategy.to_clear_before_sending:
        for stock in await broker.option_strategy.get_stocks(broker):
            # await target_option_positions.clear_positions(broker.strategy, stock.symbol)
            await target_option_positions.clear_all_positions(broker.strategy)
    await target_option_positions.create_or_update_all(target_positions)

    orders_required: List[FullOrder] = []
    quantity_differences: Dict[Tuple[str, str, float, float, OptionType], float] = {}
    for order_details in await target_option_positions.get_order_quantities_required(
        broker.option_strategy
    ):
        if order_details["quantity_difference"] == 0:
            continue
        if strategy_details["status"] == Status.inactive.value:
            # Make market order sells
            contract = Option(
                order_details["stock"],
                order_details["expiry"],
                order_details["strike"],
                order_details["option_type"],
                exchange="SMART",
                multiplier=f"{order_details['multiplier']}",
                currency="USD",
            )
            order = MarketOrder(
                "SELL" if order_details["quantity_difference"] < 0 else "BUY",
                abs(order_details["quantity_difference"]),
            )
            orders_required.append({"contract": contract, "order": order})
            continue

        quantity_differences[
            (
                order_details["stock"],
                order_details["expiry"],
                order_details["strike"],
                order_details["multiplier"],
                cast(OptionType, order_details["option_type"]),
            )
        ] = order_details["quantity_difference"]
        # if order_details['quantity_difference'] < 0:
        #     orders_required.extend(await broker.option_strategy.get_sell_order(
        #         order_details['stock'],
        #         cast(DataBroker, broker),
        #         -order_details['quantity_difference'],
        #         order_details['avg_price']
        #     ))
        # else:
        #     orders_required.extend(await broker.option_strategy.get_buy_order(
        #         order_details['stock'],
        #         cast(DataBroker, broker),
        #         order_details['quantity_difference'],
        #         order_details['quantity'],
        #         order_details['avg_price']
        #     ))
    return await broker.option_strategy.get_orders_for_quantity_difference(
        cast(DataBroker, broker), quantity_differences
    )


# ONLY USE 1 BROKER FOR EVERYTH, THANK YOU
@async_current_stock_positions_wrapper
async def validate_current_positions_for_stocks(
    current_stock_positions: AsyncCurrentStockPositionsCRUD, broker: Broker
) -> None:
    stock_current_positions = (
        await current_stock_positions.get_current_positions_overall()
    )
    broker_positions = await broker.get_current_positions()
    mismatches: Dict[str, Dict[str, int]] = {}

    for stock in broker_positions:
        local_position = stock_current_positions.get(stock, 0)
        broker_position = broker_positions[stock]
        if local_position != broker_position:
            mismatches[stock] = {"broker": broker_position, "local": local_position}
        del stock_current_positions[stock]

    for stock in stock_current_positions:
        mismatches[stock] = {"broker": 0, "local": stock_current_positions[stock]}

    if len(mismatches) > 0:
        requests.post(
            f"{RUST_BACKEND_URL}/send/positions_mismatch",
            json=mismatches,
            headers={"Authorization": f"Bearer {BEARER_TOKEN}"},
        )


async def run_and_execute_strategy(broker: Broker) -> None:
    await validate_current_positions_for_stocks(broker)
    await update_target_position_and_send_orders_for_broker(broker)
