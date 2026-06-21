import os
from typing import List, Tuple, cast
from datetime import datetime

# IBKR Automation
from app.services.broker.Broker import Broker
from app.services.broker.IBKR import IBKR

from app.services.strategy.StockStrategy import StockStrategy

# from app.services.strategy.EMA import EMA
from app.services.strategy.EarningsStraddle import EarningsStraddle
from app.services.strategy.Noise import Noise
from app.services.strategy.OptionStrategy import OptionStrategy
from app.services.strategy.TRA import TRA
from app.services.strategy.ThresholdRebalancingFrontRunner import (
    ThresholdRebalancingFrontRunner,
)
from app.services.strategy.CalendarRebalancingFrontRunner import (
    CalendarRebalancingFrontRunner,
)

# from app.services.strategy.ShortStraddle import ShortStraddle
from app.services.IBC import start_ibkr
from app.models_types_generator import generate_models_types
# from app.tasks.execution_tasks import validate_current_positions_for_stocks

# DB
from app.utils.db import init_db


async def init_app() -> Tuple[List[Broker], List[Broker], List[Broker]]:
    generate_models_types("app/models.py")

    date_str = datetime.now().strftime("%Y-%m-%d")
    init_db(f"logs/sqlalchemy/app_{date_str}.log")
    await start_ibkr()

    host = os.environ["HOST"]
    # broker = IBKR(host=host, port=4002, client_id=1, account="U6003401", stock_strategy=cast(StockStrategy, EMA))
    earnings_broker = IBKR(
        host=host,
        port=4002,
        client_id=1,
        account="U6003401",
        option_strategy=cast(OptionStrategy, EarningsStraddle),
    )
    noise_broker = IBKR(
        host=host,
        port=4002,
        client_id=2,
        account="U6003401",
        stock_strategy=cast(StockStrategy, Noise),
    )
    threshold_broker = IBKR(
        host=host,
        port=4002,
        client_id=3,
        account="U6003401",
        stock_strategy=cast(StockStrategy, ThresholdRebalancingFrontRunner),
    )
    calendar_broker = IBKR(
        host=host,
        port=4002,
        client_id=4,
        account="U6003401",
        stock_strategy=cast(StockStrategy, CalendarRebalancingFrontRunner),
    )
    tra_broker = IBKR(
        host=host,
        port=4002,
        client_id=5,
        account="U6003401",
        stock_strategy=cast(StockStrategy, TRA),
    )
    # await validate_current_positions_for_stocks(broker)

    daily_strats: List[Broker] = []
    one_hour_strats: List[Broker] = [threshold_broker, calendar_broker]
    five_min_strats: List[Broker] = [earnings_broker, noise_broker, tra_broker]
    return daily_strats, one_hour_strats, five_min_strats
