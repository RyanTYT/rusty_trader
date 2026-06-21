from app.services.models.AsyncModelsCRUD import (
    AsyncCurrentStockPositionsCRUD,
    AsyncHistoricalDataCRUD,
    AsyncStrategyCRUD,
)
from app.services.strategy.StockStrategy import StockStrategy as StrategyClass
from app.utils.custom_logging import CustomLogger
from app.utils.db import (
    async_with_db_session_for_model,
)
from app.models import (
    CurrentStockPositions,
    HistoricalData,
    Strategy,
)
from app.models_types import (
    Status,
    TargetStockPositionsDict,
)
from typing import List, TypedDict
from app.services.broker.DataBroker import DataBroker, FullOrder
from ib_async.contract import Contract, Stock
from ib_async.order import MarketOrder, StopOrder
from datetime import date, datetime, timedelta, timezone
import pytz
import pandas_market_calendars as mcal
from lxml import html
import aiohttp
import re
import ssl
import certifi

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


class TRA(StrategyClass):
    strategy = "tra"
    eastern = pytz.timezone("US/Eastern")
    calendar = mcal.get_calendar("NYSE")
    initial_equity_weight = 0.60
    next_pri_rls_date: date | None = None

    @async_strategy_wrapper
    @staticmethod
    async def create_strategy(strategy: AsyncStrategyCRUD) -> None:
        strategy_exists = await strategy.read({"strategy": TRA.strategy})
        if len(strategy_exists) > 0:
            return

        await strategy.create(
            {
                "strategy": TRA.strategy,
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
        if TRA.next_pri_rls_date is None:
            await TRA.update_historical_data_to_present(broker)

        strategy_row = await strategy.read({"strategy": TRA.strategy})
        assert len(strategy_row) > 0
        capital = strategy_row[0]["capital"]

        time_now = datetime.now(timezone.utc).astimezone(TRA.eastern)
        if time_now.date() == TRA.next_pri_rls_date and not (
            time_now.hour >= 15 and time_now.minute >= 40
        ):
            price_now = await broker.get_current_price(Stock("TLH", "SMART", "USD"))
            return [
                {
                    "stock": "TLH",
                    "strategy": TRA.strategy,
                    "stop_limit": price_now * 0.8,
                    "avg_price": price_now,  # ANYHOW price, cos order is j mkt order
                    "quantity": int(capital / price_now),
                }
            ]
        return [
            {
                "stock": "TLH",
                "strategy": TRA.strategy,
                "stop_limit": 0,
                "avg_price": 0,
                "quantity": 0,
            }
        ]

    @staticmethod
    async def get_buy_order(
        stock: str,
        broker: DataBroker,
        quantity: int,
        quantity_to_insure: int,
        avg_price: float,
    ) -> List[FullOrder]:
        return [
            {
                "contract": Stock("TLH", "SMART", "USD"),
                "order": MarketOrder("BUY", quantity),
            }
        ]

    @staticmethod
    async def get_sell_order(
        stock: str, broker: DataBroker, quantity: int, avg_price: float
    ) -> List[FullOrder]:
        return [
            {
                "contract": Stock("TLH", "SMART", "USD"),
                "order": MarketOrder("SELL", quantity),
            }
        ]

    @staticmethod
    async def get_stocks(broker: DataBroker) -> List[Contract]:
        contracts = [Stock("TLH", "SMART", "USD")]
        qualified_contracts = await broker.ib.qualifyContractsAsync(*contracts)
        return qualified_contracts

    @staticmethod
    async def update_historical_data_to_present(
        broker: DataBroker,
    ) -> None:
        # Update possible options related to stock
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        url = "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding/most-recent-quarterly-refunding-documents"

        async with aiohttp.ClientSession() as session:
            async with session.get(url, ssl=ssl_context) as response:
                content = await response.read()
                tree = html.fromstring(content)

                h3_arr = tree.xpath(
                    "//h3[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'documents released at')]"
                )
                assert type(h3_arr) is list and len(h3_arr) > 0
                h3 = h3_arr[0]
                next_rls_arr = h3.getparent().xpath(  # type: ignore
                    ".//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'the next release is scheduled for')]"
                )
                assert type(next_rls_arr) is list and len(next_rls_arr) > 0
                next_rls = next_rls_arr[0]
                match = re.search(
                    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b",
                    next_rls.text_content(),  # type: ignore
                )
                assert match is not None

                date_str = match.group(0)
                date = datetime.strptime(date_str, "%B %d, %Y")

                days = TRA.calendar.valid_days(date - timedelta(days=10), date)
                TRA.next_pri_rls_date = [
                    day for day in days if day.date() < date.date()
                ][-1]
