from datetime import datetime, date, time, timedelta
import numpy as np
from typing import Type, cast, List, Dict, TypedDict, Optional
import pytz
from sqlalchemy import text, Numeric
from sqlalchemy.orm import aliased
from app.services.models.AsyncBaseCRUD import AsyncCRUD
from app.services.strategy.StockStrategy import StockStrategy
from app.services.strategy.OptionStrategy import OptionStrategy
from app.models import (
    Strategy as StrategyModel,
    CurrentStockPositions,
    CurrentOptionPositions,
    TargetStockPositions,
    TargetOptionPositions,
    HistoricalData,
    StockTransactions,
    OptionTransactions,
    OpenStockOrders,
    OpenOptionOrders,
    HistoricalVolatilityData,
    HistoricalOptionsData,
    PhantomPortfolioValue,
)
from app.models_types import (
    OptionType,
    StrategyDict,
    StrategyDictPrimaryKeys,
    StrategyDictUpdateKeys,
    # CurrentPositionDict, CurrentPositionDictPrimaryKeys, CurrentPositionDictUpdateKeys,
    CurrentStockPositionsDict,
    CurrentStockPositionsDictPrimaryKeys,
    CurrentStockPositionsDictUpdateKeys,
    CurrentOptionPositionsDict,
    CurrentOptionPositionsDictPrimaryKeys,
    CurrentOptionPositionsDictUpdateKeys,
    # TargetPositionDict, TargetPositionDictPrimaryKeys, TargetPositionDictUpdateKeys,
    TargetStockPositionsDict,
    TargetStockPositionsDictPrimaryKeys,
    TargetStockPositionsDictUpdateKeys,
    TargetOptionPositionsDict,
    TargetOptionPositionsDictPrimaryKeys,
    TargetOptionPositionsDictUpdateKeys,
    StockTransactionsDict,
    StockTransactionsDictPrimaryKeys,
    StockTransactionsDictUpdateKeys,
    OptionTransactionsDict,
    OptionTransactionsDictPrimaryKeys,
    OptionTransactionsDictUpdateKeys,
    OpenStockOrdersDict,
    OpenStockOrdersDictPrimaryKeys,
    OpenStockOrdersDictUpdateKeys,
    OpenOptionOrdersDict,
    OpenOptionOrdersDictPrimaryKeys,
    OpenOptionOrdersDictUpdateKeys,
    # TransactionDict, TransactionDictPrimaryKeys, TransactionDictUpdateKeys,
    # OpenOrdersDict, OpenOrdersDictPrimaryKeys, OpenOrdersDictUpdateKeys,
    HistoricalDataDict,
    HistoricalDataDictPrimaryKeys,
    HistoricalDataDictUpdateKeys,
    HistoricalVolatilityDataDict,
    HistoricalVolatilityDataDictPrimaryKeys,
    HistoricalVolatilityDataDictUpdateKeys,
    # PortfolioValueDict, PortfolioValueDictPrimaryKeys, PortfolioValueDictUpdateKeys
    HistoricalOptionsDataDict,
    HistoricalOptionsDataDictPrimaryKeys,
    HistoricalOptionsDataDictUpdateKeys,
    PhantomPortfolioValueDict,
    PhantomPortfolioValueDictPrimaryKeys,
    PhantomPortfolioValueDictUpdateKeys,
)

from sqlalchemy import asc, desc, func, outerjoin, delete, text
from sqlalchemy.sql import and_, extract
from sqlalchemy.future import select

import pandas_market_calendars as mcal

from app.utils.custom_logging import CustomLogger


class QuantityRequiredStock(TypedDict):
    stock: str
    strategy: str
    quantity_difference: int
    quantity: int
    avg_price: float


class QuantityRequiredOption(TypedDict):
    stock: str
    strategy: str
    expiry: str
    strike: float
    multiplier: float
    option_type: str  # or OptionType
    quantity_difference: int
    quantity: int
    avg_price: float


class AsyncStrategyCRUD(
    AsyncCRUD[
        StrategyModel,
        StrategyDict,
        StrategyDictUpdateKeys,
        StrategyDictPrimaryKeys,
    ]
):
    pass


class AsyncCurrentStockPositionsCRUD(
    AsyncCRUD[
        CurrentStockPositions,
        CurrentStockPositionsDict,
        CurrentStockPositionsDictUpdateKeys,
        CurrentStockPositionsDictPrimaryKeys,
    ]
):
    async def get_current_positions_for_strategy(
        self, strategy: str
    ) -> List[CurrentStockPositionsDictPrimaryKeys]:
        """Returns only the stocks related to the specified strategy."""
        stmt = select(self.model.stock, self.model.strategy).where(
            self.model.strategy == strategy
        )
        result = await self.session.execute(stmt)
        rows = result.all()
        return [{"stock": stock, "strategy": strategy} for stock, strategy in rows]

    async def get_current_positions_overall(self) -> Dict[str, int]:
        """Returns the total quantity of positions grouped by stock."""
        stmt = select(self.model.stock, func.sum(self.model.quantity)).group_by(
            self.model.stock
        )
        result = await self.session.execute(stmt)
        rows = result.all()
        return {stock: int(quantity) for stock, quantity in rows}


class AsyncCurrentOptionPositionsCRUD(
    AsyncCRUD[
        CurrentOptionPositions,
        CurrentOptionPositionsDict,
        CurrentOptionPositionsDictUpdateKeys,
        CurrentOptionPositionsDictPrimaryKeys,
    ]
):
    async def get_current_positions_for_stock(
        self, stock: str
    ) -> List[CurrentOptionPositionsDict]:
        stmt = select(self.model).where(self.model.stock == stock)

        result = await self.session.execute(stmt)
        rows = result.scalars().all()

        return [self._convert_to_model_return_type(i) for i in rows]

    async def get_current_positions_for_strategy(
        self, strategy: str
    ) -> List[CurrentOptionPositionsDict]:
        stmt = select(
            self.model.stock,
            self.model.strategy,
            self.model.expiry,
            self.model.strike,
            self.model.multiplier,
            self.model.option_type,
            self.model.avg_price,
            self.model.quantity,
        ).where(self.model.strategy == strategy)

        result = await self.session.execute(stmt)
        rows = result.all()

        return [
            {
                "stock": stock,
                "strategy": strategy,
                "expiry": expiry,
                "strike": strike,
                "multiplier": multiplier,
                "option_type": option_type,
                "avg_price": avg_price,
                "quantity": quantity,
            }
            for (
                stock,
                strategy,
                expiry,
                strike,
                multiplier,
                option_type,
                avg_price,
                quantity,
            ) in rows
        ]

    # def get_current_positions_overall(self) -> Dict[str, int]:
    #     # Query to get the sum of positions grouped by stock
    #     query = (
    #         self.session.query(
    #             self.model.stock, func.sum(self.model.quantity)
    #         )
    #         .group_by(self.model.stock)
    #         .all()
    #     )
    #     return {
    #         stock: int(quantity)
    #         for stock, quantity in
    #         query
    #     }


class AsyncTargetStockPositionsCRUD(
    AsyncCRUD[
        TargetStockPositions,
        TargetStockPositionsDict,
        TargetStockPositionsDictUpdateKeys,
        TargetStockPositionsDictPrimaryKeys,
    ]
):
    async def clear_positions(self, strategy: str, stock: str) -> None:
        stmt = delete(self.model).where(
            self.model.strategy == strategy, self.model.stock == stock
        )
        await self.session.execute(stmt)
        await self.session.commit()

    async def get_order_quantities_required(
        self, strategy: StockStrategy
    ) -> List[QuantityRequiredStock]:
        curr = aliased(CurrentStockPositions)

        stmt = (
            select(
                self.model.stock,
                self.model.strategy,
                (
                    func.coalesce(self.model.quantity, 0)
                    - func.coalesce(curr.quantity, 0)
                ).label("quantity_difference"),
                curr.quantity,
                self.model.avg_price,
            )
            .select_from(
                outerjoin(
                    self.model,
                    curr,
                    and_(
                        curr.stock == self.model.stock,
                        curr.strategy == self.model.strategy,
                    ),
                )
            )
            .where(self.model.strategy == strategy.strategy)
        )

        result = await self.session.execute(stmt)
        rows = result.all()

        return [
            {
                "stock": stock,
                "strategy": strategy.strategy,
                "quantity_difference": int(quantity_difference),
                "quantity": int(quantity) if quantity is not None else 0,
                "avg_price": avg_price,
            }
            for stock, _, quantity_difference, quantity, avg_price in rows
        ]


class AsyncTargetOptionPositionsCRUD(
    AsyncCRUD[
        TargetOptionPositions,
        TargetOptionPositionsDict,
        TargetOptionPositionsDictUpdateKeys,
        TargetOptionPositionsDictPrimaryKeys,
    ]
):
    async def clear_positions(self, strategy: str, stock: str) -> None:
        stmt = delete(self.model).where(
            self.model.strategy == strategy, self.model.stock == stock
        )
        await self.session.execute(stmt)
        await self.session.commit()

    async def clear_all_positions(self, strategy: str) -> None:
        stmt = delete(self.model).where(self.model.strategy == strategy)
        await self.session.execute(stmt)
        await self.session.commit()

    async def get_order_quantities_required(
        self, strategy: OptionStrategy
    ) -> List[QuantityRequiredOption]:
        curr = aliased(CurrentOptionPositions)

        stmt = (
            select(
                self.model.stock,
                self.model.strategy,
                self.model.expiry,
                self.model.strike,
                self.model.multiplier,
                self.model.option_type,
                (
                    func.coalesce(self.model.quantity, 0)
                    - func.coalesce(curr.quantity, 0)
                ).label("quantity_difference"),
                curr.quantity,
                self.model.avg_price,
            )
            .select_from(
                outerjoin(
                    self.model,
                    curr,
                    and_(
                        curr.stock == self.model.stock,
                        curr.strategy == self.model.strategy,
                        curr.expiry == self.model.expiry,
                        curr.strike == self.model.strike,
                        curr.option_type == self.model.option_type,
                    ),
                )
            )
            .where(self.model.strategy == strategy.strategy)
        )

        result = await self.session.execute(stmt)
        rows = result.all()

        return [
            {
                "stock": stock,
                "strategy": strategy.strategy,
                "expiry": expiry,
                "strike": strike,
                "multiplier": multiplier,
                "option_type": option_type,
                "quantity_difference": int(quantity_difference),
                "quantity": int(quantity) if quantity is not None else 0,
                "avg_price": avg_price,
            }
            for (
                stock,
                _,
                expiry,
                strike,
                multiplier,
                option_type,
                quantity_difference,
                quantity,
                avg_price,
            ) in rows
        ]


class AsyncOpenStockOrdersCRUD(
    AsyncCRUD[
        OpenStockOrders,
        OpenStockOrdersDict,
        OpenStockOrdersDictUpdateKeys,
        OpenStockOrdersDictPrimaryKeys,
    ]
):
    pass


class AsyncOpenOptionOrdersCRUD(
    AsyncCRUD[
        OpenOptionOrders,
        OpenOptionOrdersDict,
        OpenOptionOrdersDictUpdateKeys,
        OpenOptionOrdersDictPrimaryKeys,
    ]
):
    pass


class AsyncStockTransactionsCRUD(
    AsyncCRUD[
        StockTransactions,
        StockTransactionsDict,
        StockTransactionsDictUpdateKeys,
        StockTransactionsDictPrimaryKeys,
    ]
):
    pass


class AsyncOptionTransactionsCRUD(
    AsyncCRUD[
        OptionTransactions,
        OptionTransactionsDict,
        OptionTransactionsDictUpdateKeys,
        OptionTransactionsDictPrimaryKeys,
    ]
):
    async def read_stock_day(
        self, stock: str, day: datetime
    ) -> List[OptionTransactionsDict]:
        stmt = (
            select(self.model)
            .where(self.model.stock == stock)
            .where(
                extract("year", self.model.time) == day.year,
                extract("month", self.model.time) == day.month,
                extract("day", self.model.time) == day.day,
            )
        )
        rows = await self.session.execute(stmt)
        return [self._convert_to_model_return_type(i) for i in rows.scalars().all()]


class AsyncHistoricalDataCRUD(
    AsyncCRUD[
        HistoricalData,
        HistoricalDataDict,
        HistoricalDataDictUpdateKeys,
        HistoricalDataDictPrimaryKeys,
    ]
):
    async def read_stock(self, stock: str, limit: int = -1) -> List[HistoricalDataDict]:
        stmt = (
            select(self.model)
            .where(self.model.stock == stock)
            .order_by(desc(self.model.time))
        )
        if limit > 0:
            stmt = stmt.limit(limit)
        result = await self.session.execute(stmt)
        rows = result.scalars().all()
        return [self._convert_to_model_return_type(i) for i in rows]

    async def has_at_least_n_rows(self, stock: str, n: int) -> bool:
        if n <= 0:
            return True  # trivially true

        stmt = (
            select(self.model.stock)
            .where(self.model.stock == stock)
            .order_by(desc(self.model.time))
            .limit(n)
        )
        result = await self.session.execute(stmt)
        rows = result.scalars().all()
        return len(rows) == n

    async def read_stock_time(
        self, stock: str, time: datetime
    ) -> List[HistoricalDataDict]:
        stmt = (
            select(self.model)
            .where((self.model.stock == stock) & (self.model.time > time))
            .order_by(desc(self.model.time))
        )
        result = await self.session.execute(stmt)
        return [self._convert_to_model_return_type(i) for i in result.scalars().all()]

    async def read_stock_time_count(self, stock: str, time: datetime) -> int:
        stmt = (
            select(func.count())
            .select_from(self.model)
            .where((self.model.stock == stock) & (self.model.time > time))
        )
        result = await self.session.execute(stmt)
        return int(result.scalar_one())

    async def avg_move_since_open(self, stock: str) -> Optional[float]:
        query = text("""
        WITH latest_bar_time AS (
            SELECT
                time::time AS latest_close
            FROM
                market_data.historical_data
            WHERE
                stock = :stock
            ORDER BY
                time DESC
            LIMIT 1
        ),
        historical_matches AS (
            SELECT
                h.stock,
                h.time::date AS trading_day,
                h.time,
                h.close
            FROM market_data.historical_data h
            JOIN latest_bar_time lb ON h.time::time = lb.latest_close
            WHERE h.stock = :stock
            ORDER BY h.time DESC
            LIMIT 15
        ),
        opens AS (
            SELECT stock, day AS trading_day, open AS open_at_0930
            FROM market_data.daily_ohlcv
            WHERE stock = :stock
        )
        SELECT
            hm.close / o.open_at_0930 AS movement_since_open
        FROM historical_matches hm
        JOIN opens o ON hm.stock = o.stock AND hm.trading_day = o.trading_day
        ORDER BY hm.time DESC;
        """)

        result = await self.session.execute(query, {"stock": stock})
        rows = result.mappings().all()

        if rows is None:
            return None  # No data

        return float(np.mean([abs(row["movement_since_open"] - 1.0) for row in rows]))

    async def get_last_max_open(self, stock: str) -> float:
        query = text("""
            SELECT day, open, close
            FROM market_data.daily_ohlcv
            WHERE stock = :stock
            ORDER BY day DESC
            LIMIT 2;
        """)

        result = await self.session.execute(query, {"stock": stock})
        rows = result.mappings().all()
        assert len(rows) == 2
        yest_close = rows[1]["close"]
        assert type(yest_close) is float

        eastern = pytz.timezone("US/Eastern")
        start = eastern.localize(datetime.combine(date.today(), time(9, 30, 0)))
        end = eastern.localize(datetime.combine(date.today(), time(9, 30, 59)))
        stmt = (
            select(self.model.open)
            .where(
                self.model.time >= start,
                self.model.time <= end,
                self.model.stock == stock,
            )
            .order_by(desc(self.model.time))
            .limit(1)
        )
        result = await self.session.execute(stmt)
        morn_open = result.scalar_one_or_none()
        if morn_open is None:
            return yest_close
        assert type(morn_open) is float

        return max(yest_close, morn_open)

    async def get_daily_vol(self, stock: str) -> float:
        query = text("""
            SELECT day, rolling_volatility
            FROM market_data.daily_volatility
            WHERE stock = :stock
            ORDER BY day DESC
            LIMIT 1;
        """)

        result = await self.session.execute(query, {"stock": stock})
        row = result.mappings().first()

        if row:
            return float(row["rolling_volatility"])

        self.logger.error("Not enough data to calculate daily volatility data")
        return 10000.0

    async def has_minimum_daily_ohlcv(self, stock: str, min_days: int = 30) -> bool:
        # Get most recent trading day before today
        nyse = mcal.get_calendar("NYSE")
        valid_days = nyse.valid_days(
            start_date=datetime.today() - timedelta(days=10), end_date=datetime.today()
        )
        valid_days_filtered = [i for i in valid_days if i != datetime.today().date()]
        most_recent_day = valid_days_filtered[-1]

        query = text("""
            SELECT
                COUNT(*) >= :min_days AS has_enough_data,
                MAX(day) = :expected_day AS has_latest_day
            FROM market_data.daily_ohlcv
            WHERE stock = :stock;
        """)

        result = await self.session.execute(
            query,
            {"stock": stock, "min_days": min_days, "expected_day": most_recent_day},
        )

        row = result.mappings().first()
        assert row
        return bool(row["has_enough_data"] and row["has_latest_day"])

    async def refresh_daily_data(self, days_back: int = 30) -> None:
        query = text(f"""
            CALL refresh_continuous_aggregate(
                'market_data.daily_ohlcv',
                NOW() - INTERVAL '{days_back} days',
                NOW()
            );
        """)
        # Execute the query using a raw connection from the engine
        # This bypasses the AsyncSession's transaction management
        async with self.engine.execution_options(
            isolation_level="AUTOCOMMIT"
        ).connect() as conn:
            await conn.execute(query)
            await conn.commit()  # Explicitly commit the CALL operation
        # query = text(f"""
        #     CALL refresh_continuous_aggregate(
        #       'market_data.daily_volatility',
        #       NOW() - INTERVAL '{days_back} days',
        #       NOW();
        #   """)
        # await self.session.execute(query)


class HistoricalVolatilityDataAnalysis(TypedDict):
    time: datetime
    open: float
    spot_open: float


class AsyncHistoricalVolatilityDataCRUD(
    AsyncCRUD[
        HistoricalVolatilityData,
        HistoricalVolatilityDataDict,
        HistoricalVolatilityDataDictUpdateKeys,
        HistoricalVolatilityDataDictPrimaryKeys,
    ]
):
    async def read_for_stock_past(
        self, stock: str, time: datetime
    ) -> List[HistoricalVolatilityDataAnalysis]:
        joined_stmt = (
            select(
                self.model.time,
                self.model.open,
                HistoricalData.open.label("spot_open"),
            )
            .join(
                HistoricalData,
                and_(
                    self.model.time == HistoricalData.time,
                    self.model.stock == HistoricalData.stock,
                ),
            )
            .where(self.model.stock == stock, self.model.time > time)
            .order_by(asc(self.model.time))
        )
        result = await self.session.execute(joined_stmt)
        later_data = result.all()

        if not later_data:
            return []

        earliest_time = later_data[0].time
        earlier_stmt = (
            select(
                self.model.time,
                self.model.open,
                HistoricalData.open.label("spot_open"),
            )
            .join(
                HistoricalData,
                and_(
                    self.model.time == HistoricalData.time,
                    self.model.stock == HistoricalData.stock,
                ),
            )
            .where((self.model.stock == stock) & (self.model.time < earliest_time))
            .order_by(desc(self.model.time))
            .limit(78)
        )
        earlier_result = await self.session.execute(earlier_stmt)
        earlier_data = earlier_result.all()

        earlier_data_actual: List[HistoricalVolatilityDataAnalysis] = [
            {
                "time": i[0],
                "open": i[1],
                "spot_open": i[2],
            }
            for i in earlier_data
        ]
        earlier_data_actual.reverse()
        later_data_actual: List[HistoricalVolatilityDataAnalysis] = [
            {"time": t, "open": o, "spot_open": so} for t, o, so in later_data
        ]
        return earlier_data_actual + later_data_actual

    async def read_stock_time(
        self, stock: str, time: datetime
    ) -> List[HistoricalVolatilityDataDict]:
        stmt = (
            select(self.model)
            .where((self.model.stock == stock) & (self.model.time > time))
            .order_by(desc(self.model.time))
        )
        result = await self.session.execute(stmt)
        return [self._convert_to_model_return_type(i) for i in result.scalars().all()]

    async def read_stock_time_count(self, stock: str, time: datetime) -> int:
        stmt = (
            select(func.count())
            .select_from(self.model)
            .where((self.model.stock == stock) & (self.model.time > time))
        )
        result = await self.session.execute(stmt)
        return int(result.scalar_one())


# ---------- SPACE FOR DB FOR STRATEGIES ----------------


class AsyncHistoricalOptionsDataCRUD(
    AsyncCRUD[
        HistoricalOptionsData,
        HistoricalOptionsDataDict,
        HistoricalOptionsDataDictUpdateKeys,
        HistoricalOptionsDataDictPrimaryKeys,
    ]
):
    async def read_stock(
        self,
        stock: str,
        expiry: str,
        strike: float,
        multiplier: float,
        option_type: OptionType,
        limit: int = -1,
    ) -> List[HistoricalOptionsDataDict]:
        stmt = (
            select(self.model)
            .where(
                (self.model.stock == stock)
                & (self.model.expiry == expiry)
                & (self.model.strike == strike)
                & (self.model.multiplier == multiplier)
                & (self.model.option_type == option_type)
            )
            .order_by(desc(self.model.time))
            .limit(limit)
        )
        result = await self.session.execute(stmt)
        return [self._convert_to_model_return_type(i) for i in result.scalars().all()]

    async def read_for_stock_past(
        self, stock: str, time: datetime
    ) -> List[HistoricalOptionsDataDict]:
        stmt = (
            select(self.model)
            .where((self.model.stock == stock) & (self.model.time > time))
            .order_by(asc(self.model.time))
        )
        result = await self.session.execute(stmt)
        return [self._convert_to_model_return_type(i) for i in result.scalars().all()]


class AsyncPhantomPortfolioValueCRUD(
    AsyncCRUD[
        PhantomPortfolioValue,
        PhantomPortfolioValueDict,
        PhantomPortfolioValueDictUpdateKeys,
        PhantomPortfolioValueDictPrimaryKeys,
    ]
):
    async def get_last_entry(self) -> List[PhantomPortfolioValueDict]:
        stmt = select(self.model).order_by(desc(self.model.time)).limit(10)
        result = await self.session.execute(stmt)
        return [self._convert_to_model_return_type(i) for i in result.scalars().all()]

    async def get_actual_last_entry(self) -> PhantomPortfolioValueDict:
        stmt = select(self.model).order_by(desc(self.model.time))
        result = await self.session.execute(stmt)
        return self._convert_to_model_return_type(result.scalar_one())
