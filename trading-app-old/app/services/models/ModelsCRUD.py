from datetime import datetime
from typing import Type, cast, List, Dict, TypedDict, Optional
from app.services.models.BaseCRUD import CRUD
from app.services.strategy.StockStrategy import StockStrategy
from app.services.strategy.OptionStrategy import OptionStrategy
from app.models import Strategy as StrategyModel, CurrentStockPositions, CurrentOptionPositions, TargetStockPositions, TargetOptionPositions, HistoricalData, StockTransactions, OptionTransactions, OpenStockOrders, OpenOptionOrders, HistoricalVolatilityData, HistoricalOptionsData, PhantomPortfolioValue
from app.models_types import (
    StrategyDict, StrategyDictPrimaryKeys, StrategyDictUpdateKeys,
    # CurrentPositionDict, CurrentPositionDictPrimaryKeys, CurrentPositionDictUpdateKeys,
    CurrentStockPositionsDict, CurrentStockPositionsDictPrimaryKeys, CurrentStockPositionsDictUpdateKeys,
    CurrentOptionPositionsDict, CurrentOptionPositionsDictPrimaryKeys, CurrentOptionPositionsDictUpdateKeys,
    # TargetPositionDict, TargetPositionDictPrimaryKeys, TargetPositionDictUpdateKeys,
    TargetStockPositionsDict, TargetStockPositionsDictPrimaryKeys, TargetStockPositionsDictUpdateKeys,
    TargetOptionPositionsDict, TargetOptionPositionsDictPrimaryKeys, TargetOptionPositionsDictUpdateKeys,

    StockTransactionsDict, StockTransactionsDictPrimaryKeys, StockTransactionsDictUpdateKeys,
    OptionTransactionsDict, OptionTransactionsDictPrimaryKeys, OptionTransactionsDictUpdateKeys,
    OpenStockOrdersDict, OpenStockOrdersDictPrimaryKeys, OpenStockOrdersDictUpdateKeys,
    OpenOptionOrdersDict, OpenOptionOrdersDictPrimaryKeys, OpenOptionOrdersDictUpdateKeys,
    # TransactionDict, TransactionDictPrimaryKeys, TransactionDictUpdateKeys,
    # OpenOrdersDict, OpenOrdersDictPrimaryKeys, OpenOrdersDictUpdateKeys,

    HistoricalDataDict, HistoricalDataDictPrimaryKeys, HistoricalDataDictUpdateKeys,
    HistoricalVolatilityDataDict, HistoricalVolatilityDataDictPrimaryKeys, HistoricalVolatilityDataDictUpdateKeys,
    # PortfolioValueDict, PortfolioValueDictPrimaryKeys, PortfolioValueDictUpdateKeys
    HistoricalOptionsDataDict, HistoricalOptionsDataDictPrimaryKeys, HistoricalOptionsDataDictUpdateKeys,
    PhantomPortfolioValueDict, PhantomPortfolioValueDictPrimaryKeys, PhantomPortfolioValueDictUpdateKeys
)

from sqlalchemy import func
from sqlalchemy.sql import and_


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


class StrategyCRUD(
    CRUD[
        StrategyModel,
        StrategyDict,
        StrategyDictUpdateKeys,
        StrategyDictPrimaryKeys,
    ]
):
    pass


class CurrentStockPositionsCRUD(
    CRUD[
        CurrentStockPositions,
        CurrentStockPositionsDict,
        CurrentStockPositionsDictUpdateKeys,
        CurrentStockPositionsDictPrimaryKeys,
    ]
):
    def get_current_positions_for_strategy(
        self, strategy: str
    ) -> List[CurrentStockPositionsDictPrimaryKeys]:
        """Only returns the stocks related to the strategy"""
        query = (
            self.session.query(
                self.model.stock,
                self.model.strategy,
            )
            .filter(self.model.strategy == strategy).all()
        )
        # return {**query}
        return [{
            'stock': stock,
            'strategy': strategy,
        }
            for (stock, strategy) in query
        ]
        # return query

    def get_current_positions_overall(self) -> Dict[str, int]:
        # Query to get the sum of positions grouped by stock
        query = (
            self.session.query(
                self.model.stock, func.sum(self.model.quantity)
            )
            .group_by(self.model.stock)
            .all()
        )
        return {
            stock: int(quantity)
            for stock, quantity in
            query
        }


class CurrentOptionPositionsCRUD(
    CRUD[
        CurrentOptionPositions,
        CurrentOptionPositionsDict,
        CurrentOptionPositionsDictUpdateKeys,
        CurrentOptionPositionsDictPrimaryKeys,
    ]
):
    def get_current_positions_for_strategy(
        self, strategy: str
    ) -> List[CurrentOptionPositionsDict]:
        query = (
            self.session.query(
                self.model.stock,
                self.model.strategy,
                self.model.expiry,
                self.model.strike,
                self.model.multiplier,
                self.model.option_type,
                self.model.avg_price,
                self.model.quantity,
            )
            .filter((self.model.strategy == strategy))
            .all()
        )

        return [{
            'stock': stock,
            'strategy': strategy,
            'expiry': expiry,
            'strike': strike,
            'multiplier': multiplier,
            'option_type': option_type,
            'avg_price': avg_price,
            'quantity': quantity
        }
            for (stock, strategy, expiry, strike, multiplier, option_type, avg_price, quantity) in query
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


class TargetStockPositionsCRUD(
    CRUD[
        TargetStockPositions,
        TargetStockPositionsDict,
        TargetStockPositionsDictUpdateKeys,
        TargetStockPositionsDictPrimaryKeys,
    ]
):
    def get_order_quantities_required(
        self, strategy: StockStrategy
    ) -> List[QuantityRequiredStock]:
        # Query to get the difference in quantities (Target - Current) for each stock & strategy
        query = (
            self.session.query(
                self.model.stock,
                self.model.strategy,
                (func.coalesce(self.model.quantity, 0) - func.coalesce(CurrentStockPositions.quantity, 0)).label("quantity_difference"),
                CurrentStockPositions.quantity,
                self.model.avg_price
            )
            .outerjoin(self.model, and_(
                CurrentStockPositions.stock == self.model.stock,
                CurrentStockPositions.strategy == self.model.strategy
            ))
            .filter(CurrentStockPositions.strategy == strategy.strategy)
            .all()
        )
        return [{
            "stock": stock,
            "strategy": strategy,
            "quantity_difference": quantity_difference,
            "quantity": quantity,
            "avg_price": avg_price
        } for stock, strategy, quantity_difference, quantity, avg_price in query]


class TargetOptionPositionsCRUD(
    CRUD[
        TargetOptionPositions,
        TargetOptionPositionsDict,
        TargetOptionPositionsDictUpdateKeys,
        TargetOptionPositionsDictPrimaryKeys,
    ]
):
    def get_order_quantities_required(
        self, strategy: OptionStrategy
    ) -> List[QuantityRequiredOption]:
        # Query to get the difference in quantities (Target - Current) for each stock & strategy
        query = (
            self.session.query(
                self.model.stock,
                self.model.strategy,
                self.model.expiry,
                self.model.strike,
                self.model.multiplier,
                self.model.option_type,
                (func.coalesce(self.model.quantity, 0) - func.coalesce(CurrentOptionPositions.quantity, 0)).label("quantity_difference"),
                CurrentOptionPositions.quantity,
                self.model.avg_price
            )
            .outerjoin(self.model, and_(
                CurrentOptionPositions.stock == self.model.stock,
                CurrentOptionPositions.strategy == self.model.strategy
            ))
            .filter(CurrentOptionPositions.strategy == strategy.strategy)
            .all()
        )
        return [{
            "stock": stock,
            "strategy": strategy,
            "expiry": expiry,
            "strike": strike,
            "multiplier": multiplier,
            "option_type": option_type,
            "quantity_difference": quantity_difference,
            "quantity": quantity,
            "avg_price": avg_price
        } for stock, strategy, expiry, strike, multiplier, option_type, quantity_difference, quantity, avg_price in query]


class OpenStockOrdersCRUD(
    CRUD[
        OpenStockOrders,
        OpenStockOrdersDict,
        OpenStockOrdersDictUpdateKeys,
        OpenStockOrdersDictPrimaryKeys,
    ]
):
    pass


class OpenOptionOrdersCRUD(
    CRUD[
        OpenOptionOrders,
        OpenOptionOrdersDict,
        OpenOptionOrdersDictUpdateKeys,
        OpenOptionOrdersDictPrimaryKeys,
    ]
):
    pass


class StockTransactionsCRUD(
    CRUD[
        StockTransactions,
        StockTransactionsDict,
        StockTransactionsDictUpdateKeys,
        StockTransactionsDictPrimaryKeys,
    ]
):
    pass


class OptionTransactionsCRUD(
    CRUD[
        OptionTransactions,
        OptionTransactionsDict,
        OptionTransactionsDictUpdateKeys,
        OptionTransactionsDictPrimaryKeys,
    ]
):
    pass


class HistoricalDataCRUD(
    CRUD[
        HistoricalData,
        HistoricalDataDict,
        HistoricalDataDictUpdateKeys,
        HistoricalDataDictPrimaryKeys,
    ]
):
    def read_stock(self, stock: str, limit: int = -1) -> List[HistoricalDataDict]:
        query = self.session.query(self.model)
        query = query.filter(self.model.stock == stock).order_by(self.model.time.desc())
        if limit > 0:
            query = query.limit(limit)
        return [self._convert_to_model_return_type(i) for i in query.all()]

    def read_stock_time(self, stock: str, time: datetime) -> List[HistoricalDataDict]:
        query = self.session.query(self.model)
        query = query.filter(
            (self.model.stock == stock)
            & (self.model.time > time)
        ).order_by(self.model.time.desc())
        return [self._convert_to_model_return_type(i) for i in query.all()]

    def read_stock_time_count(self, stock: str, time: datetime) -> int:
        count = (
            self.session
            .query(func.count())
            .select_from(self.model)
            .filter(
                (self.model.stock == stock)
                & (self.model.time > time)
            ).scalar()
        )
        return int(count)


class HistoricalVolatilityDataAnalysis(TypedDict):
    time: datetime
    open: float
    spot_open: float


class HistoricalVolatilityDataCRUD(
    CRUD[
        HistoricalVolatilityData,
        HistoricalVolatilityDataDict,
        HistoricalVolatilityDataDictUpdateKeys,
        HistoricalVolatilityDataDictPrimaryKeys,
    ]
):
    def read_for_stock_past(
        self, stock: str, time: datetime
    ) -> List[HistoricalVolatilityDataAnalysis]:
        # Original query to find data after the given time
        later_data_query = self.session.query(
            self.model.time,
            self.model.open,
            HistoricalData.open.alias('spot_open')
        ).filter(
            (self.model.stock == stock) & (self.model.time > time)
        ).leftjoin(
            (self.model.time == HistoricalData.time)
            & (self.model.stock == HistoricalData.stock)
        ).order_by(self.model.time.asc())

        later_data = later_data_query.all()

        if not later_data:
            return []

        # Find the earliest timestamp in the later data
        earliest_later_time = later_data[0].time

        # Subquery to find the 78 earlier timestamps
        earlier_data_query = (
            self.session.query(
                self.model.time, self.model.open, HistoricalData.open.label("spot_open")
            )
            .filter(
                (self.model.stock == stock) & (self.model.time < earliest_later_time)
            )
            .join(
                HistoricalData,
                and_(
                    self.model.time == HistoricalData.time,
                    self.model.stock == HistoricalData.stock,
                ),
                isouter=True,
            )
            .order_by(self.model.time.desc())
            .limit(78)
        )

        earlier_data = earlier_data_query.all()
        earlier_data.reverse()  # Restore ascending order

        # Combine and return the results
        combined_data = earlier_data + later_data
        return [
            {
                "time": cast(datetime, i.time),
                "open": cast(float, i.open),
                "spot_open": i.spot_open,
            }
            for i in combined_data
        ]

    def read_stock_time(self, stock: str, time: datetime) -> List[HistoricalVolatilityDataDict]:
        query = self.session.query(self.model)
        query = query.filter(
            (self.model.stock == stock)
            & (self.model.time > time)
        ).order_by(self.model.time.desc())
        return [self._convert_to_model_return_type(i) for i in query.all()]

    def read_stock_time_count(self, stock: str, time: datetime) -> int:
        count = (
            self.session
            .query(func.count())
            .select_from(self.model)
            .filter(
                (self.model.stock == stock)
                & (self.model.time > time)
            ).scalar()
        )
        return int(count)

# ---------- SPACE FOR DB FOR STRATEGIES ----------------


class HistoricalOptionsDataCRUD(
    CRUD[
        HistoricalOptionsData,
        HistoricalOptionsDataDict,
        HistoricalOptionsDataDictUpdateKeys,
        HistoricalOptionsDataDictPrimaryKeys,
    ]
):
    def read_for_stock_past(
        self, stock: str, time: datetime
    ) -> List[HistoricalOptionsDataDict]:
        query = self.session.query(self.model)
        query = query.filter(
            (self.model.stock == stock)
            & (self.model.time > time)
        ).order_by(self.model.time.asc())
        return [self._convert_to_model_return_type(i) for i in query.all()]


class PhantomPortfolioValueCRUD(
    CRUD[
        PhantomPortfolioValue,
        PhantomPortfolioValueDict,
        PhantomPortfolioValueDictUpdateKeys,
        PhantomPortfolioValueDictPrimaryKeys,
    ]
):
    def get_last_entry(self) -> List[PhantomPortfolioValueDict]:
        query = self.session.query(self.model)
        query = query.filter().order_by(self.model.time.desc()).limit(10)
        query_res = [i for i in query.all()]
        # if len(query_res) == 0:
        #     return None
        # return self._convert_to_model_return_type(cast(Type[PhantomPortfolioValue], query_res[0]))

        return [self._convert_to_model_return_type(i) for i in query_res]
