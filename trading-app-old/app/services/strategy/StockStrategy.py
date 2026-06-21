from abc import abstractmethod, ABC
from typing import Dict, List, Tuple, Any

from ib_async.contract import Contract
from app.services.broker.DataBroker import DataBroker, FullOrder

from app.models_types import TargetStockPositionsDict
from ib_async.order import Trade
from ib_async.objects import Fill


class StockStrategy(ABC):
    strategy: str = "Strategy Base Class"
    to_clear_before_sending: bool = False

    @staticmethod
    @abstractmethod
    async def create_strategy() -> None:
        pass

    @staticmethod
    @abstractmethod
    async def get_weights(broker: DataBroker) -> List[TargetStockPositionsDict]:
        pass

    @staticmethod
    @abstractmethod
    def get_buy_price(current_prices: Dict[str, float]) -> Dict[str, float]:
        pass

    @staticmethod
    @abstractmethod
    def get_sell_price(current_prices: Dict[str, float]) -> Dict[str, float]:
        pass

    @staticmethod
    @abstractmethod
    # def get_buy_order(current_prices: Optional[Dict[str, float]], quantity: int) -> List[LocalOrder]:
    async def get_buy_order(
        stock: str,
        broker: DataBroker,
        quantity: int,
        quantity_to_insure: int,
        avg_price: float,
    ) -> List[FullOrder]:
        pass

    @staticmethod
    @abstractmethod
    # def get_sell_order(current_prices: Optional[Dict[str, float]], quantity: int) -> List[LocalOrder]:
    async def get_sell_order(
        stock: str, broker: DataBroker, quantity: int, avg_price: float
    ) -> List[FullOrder]:
        pass

    @staticmethod
    @abstractmethod
    # def get_stocks() -> List[Tuple[str, str, str]]:
    async def get_stocks(broker: DataBroker) -> List[Contract]:
        pass

    @staticmethod
    @abstractmethod
    def execDetailsEvent(trade: Trade, fill: Fill) -> None:
        pass

    @staticmethod
    @abstractmethod
    async def update_historical_data_to_present(broker: DataBroker) -> None:
        pass
