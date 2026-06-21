from abc import abstractmethod
from typing import List, Dict, Optional
from app.services.broker.DataBroker import DataBroker, FullOrder
from app.services.strategy.StockStrategy import StockStrategy
from app.services.strategy.OptionStrategy import OptionStrategy
from ib_async.contract import Contract


class Broker(DataBroker):
    strategy: str
    stock_strategy: Optional[StockStrategy]
    option_strategy: Optional[OptionStrategy]

    @abstractmethod
    async def connect_to_broker(self) -> None:
        """
        Connect to the TWS API via ib.connectAsync()
        """
        pass

    @abstractmethod
    def disconnect_from_broker(self) -> None:
        """
        Directly disconnects to the TWS API via ib.disconnect()
        """
        pass

    @abstractmethod
    def sleep(self, seconds: int) -> None:
        """
        A sleep function based on specific broker (i.e. ib.sleep())
        """
        pass

    @abstractmethod
    async def get_current_price(
        self,
        # stock: str, exchange: str = "SMART", currency: str = "USD"
        contract: Contract,
        vwap: bool = False,
    ) -> float:
        """
        Retrieves the current price of the stock

        Returns:
            current price of the stock
                - if vwap == True: returns vwap price
                - if vwap == False: returns marketPrice(), else last close
        """
        pass

    @abstractmethod
    async def get_current_positions(self) -> Dict[str, int]:
        """
        Retrieves the current positions held by the broker.

        Returns:
            A dictionary where the keys are asset symbols (e.g., "AAPL") and the values are the number of shares held.
        """
        pass

    @abstractmethod
    def cancel_all_open_orders(self) -> None:
        """
        Cancels all open orders.

        Returns:
            None
        """
        pass

    @abstractmethod
    async def send_orders(self, orders: List[FullOrder]) -> None:
        """
        Send a list of orders to the broker.

        Args:
            orders: A list of dictionaries, where each dictionary represents an order.

        Returns:
            None
        """
        pass

    @abstractmethod
    async def update_completed_orders(self) -> bool:
        """
        Updates the Transaction and CurrentPosition DB for the completed orders since last session.

        Checks if CurrentPosition matches that from API
        """
        pass

    @abstractmethod
    async def run_live_strategies(self) -> None:
        """
        'Begins' the strategies attached by listening for price updates directly
        """
        pass

    @abstractmethod
    async def check_live_subs(self) -> None:
        """
        Checks the live subscriptions to the data lines to check if they still work, else refresh the data lines
        """
        pass
