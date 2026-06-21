from abc import ABC, abstractmethod
from typing import TypedDict, Dict, Tuple, Set, List
from ib_async import IB
from ib_async.contract import Contract
from ib_async.order import Order
from ib_async.ticker import Ticker
from datetime import datetime


class FullOrder(TypedDict):
    contract: Contract
    order: Order


class DataBroker(ABC):
    ib: IB
    live_options_data: Dict[Contract, List[Tuple[datetime, float]]]

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

    # @abstractmethod
    # async def get_current_option_price(
    #     self,
    #     # stock: str,
    #     # expiry: str,
    #     # strike: float,
    #     # multiplier: str,
    #     # option_type: str,
    #     # exchange: str = 'SMART',
    #     # currency: str = 'USD'
    #     option: Contract,
    # ) -> float:
    #     pass

    @abstractmethod
    async def _qualify_contracts_async(self, *contracts: Contract) -> None:
        """
        Runs qualifyContracts in a separate thread to prevent blocking.
        """
        pass

    @abstractmethod
    async def update_historical_data_till_today(self) -> None:
        """
        Updates the historical data of all required stocks up till today
        (based on the update data functions of StockStrategy and OptionStrategy)
        """
        pass
