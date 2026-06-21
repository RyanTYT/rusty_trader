from _typeshed import Incomplete
from stockdex.config import VALID_SECURITY_TYPES as VALID_SECURITY_TYPES
import pandas as pd


class Ticker():
    ticker: Incomplete
    isin: Incomplete
    security_type: Incomplete
    def __init__(self, ticker: str = '', isin: str = '', security_type: VALID_SECURITY_TYPES = 'stock') -> None: ...
    def yahoo_api_price(self, range: str, dataGranularity: str) -> pd.DataFrame: ...
