from datetime import datetime
from sqlalchemy import (
    String,
    Float,
    Integer,
    BigInteger,
    TIMESTAMP,
    Index,
    ForeignKey,
    desc,
    Boolean,
)
from sqlalchemy.orm import declarative_base, Mapped, mapped_column, relationship
from enum import Enum, auto
from sqlalchemy import Enum as PgEnum

Base = declarative_base()


class Status(str, Enum):
    active = "active"
    stopping = "stopping"
    inactive = "inactive"


class OptionType(str, Enum):
    C = "C"
    P = "P"


class Notification(Base):
    __tablename__ = "notifications"
    __table_args__ = {"schema": "trading"}

    title: Mapped[str] = mapped_column(String(255), nullable=False, primary_key=True)
    body: Mapped[str] = mapped_column(String(255), nullable=False)
    alert_type: Mapped[str] = mapped_column(String(255), nullable=False)


class Strategy(Base):
    __tablename__ = "strategy"
    __table_args__ = {"schema": "trading"}

    strategy: Mapped[str] = mapped_column(String(50), nullable=False, primary_key=True)

    capital: Mapped[float] = mapped_column(Float, nullable=False)
    initial_capital: Mapped[float] = mapped_column(Float, nullable=False)
    status: Mapped[str] = mapped_column(PgEnum(Status), nullable=False)

    # Child records should be deleted when strategy is deleted
    current_stock_positions: Mapped[list["CurrentStockPositions"]] = relationship(
        back_populates="strategy_rel", cascade="all, delete-orphan"
    )
    current_option_positions: Mapped[list["CurrentOptionPositions"]] = relationship(
        back_populates="strategy_rel", cascade="all, delete-orphan"
    )
    target_stock_positions: Mapped[list["TargetStockPositions"]] = relationship(
        back_populates="strategy_rel", cascade="all, delete-orphan"
    )
    target_option_positions: Mapped[list["TargetOptionPositions"]] = relationship(
        back_populates="strategy_rel", cascade="all, delete-orphan"
    )
    open_stock_orders: Mapped[list["OpenStockOrders"]] = relationship(
        back_populates="strategy_rel", cascade="all, delete-orphan"
    )
    open_option_orders: Mapped[list["OpenOptionOrders"]] = relationship(
        back_populates="strategy_rel", cascade="all, delete-orphan"
    )

    # Historical transactions should be preserved even if strategy is deleted
    stock_transactions: Mapped[list["StockTransactions"]] = relationship(
        back_populates="strategy_rel",
        cascade="save-update",  # Don't delete transactions if strategy is deleted
    )
    option_transactions: Mapped[list["OptionTransactions"]] = relationship(
        back_populates="strategy_rel",
        cascade="save-update",  # Don't delete transactions if strategy is deleted
    )


class CurrentStockPositions(Base):
    __tablename__ = "current_stock_positions"
    __table_args__ = (
        Index("idx_current_stock_strategy", "strategy"),
        {"schema": "trading"},
    )

    stock: Mapped[str] = mapped_column(String(50), nullable=False, primary_key=True)
    strategy: Mapped[str] = mapped_column(
        ForeignKey("trading.strategy.strategy"), nullable=False, primary_key=True
    )

    avg_price: Mapped[float] = mapped_column(Float, nullable=False)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)
    stop_limit: Mapped[float] = mapped_column(Float, nullable=False)

    strategy_rel: Mapped[Strategy] = relationship(
        back_populates="current_stock_positions"
    )


class CurrentOptionPositions(Base):
    __tablename__ = "current_option_positions"
    __table_args__ = (
        Index("idx_current_option_strategy", "strategy"),  # For filtering by strategy
        Index("idx_current_option_expiry", "expiry"),
        {"schema": "trading"},
    )

    stock: Mapped[str] = mapped_column(String(50), nullable=False, primary_key=True)
    strategy: Mapped[str] = mapped_column(
        ForeignKey("trading.strategy.strategy"), nullable=False, primary_key=True
    )
    expiry: Mapped[str] = mapped_column(String(20), nullable=False, primary_key=True)
    strike: Mapped[float] = mapped_column(Float, nullable=False, primary_key=True)
    multiplier: Mapped[float] = mapped_column(Float, nullable=False, primary_key=True)
    option_type: Mapped[str] = mapped_column(
        PgEnum(OptionType), nullable=False, primary_key=True
    )  # "call" or "put"

    avg_price: Mapped[float] = mapped_column(Float, nullable=False)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)

    strategy_rel: Mapped[Strategy] = relationship(
        back_populates="current_option_positions"
    )


class TargetStockPositions(Base):
    __tablename__ = "target_stock_positions"
    __table_args__ = (
        Index("idx_target_stock_strategy", "strategy"),
        {"schema": "trading"},
    )

    stock: Mapped[str] = mapped_column(String(50), nullable=False, primary_key=True)
    strategy: Mapped[str] = mapped_column(
        ForeignKey("trading.strategy.strategy"), nullable=False, primary_key=True
    )

    stop_limit: Mapped[float] = mapped_column(Float, nullable=False)
    avg_price: Mapped[float] = mapped_column(Float, nullable=False)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)

    strategy_rel: Mapped[Strategy] = relationship(
        back_populates="target_stock_positions"
    )


class TargetOptionPositions(Base):
    __tablename__ = "target_option_positions"
    __table_args__ = (
        Index("idx_target_option_strategy", "strategy"),  # For filtering by strategy
        Index("idx_target_option_expiry", "expiry"),
        {"schema": "trading"},
    )

    stock: Mapped[str] = mapped_column(String(50), nullable=False, primary_key=True)
    strategy: Mapped[str] = mapped_column(
        ForeignKey("trading.strategy.strategy"), nullable=False, primary_key=True
    )
    expiry: Mapped[str] = mapped_column(String(20), nullable=False, primary_key=True)
    strike: Mapped[float] = mapped_column(Float, nullable=False, primary_key=True)
    multiplier: Mapped[float] = mapped_column(Float, nullable=False, primary_key=True)
    option_type: Mapped[str] = mapped_column(
        PgEnum(OptionType), nullable=False, primary_key=True
    )  # "call" or "put"

    avg_price: Mapped[float] = mapped_column(Float, nullable=False)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)

    strategy_rel: Mapped[Strategy] = relationship(
        back_populates="target_option_positions"
    )


class OpenStockOrders(Base):
    __tablename__ = "open_stock_orders"
    __table_args__ = {"schema": "trading"}

    order_id: Mapped[int] = mapped_column(Integer, nullable=False, primary_key=True)
    stock: Mapped[str] = mapped_column(String(50), nullable=False, primary_key=True)
    strategy: Mapped[str] = mapped_column(
        ForeignKey("trading.strategy.strategy"), nullable=False, primary_key=True
    )
    time: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)

    quantity: Mapped[float] = mapped_column(Float, nullable=False)

    strategy_rel: Mapped[Strategy] = relationship(back_populates="open_stock_orders")


class OpenOptionOrders(Base):
    __tablename__ = "open_option_orders"
    __table_args__ = {"schema": "trading"}

    order_id: Mapped[int] = mapped_column(Integer, nullable=False, primary_key=True)
    stock: Mapped[str] = mapped_column(String(50), nullable=False, primary_key=True)
    strategy: Mapped[str] = mapped_column(
        ForeignKey("trading.strategy.strategy"), nullable=False, primary_key=True
    )
    expiry: Mapped[str] = mapped_column(String(20), nullable=False, primary_key=True)
    strike: Mapped[float] = mapped_column(Float, nullable=False, primary_key=True)
    option_type: Mapped[str] = mapped_column(
        PgEnum(OptionType), nullable=False, primary_key=True
    )  # "call" or "put"
    multiplier: Mapped[float] = mapped_column(Float, nullable=False, primary_key=True)
    time: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, primary_key=True
    )

    quantity: Mapped[float] = mapped_column(Float, nullable=False)

    strategy_rel: Mapped[Strategy] = relationship(back_populates="open_option_orders")


class StockTransactions(Base):
    __tablename__ = "stock_transactions"
    __table_args__ = (
        Index(
            "idx_stock_trans_stock_time", "stock", "time"
        ),  # For stock history queries
        Index(
            "idx_stock_trans_strategy_time", "strategy", "time"
        ),  # For strategy performance analysis
        {"schema": "trading"},
    )

    stock: Mapped[str] = mapped_column(String(50), nullable=False, primary_key=True)
    strategy: Mapped[str] = mapped_column(
        ForeignKey("trading.strategy.strategy"), nullable=False, primary_key=True
    )
    time: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, primary_key=True
    )

    price_transacted: Mapped[float] = mapped_column(Float, nullable=False)
    fees: Mapped[float] = mapped_column(Float, nullable=False)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)

    strategy_rel: Mapped[Strategy] = relationship(back_populates="stock_transactions")


class OptionTransactions(Base):
    __tablename__ = "option_transactions"
    __table_args__ = (
        Index(
            "idx_option_trans_stock_time", "stock", "time"
        ),  # For stock history queries
        Index(
            "idx_option_trans_strategy_time", "strategy", "time"
        ),  # For strategy performance analysis
        Index("idx_option_trans_expiry", "expiry"),  # For expiration analysis
        {"schema": "trading"},
    )

    stock: Mapped[str] = mapped_column(String(50), nullable=False, primary_key=True)
    strategy: Mapped[str] = mapped_column(
        ForeignKey("trading.strategy.strategy"), nullable=False, primary_key=True
    )
    expiry: Mapped[str] = mapped_column(String(20), nullable=False, primary_key=True)
    strike: Mapped[float] = mapped_column(Float, nullable=False, primary_key=True)
    multiplier: Mapped[float] = mapped_column(Float, nullable=False, primary_key=True)
    option_type: Mapped[str] = mapped_column(
        PgEnum(OptionType), nullable=False, primary_key=True
    )  # "call" or "put"
    time: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, primary_key=True
    )

    price_transacted: Mapped[float] = mapped_column(Float, nullable=False)
    fees: Mapped[float] = mapped_column(Float, nullable=False)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)

    strategy_rel: Mapped[Strategy] = relationship(back_populates="option_transactions")


class HistoricalData(Base):
    __tablename__ = "historical_data"
    __table_args__ = (
        Index("idx_historical_stock", "stock"),  # For filtering by stock
        Index("idx_historical_time_range", "time"),  # For time range queries
        {"schema": "market_data"},
    )

    stock: Mapped[str] = mapped_column(String(50), nullable=False, primary_key=True)
    time: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, primary_key=True
    )
    open: Mapped[float] = mapped_column(Float, nullable=False)
    high: Mapped[float] = mapped_column(Float, nullable=False)
    low: Mapped[float] = mapped_column(Float, nullable=False)
    close: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[int] = mapped_column(BigInteger, nullable=False)


class HistoricalVolatilityData(Base):
    __tablename__ = "historical_volatility_data"
    __table_args__ = (
        Index("idx_volatility_stock", "stock", desc("time")),  # For filtering by stock
        Index("idx_volatility_time", "time"),  # For time-based queries
        {"schema": "market_data"},
    )

    stock: Mapped[str] = mapped_column(String(50), nullable=False, primary_key=True)
    time: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, primary_key=True
    )
    # strike: Mapped[float] = mapped_column(Float, nullable=False, primary_key=True)
    open: Mapped[float] = mapped_column(Float, nullable=False)
    high: Mapped[float] = mapped_column(Float, nullable=False)
    low: Mapped[float] = mapped_column(Float, nullable=False)
    close: Mapped[float] = mapped_column(Float, nullable=False)


# ---------- SPACE FOR DB FOR STRATEGIES ----------------
class HistoricalOptionsData(Base):
    __tablename__ = "historical_options_data"
    __table_args__ = (
        Index(
            "idx_option_trans_stock_time", "stock", "time"
        ),  # For stock history queries
        Index("idx_option_trans_expiry", "expiry"),  # For expiration analysis
        {"schema": "phantom_trading"},
    )

    stock: Mapped[str] = mapped_column(String(50), nullable=False, primary_key=True)
    expiry: Mapped[str] = mapped_column(String(20), nullable=False, primary_key=True)
    strike: Mapped[float] = mapped_column(Float, nullable=False, primary_key=True)
    multiplier: Mapped[float] = mapped_column(Float, nullable=False, primary_key=True)
    option_type: Mapped[str] = mapped_column(
        PgEnum(OptionType), nullable=False, primary_key=True
    )  # "call" or "put"
    time: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, primary_key=True
    )

    open: Mapped[float] = mapped_column(Float, nullable=False)
    high: Mapped[float] = mapped_column(Float, nullable=False)
    low: Mapped[float] = mapped_column(Float, nullable=False)
    close: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[float] = mapped_column(Float, nullable=False)


class PhantomPortfolioValue(Base):
    __tablename__ = "phantom_portfolio_value"
    __table_args__ = {"schema": "phantom_trading"}

    time: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, primary_key=True
    )
    cash_portfolio_value: Mapped[float] = mapped_column(Float, nullable=False)
    option_portfolio_value: Mapped[float] = mapped_column(Float, nullable=False)
    bought_price: Mapped[float] = mapped_column(Float, nullable=False)
    strike: Mapped[float] = mapped_column(Float, nullable=False)
    peak: Mapped[float] = mapped_column(Float, nullable=False)
    paused: Mapped[bool] = mapped_column(Boolean, nullable=False)
    resume_trades: Mapped[int] = mapped_column(Integer, nullable=False)


#
# CONSIDERATIONS
# CurrentPositions and TargetPositions should remain relatively small in size and queries across the child tables should not affect performance significantly
# Data Integrity in terms of ezier to handle new instruments

# OpenOrders and Transsactions stay as single tables since transaction records largely identical for different instruments
