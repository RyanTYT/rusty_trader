# To define context managers
from asyncio import current_task
from functools import wraps
from contextlib import asynccontextmanager, contextmanager

# Sqlalchemy
from sqlalchemy import Engine, text, create_engine
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_scoped_session,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeMeta, sessionmaker, Session
from sqlalchemy.inspection import inspect
from sqlalchemy.engine import Connection
from typing import (
    AsyncGenerator,
    Callable,
    Any,
    Type,
    TypeVar,
    Generator,
    ParamSpec,
    Concatenate,
    cast,
)

# Local Files
import logging
from app.services.models.AsyncBaseCRUD import AsyncCRUD
from app.utils.custom_logging import CustomLogger
from app.models import Base
from app.services.models.BaseCRUD import CRUD
import os

engine: Engine | None = None
async_engine: AsyncEngine | None = None
SessionLocal: sessionmaker[Session] = sessionmaker()
AsyncSessionLocal: async_scoped_session[AsyncSession] = async_scoped_session(
    async_sessionmaker(), scopefunc=current_task
)


# Helper function to infer a basic default based on column type
def get_default_value(col_type: str) -> int | str | bool | None:
    if "INTEGER" in col_type:
        return 0
    elif "VARCHAR" in col_type or "TEXT" in col_type:
        return ""
    elif "BOOLEAN" in col_type:
        return False
    return None


def init_db(log_file: str) -> None:
    global engine, async_engine, SessionLocal, AsyncSessionLocal
    # Access environment variable directly using os.environ
    DATABASE_URL = os.getenv("DATABASE_URL")
    ASYNC_DATABASE_URL = os.getenv("ASYNC_DATABASE_URL")
    if DATABASE_URL is None:
        DATABASE_URL = ""
    if ASYNC_DATABASE_URL is None:
        ASYNC_DATABASE_URL = ""
    engine = create_engine(
        DATABASE_URL,
        pool_size=10,
        max_overflow=20,
        pool_timeout=30,
        pool_recycle=1800,  # Recycle connections every 30 mins
        pool_pre_ping=True,  # Check connection liveness before using
    )  # Create an engine
    async_engine = create_async_engine(
        ASYNC_DATABASE_URL,
        pool_size=100,
        max_overflow=20,
        pool_timeout=30,
        pool_recycle=1800,  # Recycle connections every 30 mins
        pool_pre_ping=True,  # Check connection liveness before using
    )
    SessionLocal = sessionmaker(bind=engine)  # Create a session factory
    # AsyncSessionLocal = async_sessionmaker(bind=async_engine)
    async_session_factory = async_sessionmaker(bind=async_engine)
    AsyncSessionLocal = async_scoped_session(
        async_session_factory, scopefunc=current_task
    )

    logger = CustomLogger("init_db()")

    sql_logger = logging.getLogger("sqlalchemy.engine")
    sql_logger.setLevel(logging.INFO)
    sql_logger.addHandler(logging.FileHandler(log_file))

    # 1. Ensure schemas exist in a separate, committed transaction
    with engine.connect() as connection:  # Use engine.connect() for a single connection
        connection.execute(
            text("CREATE EXTENSION IF NOT EXISTS timescaledb;")
        )  # Extension first
        connection.execute(
            text(
                "ALTER DATABASE trading_system SET timescaledb.enable_cagg_window_functions = TRUE;"
            )
        )
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS trading;"))
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS phantom_trading;"))
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS market_data;"))
        connection.commit()  # Explicitly commit schema creation

    # 2. Then, create tables using Base.metadata.create_all
    # This might use a new connection from the engine's pool,
    # but the schemas will now definitely exist.
    Base.metadata.create_all(engine)

    # Ensure schemas exist
    with engine.begin() as connection:
        # Step 4: Convert historical_data to hypertable
        connection.execute(
            text("""
            SELECT create_hypertable('market_data.historical_data', 'time', if_not_exists => TRUE);
        """)
        )

        # Step 5: Create continuous aggregate view
        # Daily Returns
        connection.execute(
            text("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS market_data.daily_ohlcv
            WITH (timescaledb.continuous) AS
            SELECT
                stock,
                time_bucket('1 day', time) AS day,
                first(open, time) AS open,
                max(high) AS high,
                min(low) AS low,
                last(close, time) AS close,
                sum(volume) AS volume
            FROM market_data.historical_data
            GROUP BY stock, day
            WITH NO DATA;
        """)
        )

        # Daily Volatility Calculator
        # connection.execute(
        #     text("""
        #     CREATE MATERIALIZED VIEW IF NOT EXISTS market_data.daily_volatility
        #     WITH (timescaledb.continuous) AS
        #     SELECT
        #         stock,
        #         day,
        #         stddev_samp(close / open) OVER (
        #             PARTITION BY stock
        #             ORDER BY day
        #             ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        #         ) AS rolling_volatility
        #     FROM market_data.daily_ohlcv
        #     WITH NO DATA;
        # """)
        # )
        connection.execute(
            text("""
            CREATE OR REPLACE VIEW market_data.daily_volatility AS
            SELECT
                stock,
                day,
                stddev_samp(close / open) OVER (
                    PARTITION BY stock
                    ORDER BY day
                    ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
                ) AS rolling_volatility
            FROM market_data.daily_ohlcv; -- Querying the continuous aggregate
        """)
        )

        # Step 6: Set up refresh policy to run daily
        connection.execute(
            text("""
            SELECT add_continuous_aggregate_policy('market_data.daily_ohlcv',
                start_offset => INTERVAL '1 month',
                end_offset => INTERVAL '1 hour',
                schedule_interval => INTERVAL '1 day',
                if_not_exists => TRUE);
        """)
        )
        # connection.execute(
        #     text("""
        #     SELECT add_continuous_aggregate_policy('market_data.daily_volatility',
        #         start_offset => INTERVAL '1 month',
        #         end_offset => INTERVAL '1 hour',
        #         schedule_interval => INTERVAL '1 day');
        # """)
        # )

    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()

    with engine.connect() as connection:
        for table in Base.metadata.tables.values():
            if table.name in existing_tables:
                existing_columns = [
                    col["name"] for col in inspector.get_columns(table.name)
                ]

                for column in table.columns:
                    if column.name not in existing_columns:
                        # Handle NOT NULL columns by adding a DEFAULT value
                        col_type = column.type.compile(dialect=engine.dialect)
                        alter_stmt = f"ALTER TABLE {table.schema}.{table.name} ADD COLUMN {column.name} {col_type}"

                        # If NOT NULL, require a DEFAULT value
                        if not column.nullable:
                            default = column.default
                            default_value = (
                                default.arg
                                if (default is not None and hasattr(default, "arg"))
                                else get_default_value(col_type)
                            )
                            # default_value = column.default.arg if column.default is not None else get_default_value(col_type)
                            if default_value is None:
                                raise ValueError(
                                    f"Cannot add non-nullable column '{column.name}' without a default value."
                                )
                            alter_stmt += f" DEFAULT {repr(default_value)} NOT NULL"

                        connection.execute(text(alter_stmt))
                        logger.info(
                            f'Added missing column "{column.name}" to table "{table.name}".'
                        )

    logger.info("Database initialization complete.")


# Function to get a session
@contextmanager
def get_db() -> Generator[Session, None, None]:
    db: Session = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@asynccontextmanager
async def async_get_db() -> AsyncGenerator[AsyncSession, None]:
    db: AsyncSession = AsyncSessionLocal()
    try:
        yield db
    finally:
        await db.close()


base_crud_type = TypeVar("base_crud_type", bound=CRUD[Any, Any, Any, Any])
base_async_crud_type = TypeVar(
    "base_async_crud_type", bound=AsyncCRUD[Any, Any, Any, Any]
)
return_type = TypeVar("return_type", bound=Any)
P = ParamSpec("P")


def with_engine(
    func: Callable[Concatenate[Connection, P], return_type],
) -> Callable[P, return_type]:
    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> return_type:
        assert engine
        with engine.connect() as conn:
            result = func(
                conn, *args, **kwargs
            )  # Inject into standalone or static method
            return result

    return wrapper


def async_with_engine(
    func: Callable[Concatenate[AsyncConnection, P], return_type],
) -> Callable[P, return_type]:
    @wraps(func)
    async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> return_type:
        assert async_engine
        async with async_engine.connect() as conn:
            result = await func(
                conn, *args, **kwargs
            )  # Inject into standalone or static method
            return cast(return_type, result)
            # return cast(return_type, result)

    return cast(Callable[P, return_type], async_wrapper)


def with_db_session_for_model(
    crud_model: Type[base_crud_type],
    model: Type[Base],
) -> Callable[
    [Callable[Concatenate[base_crud_type, P], return_type]], Callable[P, return_type]
]:
    def with_db_session(
        func: Callable[Concatenate[base_crud_type, P], return_type],
    ) -> Callable[P, return_type]:
        """Decorator to manage DB session lifecycle."""

        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> return_type:
            with get_db() as db:  # Automatically opens & closes session
                assert engine
                session_model = crud_model(
                    model, db, engine
                )  # Create the model instance
                try:
                    result = func(
                        session_model, *args, **kwargs
                    )  # Inject into standalone or static method
                    db.commit()  # Commit if successful
                    return result
                except Exception as e:
                    db.rollback()  # Rollback on failure
                    raise e  # Re-raise the error

        return wrapper

    return with_db_session


def async_with_db_session_for_model(
    crud_model: Type[base_async_crud_type],
    model: Type[Base],
) -> Callable[
    [Callable[Concatenate[base_async_crud_type, P], return_type]],
    Callable[P, return_type],
]:
    def with_db_session(
        func: Callable[Concatenate[base_async_crud_type, P], return_type],
    ) -> Callable[P, return_type]:
        """Decorator to manage DB session lifecycle."""

        @wraps(func)
        async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> return_type:
            async with async_get_db() as db:  # Automatically opens & closes session
                assert async_engine
                session_model = crud_model(
                    model, db, async_engine
                )  # Create the model instance
                try:
                    result = await func(
                        session_model, *args, **kwargs
                    )  # Inject into standalone or static method
                    await db.commit()  # Commit if successful
                    return cast(return_type, result)
                except Exception as e:
                    await db.rollback()  # Rollback on failure
                    raise e  # Re-raise the error

        return cast(Callable[P, return_type], async_wrapper)

    return with_db_session


def with_db_session_for_model_class_method(
    crud_model: Type[base_crud_type],
    model: Type[Base],
    variable_name: str = "model",
) -> Callable[[Callable[P, return_type]], Callable[P, return_type]]:
    def with_db_session(func: Callable[P, return_type]) -> Callable[P, return_type]:
        """Decorator to manage DB session lifecycle."""

        # Function is sync
        @wraps(func)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> return_type:
            with get_db() as db:
                assert engine
                session_model = crud_model(model, db, engine)

                try:
                    setattr(args[0], variable_name, session_model)
                    result = func(*args, **kwargs)
                    db.commit()
                    return result
                except Exception as e:
                    db.rollback()
                    raise e
                finally:
                    setattr(args[0], variable_name, None)

        return sync_wrapper

    return with_db_session


def async_with_db_session_for_model_class_method(
    crud_model: Type[base_async_crud_type],
    model: Type[Base],
    variable_name: str = "model",
) -> Callable[[Callable[P, return_type]], Callable[P, return_type]]:
    def with_db_session(func: Callable[P, return_type]) -> Callable[P, return_type]:
        """Decorator to manage DB session lifecycle."""

        # Function is async
        @wraps(func)
        async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> return_type:
            async with async_get_db() as db:  # Using async context manager
                assert async_engine
                session_model = crud_model(model, db, async_engine)

                try:
                    setattr(args[0], variable_name, session_model)
                    result = await func(*args, **kwargs)  # Await async function
                    await db.commit()  # Ensure commit is awaited
                    return cast(return_type, result)
                except Exception as e:
                    await db.rollback()
                    raise e
                finally:
                    setattr(args[0], variable_name, None)

        return cast(Callable[P, return_type], async_wrapper)

    return with_db_session


if __name__ == "__main__":
    init_db("logs/sqlalchemy/app.log")
