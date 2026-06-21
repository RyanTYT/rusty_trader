from time import struct_time
import pytz
import logging
from logging.handlers import TimedRotatingFileHandler
import shutil
from colorama import Fore, Style, init
from datetime import datetime, timedelta, timezone, time
from typing import Callable, Dict
from zoneinfo import ZoneInfo

# Initialize colorama
init(autoreset=True)


class TZTimedRotatingFileHandler(TimedRotatingFileHandler):
    def __init__(
        self,
        filename: str,
        when: str = "midnight",
        interval: int = 1,
        backupCount: int = 0,
        encoding: str | None = None,
        delay: bool = False,
        utc: bool = False,
        atTime: time | None = None,
        tz: ZoneInfo | None = None,
    ) -> None:
        self.tz = tz or ZoneInfo("UTC")
        super().__init__(
            filename,
            when,
            interval,
            backupCount,
            encoding,
            delay,
            utc=utc,
            atTime=atTime,
        )

    def computeRollover(self, currentTime: int) -> int:
        """
        Compute the next rollover time for the handler, considering the specified timezone.
        """
        if self.utc:
            t = datetime.fromtimestamp(currentTime, tz=timezone.utc)
        elif self.tz:
            t = datetime.fromtimestamp(currentTime).astimezone(self.tz)
        else:
            t = datetime.fromtimestamp(
                currentTime
            ).astimezone()  # Local system timezone

        # Initialize nextRollover. This will be timezone-aware from the start.
        nextRollover = None

        if self.when == "s":
            # Seconds
            nextRollover = t + timedelta(seconds=self.interval)
        elif self.when == "m":
            # Minutes
            remainder = t.second % self.interval
            nextRollover = t + timedelta(seconds=self.interval - remainder)
            nextRollover = nextRollover.replace(microsecond=0)
        elif self.when == "h":
            # Hours
            remainder = t.minute % self.interval
            nextRollover = t + timedelta(minutes=self.interval - remainder)
            nextRollover = nextRollover.replace(second=0, microsecond=0)
        elif self.when == "d":
            # Days (midnight)
            nextRollover = t.replace(
                hour=0, minute=0, second=0, microsecond=0
            ) + timedelta(days=1)
        elif self.when == "midnight":
            nextRollover = t.replace(
                hour=0, minute=0, second=0, microsecond=0
            ) + timedelta(days=1)
            if self.atTime is not None:
                # Replace time components with atTime in the *next day's* context
                nextRollover = nextRollover.replace(
                    hour=self.atTime.hour,
                    minute=self.atTime.minute,
                    second=self.atTime.second,
                )

        elif self.when.startswith("w"):
            # Weekly rotation
            dayOfWeek = int(self.when[1])
            daysToTarget = (dayOfWeek - t.weekday() + 7) % 7
            if daysToTarget == 0:  # If today is the target day, roll over next week
                daysToTarget = 7
            nextRollover = t.replace(
                hour=0, minute=0, second=0, microsecond=0
            ) + timedelta(days=daysToTarget)
            if self.atTime is not None:
                nextRollover = nextRollover.replace(
                    hour=self.atTime.hour,
                    minute=self.atTime.minute,
                    second=self.atTime.second,
                )

        if nextRollover is None:
            # Fallback for unsupported 'when' types or initial calculation issues
            return super().computeRollover(currentTime)

        return int(nextRollover.timestamp())


class CustomLogger:
    # Define the separator as a class attribute
    _logger_cache: Dict[str, logging.Logger] = {}
    TERMINAL_WIDTH = shutil.get_terminal_size((80, 20)).columns
    SEPARATOR = "-" * TERMINAL_WIDTH

    def __init__(self, name: str):
        """
        Initialize the custom logger.

        Args:
            name (str): Name of the logger.
            log_file (str): File where logs will be written.
        """
        self.name = name
        if name in CustomLogger._logger_cache:
            return
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        # Create handlers
        # file_handler = logging.FileHandler(log_file)
        file_handler = TZTimedRotatingFileHandler(
            "logs/trading-app.log",
            when="midnight",  # Rotate daily
            backupCount=7,  # Keep 7 backup files
            atTime=time(0, 0, 0),  # Rotate at 3:00 AM in Singapore time
            tz=ZoneInfo("US/Eastern"),  # Specify the timezone name
        )
        console_handler = logging.StreamHandler()

        # Set log format
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(name)s - %(module)s.%(funcName)s:%(lineno)d - %(message)s"
        )

        def eastern_time_converter(timestamp: float | None) -> struct_time:
            assert timestamp is not None
            return (
                datetime.fromtimestamp(timestamp, tz=timezone.utc)
                .astimezone(ZoneInfo("US/Eastern"))
                .timetuple()
            )

        formatter.converter = eastern_time_converter

        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add handlers to the logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        CustomLogger._logger_cache[name] = self.logger

    def get_logger(self) -> logging.Logger:
        return CustomLogger._logger_cache[self.name]

    @staticmethod
    def with_separator(color: str) -> Callable[..., Callable[[object, str], None]]:
        """
        Decorator to add separators around log messages.

        Args:
            color (str): The color to use for the log message.

        Returns:
            Callable: The wrapped function.
        """

        def decorator(func: Callable[..., None]) -> Callable[..., None]:
            def wrapper(self: object, message: str) -> None:
                formatted_message = f"\n{color}{CustomLogger.SEPARATOR}\n{message}\n{CustomLogger.SEPARATOR}{Style.RESET_ALL}"
                return func(self, formatted_message)

            return wrapper

        return decorator

    @staticmethod
    def without_separator(color: str) -> Callable[..., Callable[[object, str], None]]:
        """
        Decorator to add separators around log messages.

        Args:
            color (str): The color to use for the log message.

        Returns:
            Callable: The wrapped function.
        """

        def decorator(func: Callable[..., None]) -> Callable[..., None]:
            def wrapper(self: object, message: str) -> None:
                # formatted_message = f"\n{color}{CustomLogger.SEPARATOR}\n{message}\n{CustomLogger.SEPARATOR}{Style.RESET_ALL}"
                formatted_message = f"\n{color}{message}{Style.RESET_ALL}"
                return func(self, formatted_message)

            return wrapper

        return decorator

    @without_separator(color=Fore.BLUE)
    def debug(self, message: str, *args, **kwargs) -> None:
        """Log a debug message."""
        self.get_logger().debug(message, *args, **kwargs, stacklevel=3)

    @without_separator(color=Fore.GREEN)
    def info(self, message: str, *args, **kwargs) -> None:
        """Log an info message."""
        self.get_logger().info(message, *args, **kwargs, stacklevel=3)

    @without_separator(color=Fore.YELLOW)
    def warning(self, message: str, *args, **kwargs) -> None:
        """Log a warning message."""
        self.get_logger().warning(message, *args, **kwargs, stacklevel=3)

    @with_separator(color=Fore.RED)
    def error(self, message: str, *args, **kwargs) -> None:
        """Log an error message."""
        self.get_logger().error(message, *args, **kwargs, stacklevel=3)

    @with_separator(color=Fore.MAGENTA)
    def critical(self, message: str, *args, **kwargs) -> None:
        """Log a critical message."""
        self.get_logger().critical(message, *args, **kwargs, stacklevel=3)


if __name__ == "__main__":
    # Create a logger instance
    logger = CustomLogger(name="MyAppLogger")

    # Log messages
    def hello():
        logger.debug("This is a debug message.")
    hello()
    logger.info("This is an info message.")
    logger.warning("This is a warning message.")
    logger.error("This is an error message.")
    logger.critical("This is a critical message.")
