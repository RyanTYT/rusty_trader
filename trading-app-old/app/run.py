from time import time
import pandas_market_calendars as mcal
import pytz
import datetime
from fastapi import FastAPI
from uvicorn import Config, Server
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from typing import List, Any, Dict, Tuple
from app.services.broker.Broker import Broker
from app.utils.custom_logging import CustomLogger
from app.tasks.execution_tasks import (
    validate_current_positions_for_stocks,
    update_target_position_and_send_orders_for_broker,
)
from app.create_app import init_app
from app.services.IBC import send_command_to_ibc
import asyncio
import nest_asyncio  # type: ignore
import traceback
import os
from threading import Thread

nest_asyncio.apply()

server_started = False


def start_server(all_brokers: List[Broker]) -> None:
    """Start FastAPI Server"""
    global server_started
    if server_started:
        return

    server_started = True
    app = FastAPI()

    @app.post("/update-all-orders")
    async def update_all_orders() -> Dict[str, Any]:
        status, error_str = await update_orders_for_all_strats(all_brokers)
        if status:
            return {"status": 200, "msg": "Orders Triggered for updated positions!"}
        else:
            return {"status": 500, "msg": error_str}

    @app.get("/check-health")
    async def health_check() -> Dict[str, str]:
        return {"status": "ok"}

    host = os.environ["SERVER_HOST"]
    port = int(os.environ["PORT"])
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    config = Config(app=app, host=host, port=port, log_level="info")
    server = Server(config=config)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(server.serve())


async def low_frequency_updates_for(brokers: List[Broker]) -> None:
    # await validate_current_positions_for_stocks(brokers[0])
    for broker in brokers:
        await broker.connect_to_broker()
        CustomLogger(broker.strategy).info("Updating orders...")
        await update_target_position_and_send_orders_for_broker(broker)


async def update_orders_for_all_strats(brokers: List[Broker]) -> Tuple[bool, str]:
    for broker in brokers:
        try:
            await broker.connect_to_broker()
        except Exception as e:
            return (
                False,
                f"Unable to connect to Trader Workstation: {e} \nIs it closed for now?",
            )
        CustomLogger(broker.strategy).info("Updating orders...")
        await update_target_position_and_send_orders_for_broker(broker)
    return (True, "")


async def daily_run() -> None:
    global server_started
    try:
        daily, hourly, five_minute = await init_app()

        all_brokers = [
            broker for brokers in (daily, hourly, five_minute) for broker in brokers
        ]

        # Start FastAPI server in a background thread
        Thread(target=lambda: start_server(all_brokers), daemon=True).start()

        # Schedule every 24 hours
        scheduler = AsyncIOScheduler()
        trigger_6h = CronTrigger(
            year="*", month="*", day="*", hour="*/6", minute="0", second="0"
        )

        async def daily_updates() -> None:
            await low_frequency_updates_for(daily)

        scheduler.add_job(
            daily_updates, trigger_6h, id="daily_updates", replace_existing=True
        )
        await low_frequency_updates_for(daily)

        # Schedule every 1 hour
        trigger_1h = CronTrigger(
            year="*", month="*", day="*", hour="*", minute="0", second="0"
        )

        async def hourly_updates() -> None:
            await low_frequency_updates_for(hourly)

        scheduler.add_job(
            hourly_updates, trigger_1h, id="hourly_updates", replace_existing=True
        )
        await low_frequency_updates_for(hourly)

        # Schedula every 5 min
        trigger_5min = CronTrigger(
            year="*", month="*", day="*", hour="*", minute="*/5", second="0"
        )
        await low_frequency_updates_for(five_minute)
        for broker in five_minute:
            await broker.run_live_strategies()

        async def minute_updates() -> None:
            for broker in five_minute:
                await broker.check_live_subs()

        scheduler.add_job(
            minute_updates, trigger_5min, id="minute_updates", replace_existing=True
        )

        scheduler.start()

        # Calculate sleep time until 4:30 PM Eastern Time
        eastern_tz = pytz.timezone("US/Eastern")
        now_eastern = datetime.datetime.now(datetime.timezone.utc).astimezone(
            eastern_tz
        )

        market_close_time = now_eastern.replace(
            hour=16, minute=30, second=0, microsecond=0
        )  # 4:30 PM Eastern

        if now_eastern >= market_close_time:
            # If current time is already past 4:30 PM ET, sleep until 4:30 PM ET the next day
            sleep_until = market_close_time + datetime.timedelta(days=1)
        else:
            sleep_until = market_close_time

        time_to_sleep = (sleep_until - now_eastern).total_seconds()

        five_minute[0].sleep(int(time_to_sleep))

    except KeyboardInterrupt:
        CustomLogger("run.py").error("Keyboard Interrupted: Disconnecting now")
    except Exception:
        e = traceback.format_exc()
        CustomLogger("run.py").error(f"Failed, Disconnecting from brokers now: {e}")
    finally:
        send_command_to_ibc("STOP\r\n")


async def run() -> None:
    while True:
        time_now_eastern = datetime.datetime.now(datetime.timezone.utc).astimezone(
            pytz.timezone("US/Eastern")
        )
        if time_now_eastern.time() < datetime.time(
            hour=9, minute=0, second=0
        ) or time_now_eastern.time() > datetime.time(hour=17, minute=0, second=0):
            start_time = time_now_eastern
            if time_now_eastern.time() > datetime.time(hour=17, minute=0, second=0):
                start_time = (time_now_eastern + datetime.timedelta(days=1)).replace(
                    hour=0, minute=0, second=0
                )
            valid_days = mcal.get_calendar("NYSE").valid_days(
                start_time,
                start_time + datetime.timedelta(days=4),
            )
            next_earliest_day_mcal = valid_days[0]
            next_earliest_datetime = pytz.timezone("US/Eastern").localize(
                datetime.datetime(
                    next_earliest_day_mcal.year,
                    next_earliest_day_mcal.month,
                    next_earliest_day_mcal.day,
                    9,
                    0,
                    0,
                )
            )
            time_to_sleep = (
                next_earliest_datetime
                - datetime.datetime.now(datetime.timezone.utc).astimezone(
                    pytz.timezone("US/Eastern")
                )
            ).total_seconds()

            await asyncio.sleep(time_to_sleep)
        await daily_run()


if __name__ == "__main__":
    asyncio.run(run())
