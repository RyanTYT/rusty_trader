from asyncio.subprocess import Process
import os
import re
import socket
import asyncio

from app.utils.custom_logging import CustomLogger


def send_command_to_ibc(command: str) -> None:
    """Decorator to manage DB session lifecycle."""
    host = os.environ["HOST"]

    logger = CustomLogger("send_command_to_ibc()")
    try:
        sock = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM
        )  # AF_INET: IPv4, SOCK_STREAM: TCP
    except Exception as e:
        logger.info(f"Failed to connect to Command Server. Command Server not up?: {e}")
        return
    sock.connect((host, 7462))

    sock.sendall(command.encode("utf-8"))

    response = sock.recv(1024)
    logger.info(f"Response: {response.decode('utf-8')}")

    sock.sendall("EXIT\r\n".encode("utf-8"))

    # Close the connection
    sock.close()


# async def start_ibkr() -> None:
#     logger = CustomLogger("IBC")
#
#     # out = await asyncio.create_subprocess_exec(
#     #     "ls",
#     #     "/home/tws/.local/share/i4j_jres/Oda-jK0QgTEmVssfllLP/1.8.0_202_64/lib/amd64"
#     # )
#     out = await asyncio.create_subprocess_exec(
#         "/IBCLinux-3.21.2/scripts/ibcstart.sh",
#         "1030",
#         "--tws-path=/home/tws",
#         "--tws-settings-path=/home/tws",
#         "--ibc-path=/IBCLinux-3.21.2",
#         "--ibc-ini=/IBCLinux-3.21.2/config.ini",
#         "--user=",
#         "--pw=",
#         "--fix-user=",
#         "--fix-pw=",
#         "--java-path=",
#         "--mode=paper",
#         "--on2fatimeout=restart",
#         stdout=asyncio.subprocess.PIPE,
#     )
#     logger = CustomLogger("IBC")
#
#     ibc_started = False
#     try_no = 0
#     logger.info("IBC LOGS START")
#     while not ibc_started:
#         if try_no == 10:
#             break
#         try_no += 1
#         logger.info(f"Try {try_no} at starting IBC and IBKR:\n")
#         while out:
#             if out.stdout is None:
#                 logger.error("start_ibkr(): out.stdout is None?")
#                 break
#             line = await out.stdout.readline()
#             if not line:
#                 logger.error("Line is empty! IBC start failed, retrying again.")
#                 break
#             log = line.strip().decode()
#             pattern = (
#                 r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}:\d{3} IBC: Click button: OK"
#             )
#             if bool(re.match(pattern, log)):
#                 logger.info("IBC/IBKR succesfully started!")
#                 ibc_started = True
#                 break
#
#     logger.info("IBC LOGS END - IBKR started")


async def monitor_ibc_output(process: Process, logger: CustomLogger) -> bool:
    assert process.stdout is not None, "Process stdout is None"

    debug_patterns = [
        r"^trading-bot_1 \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}:\d{3} IBC: CommandServer is starting with port \d+$",
        r"^trading-bot_1 \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}:\d{3} IBC: CommandServer listening on addresses: [\d\.,]+; port: \d+$",
        r"^trading-bot_1 \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}:\d{3} IBC: Starting Gateway$",
        r"^trading-bot_1 \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}:\d{3} IBC: Login attempt: 1$",
        r"^trading-bot_1 \| \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}:\d{3} IBC: Found Gateway main window$",
    ]

    pattern = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}:\d{3} IBC: Click button: OK"
    exit_pattern = r"IBC returned exit status \d+"

    while True:
        line = await process.stdout.readline()
        log = line.strip().decode()
        for patt in debug_patterns:
            if re.match(patt, log):
                logger.info(log)
        if re.match(exit_pattern, log):
            return False
        if re.match(pattern, log):
            return True  # Success


async def start_ibkr() -> None:
    logger = CustomLogger("IBC")

    ibc_started = False
    try_no = 0
    logger.info("IBC LOGS START")

    while not ibc_started and try_no < 10:
        try_no += 1
        logger.info(f"Try {try_no} at starting IBC and IBKR:\n")

        process = await asyncio.create_subprocess_exec(
            "/IBCLinux-3.21.2/scripts/ibcstart.sh",
            "1030",
            "--gateway",
            "--tws-path=/home/tws",
            "--tws-settings-path=/home/tws",
            "--ibc-path=/IBCLinux-3.21.2",
            "--ibc-ini=/IBCLinux-3.21.2/config.ini",
            "--user=",
            "--pw=",
            "--fix-user=",
            "--fix-pw=",
            "--java-path=",
            "--mode=paper",
            "--on2fatimeout=restart",
            stdout=asyncio.subprocess.PIPE,
        )

        try:
            # Timeout per attempt (e.g., 10 seconds)
            ibc_started = await asyncio.wait_for(
                monitor_ibc_output(process, logger), timeout=180
            )
            if ibc_started:
                logger.info("IBC/IBKR successfully started!")
            else:
                logger.warning(
                    f"IBC failed to start IBKR.\nRetrying again for try no. {try_no}"
                )
        except asyncio.TimeoutError:
            logger.warning(f"Attempt {try_no} timed out — retrying...")
            process.terminate()  # Gracefully terminate process
            try:
                await asyncio.wait_for(process.wait(), timeout=20)
            except asyncio.TimeoutError:
                logger.warning("Process did not terminate in time. Killing it...")
                process.kill()

    if not ibc_started:
        logger.error(f"Failed to start IBC/IBKR after {try_no} attempts.")

    logger.info("IBC LOGS END - IBKR start process complete.")


async def full_restart_ibkr() -> None:
    """
    Attempts to completely restart IBKR (i.e. Completely shut down, then reload the application again using IBC)
    """
    send_command_to_ibc("STOP\r\n")
    await start_ibkr()
