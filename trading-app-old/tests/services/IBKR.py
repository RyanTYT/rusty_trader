import os
import pytest
from app.services.IBKR import IBAPIBroker
from app.utils.custom_logging import CustomLogger


@pytest.fixture
def broker():
    host = os.environ['HOST']
    return IBAPIBroker(host, 7497, 123)


@pytest.mark.asyncio
async def test_connect_to_ib_disconnect_on_error(broker, mocker):
    # Mock CustomLogger error method using mocker
    mock_logger = mocker.patch('app.utils.custom_logging.CustomLogger', autospec=True)
    mock_logger.SEPARATOR = "---"

    mock_logger_error = mocker.patch.object(CustomLogger, 'error')

    # Mock connect to raise an exception
    mock_connect = mocker.patch.object(broker, 'connect', side_effect=Exception("Connection error"))

    # Mock disconnect_from_ib to ensure it gets called
    mock_disconnect = mocker.patch.object(broker, 'disconnect', autospec=True)

    # Ensure that the exception in connect triggers the error logging and disconnect
    await broker.connect_to_broker()

    # Assert logger.error was called with the correct message
    mock_connect.assert_called_once()
    mock_logger_error.assert_called_once_with("Error connecting to IBKR: Connection error")
    mock_disconnect.assert_called_once()


@pytest.mark.asyncio
async def test_send_orders_disconnect_on_error(broker, mocker):
    mock_logger = mocker.patch('app.utils.custom_logging.CustomLogger', autospec=True)
    mock_logger.SEPARATOR = "---"

    mock_logger_error = mocker.patch.object(CustomLogger, 'error')

    # Mock disconnect_from_ib to ensure it gets called
    mock_disconnect = mocker.patch.object(broker, 'disconnect', autospec=True)

    # Mock send orders to raise an exception
    private_send_order = mocker.patch.object(IBAPIBroker, '_send_order', side_effect=Exception("Order sending error"))
    orders = [{"symbol": "AAPL", "secType": "STK", "exchange": "SMART", "currency": "USD", "action": "BUY", "quantity": 10, "price": 150}]

    responses = await broker.send_orders(orders)
    assert responses == []

    mock_logger_error.assert_called_once_with("Error sending orders: Order sending error")
    mock_disconnect.assert_called_once()
