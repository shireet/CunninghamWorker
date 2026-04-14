import json
import logging

import aio_pika

from cunninghamworker.bll.config import Settings
from cunninghamworker.bll.interfaces import IResultReporter
from cunninghamworker.domain.entities import ExecutionResult

logger = logging.getLogger(__name__)


class CoreApiResultReporter(IResultReporter):
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._connection: aio_pika.RobustConnection | None = None
        self._channel: aio_pika.Channel | None = None

    async def _ensure_connected(self) -> None:
        if self._connection and self._channel:
            return

        self._connection = await aio_pika.connect_robust(
            host=self._settings.rabbitmq_host,
            port=self._settings.rabbitmq_port,
            login=self._settings.rabbitmq_username,
            password=self._settings.rabbitmq_password,
        )
        self._channel = await self._connection.channel()

    async def close(self) -> None:
        if self._connection:
            await self._connection.close()
        logger.info("Disconnected from RabbitMQ")

    async def _publish(self, queue_name: str, payload: dict) -> None:
        await self._ensure_connected()
        assert self._channel is not None

        queue = await self._channel.declare_queue(queue_name, durable=True)
        body = json.dumps(payload).encode()
        message = aio_pika.Message(body=body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT)
        await self._channel.default_exchange.publish(message, routing_key=queue.name)

    async def report_result(self, result: ExecutionResult) -> None:
        await self._publish(
            self._settings.rabbitmq_execution_results_queue_name,
            {
                "session_id": str(result.session_id),
                "statement_id": str(result.statement_id),
                "bot_response": result.bot_response,
                "success": result.success,
                "error_message": result.error_message,
            },
        )

    async def report_session_complete(self, session_id: str) -> None:
        await self._publish(
            self._settings.rabbitmq_session_completed_queue_name,
            {"session_id": session_id},
        )
