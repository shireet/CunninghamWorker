import json
import logging

import aio_pika
from aio_pika.exceptions import QueueEmpty

from cunninghamworker.bll.config import Settings
from cunninghamworker.bll.interfaces import IJobConsumer
from cunninghamworker.domain.entities import ExecutionJob

logger = logging.getLogger(__name__)


class RabbitMqJobConsumer(IJobConsumer):
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._connection: aio_pika.RobustConnection | None = None
        self._channel: aio_pika.Channel | None = None

    async def connect(self) -> None:
        self._connection = await aio_pika.connect_robust(
            host=self._settings.rabbitmq_host,
            port=self._settings.rabbitmq_port,
            login=self._settings.rabbitmq_username,
            password=self._settings.rabbitmq_password,
        )
        self._channel = await self._connection.channel()
        logger.info("Connected to RabbitMQ")

    async def disconnect(self) -> None:
        if self._connection:
            await self._connection.close()
        logger.info("Disconnected from RabbitMQ")

    async def consume_job(self) -> ExecutionJob | None:
        if not self._channel:
            raise RuntimeError("Not connected to RabbitMQ")

        queue = await self._channel.declare_queue(
            self._settings.rabbitmq_queue_name,
            durable=True,
        )

        try:
            message = await queue.get(timeout=self._settings.worker_poll_timeout_seconds)
        except QueueEmpty:
            return None

        if message is None:
            return None

        # Parse before ACK — reject malformed messages back to queue
        try:
            data = json.loads(message.body.decode())
        except Exception as e:
            logger.error("Failed to parse job message: %s", e)
            await message.nack(requeue=True)
            return None

        async with message.process():
            return ExecutionJob(
                job_id=data["job_id"],
                session_id=data["session_id"],
                statement_id=data["statement_id"],
                target_bot_username=data["target_bot_username"],
                content=data["content"],
                max_retries=data.get("max_retries", 3),
                total_jobs_in_session=data.get("total_jobs_in_session", 1),
            )
