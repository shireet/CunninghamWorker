import logging

from telethon import TelegramClient
from telethon.errors import FloodWaitError

from cunninghamworker.bll.config import Settings
from cunninghamworker.bll.interfaces import IJobExecutor
from cunninghamworker.domain.entities import ExecutionJob, ExecutionResult
from cunninghamworker.domain.exceptions import TelegramError

logger = logging.getLogger(__name__)


class TelethonJobExecutor(IJobExecutor):
    def __init__(self, settings: Settings) -> None:
        self._client = TelegramClient(
            settings.telegram_session_string or "cunningham_worker",
            settings.telegram_api_id,
            settings.telegram_api_hash,
        )
        self._settings = settings

    async def start(self) -> None:
        await self._client.start(bot_token=None)
        logger.info("Telegram client started")

    async def stop(self) -> None:
        await self._client.disconnect()
        logger.info("Telegram client stopped")

    async def execute(self, job: ExecutionJob) -> ExecutionResult:
        try:
            if not self._client.is_connected():
                await self._client.connect()

            entity = await self._client.get_entity(job.target_bot_username)

            response = await self._client.send_message(entity, job.content)

            await response.wait_read()

            return ExecutionResult(
                job_id=job.job_id,
                session_id=job.session_id,
                statement_id=job.statement_id,
                bot_response=response.message,
                success=True,
            )

        except FloodWaitError as e:
            logger.warning(f"Rate limited, waiting {e.seconds} seconds")
            raise TelegramError(f"Rate limited: {e.seconds} seconds") from e

        except Exception as e:
            logger.error(f"Telegram execution failed: {e}")
            return ExecutionResult(
                job_id=job.job_id,
                session_id=job.session_id,
                statement_id=job.statement_id,
                bot_response=None,
                success=False,
                error_message=str(e),
            )
