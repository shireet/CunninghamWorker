import asyncio
import logging
import time
from collections import defaultdict

from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession

from cunninghamworker.bll.config import Settings
from cunninghamworker.bll.interfaces import IJobExecutor
from cunninghamworker.domain.entities import ExecutionJob, ExecutionResult
from cunninghamworker.domain.exceptions import TelegramError

logger = logging.getLogger(__name__)


class TelethonJobExecutor(IJobExecutor):
    def __init__(self, settings: Settings) -> None:
        session_string = settings.telegram_session_string or ""

        if session_string:
            session = StringSession(session_string)
        else:
            session = StringSession()

        self._client = TelegramClient(
            session,
            settings.telegram_api_id,
            settings.telegram_api_hash,
        )
        self._settings = settings
        self._semaphore = asyncio.Semaphore(settings.telegram_rate_limit)
        self._session_locks: dict[str, asyncio.Lock] = {}
        self._locks_lock = asyncio.Lock()
        self._active_sessions: set[str] = set()

    async def _get_session_lock(self, session_id: str) -> asyncio.Lock:
        async with self._locks_lock:
            if session_id not in self._session_locks:
                self._session_locks[session_id] = asyncio.Lock()
                self._active_sessions.add(session_id)
            return self._session_locks[session_id]

    async def cleanup_session_lock(self, session_id: str) -> None:
        async with self._locks_lock:
            self._session_locks.pop(session_id, None)
            self._active_sessions.discard(session_id)
            logger.debug("Cleaned up session lock for %s", session_id)

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

            session_id = str(job.session_id)
            session_lock = await self._get_session_lock(session_id)

            async with session_lock:
                logger.info("Job %s: Acquired session lock for %s", job.job_id, session_id)
                async with self._semaphore:
                    return await self._execute_job(job)

        except FloodWaitError as e:
            if e.seconds > 3600:
                logger.critical("Account appears to be banned or severely rate limited (%s seconds). Stopping.", e.seconds)
                raise TelegramError(f"Account likely banned: {e.seconds} seconds wait")
            
            logger.warning("Rate limited, waiting %s seconds", e.seconds)
            raise TelegramError(f"Rate limited: {e.seconds} seconds") from e

        except ConnectionError as e:
            logger.critical("Connection error - possible account block: %s", e)
            raise TelegramError(f"Connection error (possible block): {e}") from e

        except Exception as e:
            error_str = str(e).lower()
            if any(term in error_str for term in ["banned", "blocked", "deactivated", "auth key"]):
                logger.critical("Critical Telegram error - account may be banned: %s", e)
                raise TelegramError(f"Account likely banned/blocked: {e}") from e
            
            logger.error("Telegram execution failed: %s", e)
            return ExecutionResult(
                job_id=job.job_id,
                session_id=job.session_id,
                statement_id=job.statement_id,
                bot_response=None,
                success=False,
                error_message=str(e),
            )

    async def _execute_job(self, job: ExecutionJob) -> ExecutionResult:
        entity = await self._client.get_entity(job.target_bot_username)

        sent_msg = await self._client.send_message(entity, job.content)

        if not sent_msg or not sent_msg.id:
            logger.error("Failed to get ID of sent message.")
            return ExecutionResult(
                job_id=job.job_id,
                session_id=job.session_id,
                statement_id=job.statement_id,
                bot_response=None,
                success=False,
                error_message="Failed to track sent message ID.",
            )

        last_id = sent_msg.id
        logger.info(
            "Job %s: Sent message ID %s. Waiting for reply from bot...",
            job.job_id, last_id
        )

        bot_response_text = await self._wait_for_reply(
            entity, last_id, job.job_id, timeout=60
        )

        if bot_response_text:
            logger.info(
                "Job %s: Successfully captured response: %s...",
                job.job_id, bot_response_text[:50]
            )
        else:
            logger.warning(
                "Job %s: No response received from bot within timeout.",
                job.job_id
            )

        return ExecutionResult(
            job_id=job.job_id,
            session_id=job.session_id,
            statement_id=job.statement_id,
            bot_response=bot_response_text,
            success=True,
        )

    async def _wait_for_reply(self, entity, last_id, job_id, timeout=60):
        start_time = time.time()
        check_count = 0

        while time.time() - start_time < timeout:
            check_count += 1
            try:
                async for msg in self._client.iter_messages(entity, limit=10):
                    if msg.id <= last_id:
                        continue

                    if not msg.out:
                        logger.debug(
                            "Job %s: Found response! Msg ID: %s, text='%s...'",
                            job_id, msg.id, msg.text[:20]
                        )
                        return msg.text or ""
            except Exception as e:
                logger.error("Job %s: Error checking messages: %s", job_id, e)

            if check_count % 10 == 0:
                logger.debug(
                    "Job %s: Attempt %d, waiting for reply (last_id=%s)...",
                    job_id, check_count, last_id
                )
            await asyncio.sleep(2)

        logger.warning(
            "Job %s: Timeout reached after %d attempts. Last known ID was %s.",
            job_id, check_count, last_id
        )
        return None

