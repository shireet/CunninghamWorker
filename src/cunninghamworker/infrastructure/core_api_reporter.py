import asyncio
import logging

import httpx

from cunninghamworker.bll.config import Settings
from cunninghamworker.bll.interfaces import IResultReporter
from cunninghamworker.domain.entities import ExecutionResult

logger = logging.getLogger(__name__)


class CoreApiResultReporter(IResultReporter):
    def __init__(self, settings: Settings) -> None:
        self._client = httpx.AsyncClient(
            base_url=settings.core_api_base_url,
            timeout=settings.core_api_timeout_seconds,
        )
        self._max_retries = 3
        self._retry_delay = 2

    async def close(self) -> None:
        await self._client.aclose()

    async def _retry_post(self, url: str, json: dict, operation: str) -> None:
        last_error = None
        delay = self._retry_delay
        for attempt in range(1, self._max_retries + 1):
            try:
                response = await self._client.post(url, json=json)
                response.raise_for_status()
                logger.info("%s: HTTP %s", operation, response.status_code)
                return
            except Exception as e:
                last_error = e
                if attempt < self._max_retries:
                    logger.warning("%s attempt %d failed, retrying in %ds: %s",
                                   operation, attempt, delay, e)
                    await asyncio.sleep(delay)
                    delay *= 2
        raise RuntimeError(f"{operation} failed after {self._max_retries} attempts: {last_error}")

    async def report_result(self, result: ExecutionResult) -> None:
        await self._retry_post(
            "/api/v1/execution/exchange/complete",
            json={
                "session_id": str(result.session_id),
                "statement_id": str(result.statement_id),
                "bot_response": result.bot_response,
            },
            operation=f"Report result for job {result.job_id}",
        )

    async def report_session_complete(self, session_id: str) -> None:
        logger.info("Reporting session %s as complete", session_id)
        await self._retry_post(
            "/api/v1/execution/session/complete",
            json={"session_id": session_id},
            operation=f"Report session {session_id} complete",
        )

    async def get_session_status(self, session_id: str) -> dict | None:
        try:
            logger.debug("Querying session status for %s", session_id)
            response = await self._client.get(
                f"/api/v1/execution/session/{session_id}/status",
            )
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                logger.warning("Session %s not found", session_id)
                return None
            else:
                logger.error("Failed to get session status: %s", response.status_code)
                return None
        except Exception as e:
            logger.error("Failed to get session status: %s", e)
            return None
