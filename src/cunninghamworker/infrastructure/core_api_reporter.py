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

    async def close(self) -> None:
        await self._client.aclose()

    async def report_result(self, result: ExecutionResult) -> None:
        try:
            await self._client.post(
                "/api/v1/execution/exchange/complete",
                json={
                    "session_id": str(result.session_id),
                    "statement_id": str(result.statement_id),
                    "bot_response": result.bot_response,
                },
            )
            logger.info("Reported result for job %s", result.job_id)
        except Exception as e:
            logger.error("Failed to report result: %s", e)
            raise

    async def report_session_complete(self, session_id: str) -> None:
        try:
            logger.info("Reporting session %s as complete", session_id)
            response = await self._client.post(
                "/api/v1/execution/session/complete",
                json={
                    "session_id": session_id,
                },
            )
            logger.info("Session %s marked as complete: %s", session_id, response.status_code)
        except Exception as e:
            logger.error("Failed to report session complete: %s", e)
            raise

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

    async def mark_job_failed(
        self,
        job_id: str,
        error_message: str,
        reason: str,
        attempt_count: int,
    ) -> None:
        try:
            logger.info("Marking job %s as failed with reason: %s", job_id, reason)
            response = await self._client.post(
                f"/api/v1/execution/jobs/{job_id}/fail",
                json={
                    "error_message": error_message,
                    "reason": reason,
                    "attempt_count": attempt_count,
                },
            )
            logger.info("Job %s marked as failed: %s", job_id, response.status_code)
        except Exception as e:
            logger.error("Failed to mark job as failed: %s", e)
