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

    async def report_failed_job(
        self,
        job_id: str,
        session_id: str,
        statement_id: str,
        error_message: str,
        attempt_count: int,
    ) -> None:
        try:
            logger.info("Reporting failed job %s to dead letter queue", job_id)
            await self._client.post(
                "/api/v1/execution/jobs/failed",
                json={
                    "job_id": job_id,
                    "session_id": session_id,
                    "statement_id": statement_id,
                    "error_message": error_message,
                    "attempt_count": attempt_count,
                },
            )
            logger.info("Failed job %s saved to dead letter queue", job_id)
        except Exception as e:
            logger.error("Failed to report failed job: %s", e)
