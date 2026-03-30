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
            logger.info(f"Reported result for job {result.job_id}")
        except Exception as e:
            logger.error(f"Failed to report result: {e}")
            raise
