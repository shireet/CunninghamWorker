import asyncio
import logging
from cunninghamworker.bll.exceptions import JobExecutionError
from cunninghamworker.bll.interfaces import IJobExecutor, IResultReporter
from cunninghamworker.domain.entities import ExecutionJob, ExecutionResult

logger = logging.getLogger(__name__)


class JobProcessor:
    def __init__(
        self,
        job_executor: IJobExecutor,
        result_reporter: IResultReporter,
        max_retries: int = 3,
        retry_delay_seconds: int = 5,
    ) -> None:
        self._executor = job_executor
        self._reporter = result_reporter
        self._max_retries = max_retries
        self._retry_delay = retry_delay_seconds

    async def process_job(self, job: ExecutionJob) -> None:
        attempt = 0
        last_error = None
        failure_reason = "Unknown"

        while attempt < self._max_retries:
            try:
                logger.info("Processing job %s (attempt %s/%s)", job.job_id, attempt + 1, self._max_retries)

                result = await self._executor.execute(job)

                await self._reporter.report_result(result)

                if result.success:
                    logger.info("Job %s completed successfully", job.job_id)
                else:
                    logger.warning("Job %s failed: %s", job.job_id, result.error_message)
                    last_error = result.error_message
                    failure_reason = self._classify_failure_reason(result)

                return

            except Exception as e:
                attempt += 1
                last_error = str(e)
                logger.error("Job %s attempt %s failed: %s", job.job_id, attempt, e)

                if attempt < self._max_retries:
                    logger.info("Retrying job %s in %s seconds...", job.job_id, self._retry_delay)
                    await asyncio.sleep(self._retry_delay)
                else:
                    logger.error("Job %s failed after %s attempts", job.job_id, self._max_retries)
                    await self._reporter.report_result(
                        ExecutionResult(
                            job_id=job.job_id,
                            session_id=job.session_id,
                            statement_id=job.statement_id,
                            bot_response=None,
                            success=False,
                            error_message=f"Failed after {self._max_retries} attempts: {last_error}",
                        )
                    )
                    await self._reporter.mark_job_failed(
                        job_id=job.job_id,
                        error_message=last_error,
                        reason=failure_reason,
                        attempt_count=attempt,
                    )

    def _classify_failure_reason(self, result: ExecutionResult) -> str:
        if result.error_message is None:
            return "Unknown"
        error_lower = result.error_message.lower()
        if "timeout" in error_lower:
            return "BotTimeout"
        if "rate" in error_lower or "limit" in error_lower:
            return "RateLimited"
        if "banned" in error_lower or "blocked" in error_lower:
            return "AccountBanned"
        if "deleted" in error_lower or "not found" in error_lower:
            return "MessageDeleted"
        if "telegram" in error_lower:
            return "TelegramError"
        return "MaxRetriesExceeded"
