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

        while attempt < self._max_retries:
            try:
                logger.info("Processing job %s (attempt %s/%s)", job.job_id, attempt + 1, self._max_retries)

                result = await self._executor.execute(job)

                await self._reporter.report_result(result)

                if result.success:
                    logger.info("Job %s completed successfully", job.job_id)
                else:
                    logger.warning("Job %s failed: %s", job.job_id, result.error_message)

                return

            except Exception as e:
                attempt += 1
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
                            error_message=f"Failed after {self._max_retries} attempts: {str(e)}",
                        )
                    )
