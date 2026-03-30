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
                logger.info(f"Processing job {job.job_id} (attempt {attempt + 1}/{self._max_retries})")

                result = await self._executor.execute(job)

                await self._reporter.report_result(result)

                if result.success:
                    logger.info(f"Job {job.job_id} completed successfully")
                else:
                    logger.warning(f"Job {job.job_id} failed: {result.error_message}")

                return

            except Exception as e:
                attempt += 1
                logger.error(f"Job {job.job_id} attempt {attempt} failed: {e}")

                if attempt < self._max_retries:
                    logger.info(f"Retrying job {job.job_id} in {self._retry_delay} seconds...")
                    await asyncio.sleep(self._retry_delay)
                else:
                    logger.error(f"Job {job.job_id} failed after {self._max_retries} attempts")
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
