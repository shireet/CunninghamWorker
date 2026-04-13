import asyncio
import logging
from typing import Dict, Set

from cunninghamworker.bll.job_processor import JobProcessor
from cunninghamworker.bll.db_backed_session_tracker import DBBackedSessionTracker
from cunninghamworker.domain.entities import ExecutionJob

logger = logging.getLogger(__name__)


class ConcurrentJobPool:

    def __init__(
        self,
        job_processor: JobProcessor,
        session_tracker: DBBackedSessionTracker,
        max_concurrent_jobs: int = 10,
        rate_limit_per_second: int = 30,
    ) -> None:
        self._job_processor = job_processor
        self._session_tracker = session_tracker
        self._max_concurrent_jobs = max_concurrent_jobs
        self._semaphore = asyncio.Semaphore(max_concurrent_jobs)
        self._rate_limiter = asyncio.Semaphore(rate_limit_per_second)
        self._active_tasks: Dict[str, asyncio.Task] = {}
        self._shutdown_event = asyncio.Event()
        self._task_counter = 0

    async def submit_job(self, job: ExecutionJob) -> bool:
        if self._shutdown_event.is_set():
            logger.warning("Job pool is shutting down, rejecting job")
            return False

        if job.job_id in self._active_tasks:
            logger.warning("Job %s is already running", job.job_id)
            return False

        task = asyncio.create_task(self._execute_with_limits(job))
        self._active_tasks[job.job_id] = task
        self._task_counter += 1

        task.add_done_callback(
            lambda t: self._active_tasks.pop(job.job_id, None)
        )

        logger.info(
            "Submitted job %s (active: %s/%s)",
            job.job_id, len(self._active_tasks), self._max_concurrent_jobs
        )
        return True

    async def _execute_with_limits(self, job: ExecutionJob) -> None:
        await self._session_tracker.register_session(
            session_id=str(job.session_id),
            job_id=job.job_id,
            total_jobs=job.total_jobs_in_session,
        )

        async with self._semaphore:
            async with self._rate_limiter:
                try:
                    logger.info(
                        "Executing job %s (pool: %s/%s)",
                        job.job_id, len(self._active_tasks), self._max_concurrent_jobs
                    )
                    await self._job_processor.process_job(job)
                    logger.info("Job %s completed successfully", job.job_id)

                    await self._session_tracker.mark_job_complete(
                        session_id=str(job.session_id),
                        job_id=job.job_id,
                    )
                except Exception as e:
                    logger.error("Job %s failed with exception: %s", job.job_id, e)
                    await self._session_tracker.mark_job_complete(
                        session_id=str(job.session_id),
                        job_id=job.job_id,
                    )
                finally:
                    await asyncio.sleep(1.0 / 30)

    async def wait_for_completion(self, timeout: float = None) -> bool:
        if not self._active_tasks:
            return True

        tasks = list(self._active_tasks.values())
        logger.info("Waiting for %s active jobs to complete...", len(tasks))

        try:
            if timeout:
                await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True),
                    timeout=timeout,
                )
            else:
                await asyncio.gather(*tasks, return_exceptions=True)
            return True
        except asyncio.TimeoutError:
            logger.warning("Timeout waiting for jobs to complete")
            return False

    async def shutdown(self, timeout: float = 30.0) -> None:
        logger.info("Shutting down job pool...")
        self._shutdown_event.set()

        if self._active_tasks:
            logger.info(
                "Waiting for %s tasks to finish...", len(self._active_tasks)
            )
            await self.wait_for_completion(timeout=timeout)

        for job_id, task in self._active_tasks.items():
            if not task.done():
                logger.warning("Cancelling task %s", job_id)
                task.cancel()

        self._active_tasks.clear()
        logger.info("Job pool shut down complete")

    @property
    def active_count(self) -> int:
        return len(self._active_tasks)

    @property
    def is_idle(self) -> bool:
        return len(self._active_tasks) == 0
