import asyncio
import logging
from collections import defaultdict
from typing import Any, Callable, Dict, Set

logger = logging.getLogger(__name__)


class DBBackedSessionTracker:

    def __init__(self, core_api_reporter) -> None:
        self._active_jobs: Dict[str, Set[str]] = defaultdict(set)
        self._total_jobs: Dict[str, int] = defaultdict(int)
        self._on_session_complete: Callable | None = None
        self._lock = asyncio.Lock()
        self._reporter = core_api_reporter
        self._completed_sessions: Set[str] = set()

    def set_session_complete_callback(self, callback: Callable) -> None:
        self._on_session_complete = callback

    async def rebuild_from_db(self, session_id: str) -> bool:
        try:
            logger.info("Rebuilding session %s state from DB", session_id)

            status = await self._reporter.get_session_status(session_id)

            if status is None:
                logger.error("Failed to get status for session %s", session_id)
                return False

            if status.get("is_complete", False):
                logger.info("Session %s is already complete in DB", session_id)
                self._completed_sessions.add(session_id)
                return True

            total_jobs = status.get("totalJobs", 0)
            completed_job_ids = set(status.get("completedJobIds", []))
            pending_jobs = total_jobs - len(completed_job_ids)

            self._total_jobs[session_id] = total_jobs

            self._active_jobs[session_id] = set()

            logger.info(
                "Session %s rebuilt from DB: %s/%s completed, %s pending",
                session_id, status.get('completedJobs', 0), total_jobs, pending_jobs
            )

            if pending_jobs == 0 and total_jobs > 0:
                logger.info("Session %s was already complete in DB", session_id)
                self._completed_sessions.add(session_id)
                await self._trigger_session_complete(session_id)
                return True

            return False

        except Exception as e:
            logger.error("Failed to rebuild session %s from DB: %s", session_id, e)
            return False

    async def register_session(self, session_id: str, job_id: str, total_jobs: int) -> None:
        async with self._lock:
            if session_id in self._completed_sessions:
                logger.debug("Session %s already complete, skipping", session_id)
                return

            if session_id not in self._total_jobs:
                already_complete = await self.rebuild_from_db(session_id)
                if already_complete:
                    return
                self._total_jobs[session_id] = total_jobs
                logger.info("Session %s registered with %s total jobs", session_id, total_jobs)

            self._active_jobs[session_id].add(job_id)
            logger.debug("Job %s registered for session %s", job_id, session_id)

    async def mark_job_complete(self, session_id: str, job_id: str) -> None:
        async with self._lock:
            if session_id in self._completed_sessions:
                logger.debug("Session %s already complete, ignoring job completion", session_id)
                return

            if session_id not in self._active_jobs:
                logger.warning("Session %s not found in tracker", session_id)
                return

            self._active_jobs[session_id].discard(job_id)

            remaining = len(self._active_jobs[session_id])
            total = self._total_jobs.get(session_id, 0)

            try:
                status = await self._reporter.get_session_status(session_id)
                if status:
                    completed = status.get("completedJobs", 0)
                    failed = status.get("failedJobs", 0)
                    total = status.get("totalJobs", self._total_jobs.get(session_id, 0))
                    finished = completed + failed
                    pending = total - finished

                    logger.info(
                        "Job %s complete for session %s (DB: %s/%s done, %s remaining)",
                        job_id, session_id, finished, total, pending
                    )

                    if pending <= 0 and finished > 0:
                        logger.info("Session %s fully complete! (%s/%s jobs)", session_id, finished, total)
                        await self._trigger_session_complete(session_id)
                else:
                    completed = total - remaining
                    logger.info(
                        "Job %s complete for session %s (memory: %s/%s done, %s remaining)",
                        job_id, session_id, completed, total, remaining
                    )

                    if remaining == 0 and total > 0:
                        logger.info("Session %s fully complete! (%s/%s jobs)", session_id, total, total)
                        await self._trigger_session_complete(session_id)
            except Exception as e:
                logger.error("Error checking session status for %s: %s", session_id, e)
                if remaining == 0 and total > 0:
                    logger.info("Session %s fully complete! (%s/%s jobs)", session_id, total, total)
                    await self._trigger_session_complete(session_id)

    async def _trigger_session_complete(self, session_id: str) -> None:
        if session_id in self._completed_sessions:
            logger.debug("Session %s already marked complete", session_id)
            return

        logger.info("Session %s fully complete! Triggering completion...", session_id)
        self._completed_sessions.add(session_id)

        if self._on_session_complete:
            try:
                logger.info("Calling session complete callback for %s", session_id)
                await self._on_session_complete(session_id)
                logger.info("Session %s completion callback executed successfully", session_id)
            except Exception as e:
                logger.error("Session complete callback failed for %s: %s", session_id, e, exc_info=True)
        else:
            logger.warning("No callback set for session completion %s", session_id)

    async def get_session_status(self, session_id: str) -> dict:
        async with self._lock:
            total = self._total_jobs.get(session_id, 0)
            remaining = len(self._active_jobs.get(session_id, set()))
            completed = total - remaining

            return {
                "session_id": session_id,
                "total_jobs": total,
                "completed_jobs": completed,
                "remaining_jobs": remaining,
                "is_complete": session_id in self._completed_sessions,
            }

    async def cleanup_session(self, session_id: str) -> None:
        async with self._lock:
            self._active_jobs.pop(session_id, None)
            self._total_jobs.pop(session_id, None)
            logger.info("Session %s cleaned up from tracker", session_id)
