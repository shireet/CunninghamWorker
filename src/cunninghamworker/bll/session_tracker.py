import asyncio
import logging
from collections import defaultdict
from typing import Callable, Dict, Set

logger = logging.getLogger(__name__)


class SessionTracker:

    def __init__(self) -> None:
        self._active_jobs: Dict[str, Set[str]] = defaultdict(set)
        self._total_jobs: Dict[str, int] = defaultdict(int)
        self._on_session_complete: Callable | None = None
        self._lock = asyncio.Lock()

    def set_session_complete_callback(self, callback: Callable) -> None:
        self._on_session_complete = callback

    async def register_session(self, session_id: str, job_id: str, total_jobs: int) -> None:
        async with self._lock:
            if session_id not in self._total_jobs:
                self._total_jobs[session_id] = total_jobs
                logger.info("Session %s registered with %s total jobs", session_id, total_jobs)

            self._active_jobs[session_id].add(job_id)
            logger.debug("Job %s registered for session %s", job_id, session_id)

    async def mark_job_complete(self, session_id: str, job_id: str) -> None:
        async with self._lock:
            if session_id not in self._active_jobs:
                logger.warning("Session %s not found in tracker", session_id)
                return

            self._active_jobs[session_id].discard(job_id)

            remaining = len(self._active_jobs[session_id])
            total = self._total_jobs.get(session_id, 0)
            completed = total - remaining

            logger.info(
                "Job %s complete for session %s (%s/%s done, %s remaining)",
                job_id, session_id, completed, total, remaining
            )

            if remaining == 0 and total > 0:
                logger.info("Session %s fully complete! (%s/%s jobs)", session_id, total, total)
                await self._trigger_session_complete(session_id)

    async def _trigger_session_complete(self, session_id: str) -> None:
        if self._on_session_complete:
            try:
                logger.info("Calling session complete callback for %s", session_id)
                await self._on_session_complete(session_id)
            except Exception as e:
                logger.error("Session complete callback failed for %s: %s", session_id, e)
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
                "is_complete": remaining == 0 and total > 0,
            }

    async def cleanup_session(self, session_id: str) -> None:
        async with self._lock:
            self._active_jobs.pop(session_id, None)
            self._total_jobs.pop(session_id, None)
            logger.info("Session %s cleaned up from tracker", session_id)
