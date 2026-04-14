import asyncio
import logging
from collections import defaultdict
from typing import Awaitable, Callable, Dict, Set

logger = logging.getLogger(__name__)


class DBBackedSessionTracker:
    def __init__(self) -> None:
        self._active_jobs: Dict[str, Set[str]] = defaultdict(set)
        self._total_jobs: Dict[str, int] = defaultdict(int)
        self._on_session_complete: Callable[[str], Awaitable[None]] | None = None
        self._lock = asyncio.Lock()
        self._completed_sessions: Set[str] = set()
        self._completing_sessions: Set[str] = set()

    def set_session_complete_callback(self, callback: Callable[[str], Awaitable[None]]) -> None:
        self._on_session_complete = callback

    async def register_session(self, session_id: str, job_id: str, total_jobs: int) -> None:
        async with self._lock:
            if session_id in self._completed_sessions:
                logger.debug("Session %s already complete, skipping", session_id)
                return

            self._total_jobs.setdefault(session_id, total_jobs)
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

        completed = total - remaining
        logger.info(
            "Job %s complete for session %s (%s/%s done, %s remaining)",
            job_id, session_id, completed, total, remaining,
        )

        if remaining == 0 and total > 0:
            logger.info("Session %s fully complete!", session_id)
            await self._trigger_session_complete(session_id)

    async def _trigger_session_complete(self, session_id: str) -> None:
        async with self._lock:
            if session_id in self._completed_sessions:
                logger.debug("Session %s already marked complete", session_id)
                return
            if session_id in self._completing_sessions:
                logger.debug("Session %s completion already in progress", session_id)
                return
            self._completing_sessions.add(session_id)

        try:
            if self._on_session_complete:
                await self._on_session_complete(session_id)
                async with self._lock:
                    self._completed_sessions.add(session_id)
            else:
                logger.warning("No callback set for session completion %s", session_id)
        except Exception as exc:
            logger.error("Session complete callback failed for %s: %s", session_id, exc, exc_info=True)
        finally:
            async with self._lock:
                self._completing_sessions.discard(session_id)
