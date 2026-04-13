from dataclasses import dataclass
from uuid import UUID


@dataclass
class ExecutionJob:
    job_id: UUID
    session_id: UUID
    statement_id: UUID
    target_bot_username: str
    content: str
    max_retries: int = 3
    total_jobs_in_session: int = 1


@dataclass
class ExecutionResult:
    job_id: UUID
    session_id: UUID
    statement_id: UUID
    bot_response: str | None
    success: bool
    error_message: str | None = None
