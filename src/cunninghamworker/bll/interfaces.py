from abc import ABC, abstractmethod

from cunninghamworker.domain.entities import ExecutionJob, ExecutionResult


class IJobExecutor(ABC):
    @abstractmethod
    async def execute(self, job: ExecutionJob) -> ExecutionResult:
        pass


class IJobConsumer(ABC):
    @abstractmethod
    async def consume_job(self) -> ExecutionJob | None:
        pass


class IResultReporter(ABC):
    @abstractmethod
    async def report_result(self, result: ExecutionResult) -> None:
        pass

    @abstractmethod
    async def report_session_complete(self, session_id: str) -> None:
        pass

    @abstractmethod
    async def mark_job_failed(
        self,
        job_id: str,
        error_message: str,
        reason: str,
        attempt_count: int,
    ) -> None:
        pass
