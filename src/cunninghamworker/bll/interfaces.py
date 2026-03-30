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
