from uuid import uuid4

from cunninghamworker.domain.entities import ExecutionJob, ExecutionResult


class TestExecutionJob:
    def test_create_job(self) -> None:
        job = ExecutionJob(
            job_id=uuid4(),
            session_id=uuid4(),
            statement_id=uuid4(),
            target_bot_username="@testbot",
            content="Test message",
            max_retries=3,
        )
        assert job.target_bot_username == "@testbot"
        assert job.max_retries == 3


class TestExecutionResult:
    def test_success_result(self) -> None:
        result = ExecutionResult(
            job_id=uuid4(),
            session_id=uuid4(),
            statement_id=uuid4(),
            bot_response="Bot response",
            success=True,
        )
        assert result.success is True
        assert result.bot_response == "Bot response"
        assert result.error_message is None

    def test_failure_result(self) -> None:
        result = ExecutionResult(
            job_id=uuid4(),
            session_id=uuid4(),
            statement_id=uuid4(),
            bot_response=None,
            success=False,
            error_message="Test error",
        )
        assert result.success is False
        assert result.error_message == "Test error"
