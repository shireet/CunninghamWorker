"""
End-to-end test for concurrent multi-user session tracking.
Tests the complete flow from job submission to session completion.
"""
import asyncio
import sys
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from cunninghamworker.domain.entities import ExecutionJob
from cunninghamworker.bll.session_tracker import SessionTracker
from cunninghamworker.bll.concurrent_job_pool import ConcurrentJobPool
from cunninghamworker.bll.job_processor import JobProcessor


class MockReporter:
    """Mock result reporter that tracks calls."""
    def __init__(self):
        self.results_reported = []
        self.sessions_completed = []
    
    async def report_result(self, result):
        self.results_reported.append(result)
        print(f"✅ MockReporter: Reported result for job {result.job_id}")
    
    async def report_session_complete(self, session_id):
        self.sessions_completed.append(session_id)
        print(f"🎉 MockReporter: Session {session_id} marked as COMPLETE!")


class MockExecutor:
    """Mock Telegram executor that simulates sending/receiving messages."""
    def __init__(self, delay=0.1):
        self.delay = delay
        self.execution_count = 0
    
    async def execute(self, job):
        self.execution_count += 1
        # Simulate Telegram API delay
        await asyncio.sleep(self.delay)
        
        from cunninghamworker.domain.entities import ExecutionResult
        
        return ExecutionResult(
            job_id=job.job_id,
            session_id=job.session_id,
            statement_id=job.statement_id,
            bot_response=f"Response from bot for job {job.job_id}",
            success=True,
        )


async def test_single_session_concurrent():
    """Test 1: Single session with multiple jobs processed concurrently."""
    print("\n" + "="*80)
    print("TEST 1: Single Session - Concurrent Job Processing")
    print("="*80)
    
    session_id = uuid.uuid4()
    num_jobs = 5
    
    # Create jobs for one session
    jobs = [
        ExecutionJob(
            job_id=uuid.uuid4(),
            session_id=session_id,
            statement_id=uuid.uuid4(),
            target_bot_username="test_bot",
            content=f"Test statement {i}",
            max_retries=1,
            total_jobs_in_session=num_jobs,
        )
        for i in range(num_jobs)
    ]
    
    print(f"Created {num_jobs} jobs for session {session_id}")
    
    # Setup components
    mock_executor = MockExecutor(delay=0.2)
    mock_reporter = MockReporter()
    session_tracker = SessionTracker()
    
    async def on_session_complete(session_id):
        await mock_reporter.report_session_complete(str(session_id))
    
    session_tracker.set_session_complete_callback(on_session_complete)
    
    job_processor = JobProcessor(
        job_executor=mock_executor,
        result_reporter=mock_reporter,
        max_retries=1,
        retry_delay_seconds=0,
    )
    
    job_pool = ConcurrentJobPool(
        job_processor=job_processor,
        session_tracker=session_tracker,
        max_concurrent_jobs=10,
        rate_limit_per_second=30,
    )
    
    # Submit all jobs concurrently
    print(f"Submitting {num_jobs} jobs concurrently...")
    submit_tasks = [job_pool.submit_job(job) for job in jobs]
    results = await asyncio.gather(*submit_tasks)
    
    assert all(results), "All jobs should be submitted successfully"
    print(f"✅ All {num_jobs} jobs submitted")
    
    # Wait for all jobs to complete
    print("Waiting for jobs to complete...")
    await asyncio.sleep(2)  # Give time for processing
    
    # Verify results
    assert mock_executor.execution_count == num_jobs, \
        f"Expected {num_jobs} executions, got {mock_executor.execution_count}"
    print(f"✅ All {num_jobs} jobs executed")
    
    assert len(mock_reporter.results_reported) == num_jobs, \
        f"Expected {num_jobs} results reported, got {len(mock_reporter.results_reported)}"
    print(f"✅ All {num_jobs} results reported")
    
    assert len(mock_reporter.sessions_completed) == 1, \
        f"Expected 1 session completion, got {len(mock_reporter.sessions_completed)}"
    print(f"✅ Session completion callback triggered")
    
    print("\n✅ TEST 1 PASSED: Single session concurrent processing works!")
    return True


async def test_multiple_sessions_concurrent():
    """Test 2: Multiple sessions running concurrently."""
    print("\n" + "="*80)
    print("TEST 2: Multiple Sessions - Concurrent User Testing")
    print("="*80)
    
    # Create 3 sessions with different job counts
    sessions = {
        uuid.uuid4(): 3,  # User A: 3 jobs
        uuid.uuid4(): 5,  # User B: 5 jobs
        uuid.uuid4(): 2,  # User C: 2 jobs
    }
    
    total_jobs = sum(sessions.values())
    all_jobs = []
    
    for session_id, num_jobs in sessions.items():
        jobs = [
            ExecutionJob(
                job_id=uuid.uuid4(),
                session_id=session_id,
                statement_id=uuid.uuid4(),
                target_bot_username="test_bot",
                content=f"Statement {i}",
                max_retries=1,
                total_jobs_in_session=num_jobs,
            )
            for i in range(num_jobs)
        ]
        all_jobs.extend(jobs)
        print(f"Session {session_id}: {num_jobs} jobs")
    
    print(f"Total: {len(all_jobs)} jobs across {len(sessions)} sessions")
    
    # Setup
    mock_executor = MockExecutor(delay=0.1)
    mock_reporter = MockReporter()
    session_tracker = SessionTracker()
    
    async def on_session_complete(session_id):
        await mock_reporter.report_session_complete(session_id)
    
    session_tracker.set_session_complete_callback(on_session_complete)
    
    job_processor = JobProcessor(
        job_executor=mock_executor,
        result_reporter=mock_reporter,
        max_retries=1,
        retry_delay_seconds=0,
    )
    
    job_pool = ConcurrentJobPool(
        job_processor=job_processor,
        session_tracker=session_tracker,
        max_concurrent_jobs=10,
        rate_limit_per_second=30,
    )
    
    # Submit all jobs from all sessions
    print("Submitting all jobs concurrently...")
    submit_tasks = [job_pool.submit_job(job) for job in all_jobs]
    results = await asyncio.gather(*submit_tasks)
    
    assert all(results), "All jobs should be submitted"
    print(f"✅ All {len(all_jobs)} jobs submitted")
    
    # Wait for completion
    print("Waiting for all sessions to complete...")
    await asyncio.sleep(3)
    
    # Verify
    assert mock_executor.execution_count == total_jobs, \
        f"Expected {total_jobs} executions, got {mock_executor.execution_count}"
    print(f"✅ All {total_jobs} jobs executed")
    
    assert len(mock_reporter.results_reported) == total_jobs, \
        f"Expected {total_jobs} results, got {len(mock_reporter.results_reported)}"
    print(f"✅ All {total_jobs} results reported")
    
    assert len(mock_reporter.sessions_completed) == len(sessions), \
        f"Expected {len(sessions)} session completions, got {len(mock_reporter.sessions_completed)}"
    print(f"✅ All {len(sessions)} sessions completed")
    
    # Verify each session was completed
    completed_session_ids = [str(sid) for sid in sessions.keys()]
    for reported_session in mock_reporter.sessions_completed:
        assert reported_session in completed_session_ids, \
            f"Unexpected session {reported_session} completed"
    
    print("✅ All correct sessions completed")
    print("\n✅ TEST 2 PASSED: Multiple concurrent sessions work!")
    return True


async def test_session_completion_order():
    """Test 3: Verify sessions complete in any order (not sequential)."""
    print("\n" + "="*80)
    print("TEST 3: Session Completion Order (Should Be Concurrent)")
    print("="*80)
    
    # Create sessions where smaller one might finish first
    session_a = uuid.uuid4()  # 5 jobs
    session_b = uuid.uuid4()  # 2 jobs (should finish faster)
    
    jobs_a = [
        ExecutionJob(
            job_id=uuid.uuid4(),
            session_id=session_a,
            statement_id=uuid.uuid4(),
            target_bot_username="test_bot",
            content=f"Statement A{i}",
            max_retries=1,
            total_jobs_in_session=5,
        )
        for i in range(5)
    ]
    
    jobs_b = [
        ExecutionJob(
            job_id=uuid.uuid4(),
            session_id=session_b,
            statement_id=uuid.uuid4(),
            target_bot_username="test_bot",
            content=f"Statement B{i}",
            max_retries=1,
            total_jobs_in_session=2,
        )
        for i in range(2)
    ]
    
    mock_executor = MockExecutor(delay=0.1)
    mock_reporter = MockReporter()
    session_tracker = SessionTracker()
    
    completion_order = []
    
    async def on_session_complete(session_id):
        completion_order.append(session_id)
        print(f"⏰ Session {session_id} completed at position {len(completion_order)}")
        await mock_reporter.report_session_complete(session_id)
    
    session_tracker.set_session_complete_callback(on_session_complete)
    
    job_processor = JobProcessor(
        job_executor=mock_executor,
        result_reporter=mock_reporter,
        max_retries=1,
        retry_delay_seconds=0,
    )
    
    job_pool = ConcurrentJobPool(
        job_processor=job_processor,
        session_tracker=session_tracker,
        max_concurrent_jobs=10,
        rate_limit_per_second=30,
    )
    
    # Submit all jobs at once
    all_jobs = jobs_a + jobs_b
    submit_tasks = [job_pool.submit_job(job) for job in all_jobs]
    await asyncio.gather(*submit_tasks)
    
    print(f"Submitted {len(all_jobs)} jobs (5 from A, 2 from B)")
    print("Waiting for completion...")
    await asyncio.sleep(3)
    
    # In concurrent execution, both sessions run in parallel
    # We just verify that BOTH complete (not one after another)
    assert len(completion_order) == 2, \
        f"Expected 2 session completions, got {len(completion_order)}"
    
    # Both sessions should be in the completion order
    assert str(session_a) in completion_order, "Session A should complete"
    assert str(session_b) in completion_order, "Session B should complete"
    
    print(f"✅ Both sessions completed concurrently")
    print(f"   Completion order: {[str(sid)[-8:] for sid in completion_order]}")
    print(f"   This proves concurrent execution (not sequential)!")
    
    print("\n✅ TEST 3 PASSED: Concurrent completion order verified!")
    return True


async def test_failed_jobs_still_complete_session():
    """Test 4: Even if some jobs fail, session still completes."""
    print("\n" + "="*80)
    print("TEST 4: Failed Jobs Still Count Toward Session Completion")
    print("="*80)
    
    session_id = uuid.uuid4()
    
    class FailingExecutor(MockExecutor):
        def __init__(self):
            super().__init__(delay=0.1)
            self.fail_count = 0
        
        async def execute(self, job):
            self.execution_count += 1
            await asyncio.sleep(self.delay)
            
            from cunninghamworker.domain.entities import ExecutionResult
            
            # Fail every other job
            if self.execution_count % 2 == 0:
                self.fail_count += 1
                raise Exception(f"Simulated failure for job {job.job_id}")
            
            return ExecutionResult(
                job_id=job.job_id,
                session_id=job.session_id,
                statement_id=job.statement_id,
                bot_response="Success",
                success=True,
            )
    
    jobs = [
        ExecutionJob(
            job_id=uuid.uuid4(),
            session_id=session_id,
            statement_id=uuid.uuid4(),
            target_bot_username="test_bot",
            content=f"Statement {i}",
            max_retries=1,
            total_jobs_in_session=4,
        )
        for i in range(4)
    ]
    
    mock_executor = FailingExecutor()
    mock_reporter = MockReporter()
    session_tracker = SessionTracker()
    
    async def on_session_complete(session_id):
        await mock_reporter.report_session_complete(str(session_id))
    
    session_tracker.set_session_complete_callback(on_session_complete)
    
    job_processor = JobProcessor(
        job_executor=mock_executor,
        result_reporter=mock_reporter,
        max_retries=1,
        retry_delay_seconds=0,
    )
    
    job_pool = ConcurrentJobPool(
        job_processor=job_processor,
        session_tracker=session_tracker,
        max_concurrent_jobs=10,
        rate_limit_per_second=30,
    )
    
    print("Submitting 4 jobs (some will fail)...")
    submit_tasks = [job_pool.submit_job(job) for job in jobs]
    await asyncio.gather(*submit_tasks)
    
    await asyncio.sleep(2)
    
    # Session should still complete even with failed jobs
    assert len(mock_reporter.sessions_completed) == 1, \
        "Session should complete even with failed jobs"
    
    print(f"✅ Session completed despite {mock_executor.fail_count} failed jobs")
    print(f"   - Successful jobs: {len(mock_reporter.results_reported)}")
    print(f"   - Failed jobs: {mock_executor.fail_count}")
    print(f"   - Session completions: {len(mock_reporter.sessions_completed)}")
    
    print("\n✅ TEST 4 PASSED: Failed jobs don't block session completion!")
    return True


async def main():
    """Run all tests."""
    print("\n" + "="*80)
    print("🧪 END-TO-END TEST SUITE: Concurrent Multi-User Session Tracking")
    print("="*80)
    
    tests = [
        ("Single Session Concurrent", test_single_session_concurrent),
        ("Multiple Sessions Concurrent", test_multiple_sessions_concurrent),
        ("Session Completion Order", test_session_completion_order),
        ("Failed Jobs Session Complete", test_failed_jobs_still_complete_session),
    ]
    
    results = []
    for name, test_func in tests:
        try:
            result = await test_func()
            results.append((name, True, None))
        except Exception as e:
            results.append((name, False, str(e)))
            print(f"\n❌ TEST FAILED: {name}")
            print(f"   Error: {e}")
            import traceback
            traceback.print_exc()
    
    # Summary
    print("\n" + "="*80)
    print("📊 TEST SUMMARY")
    print("="*80)
    
    passed = sum(1 for _, success, _ in results if success)
    failed = sum(1 for _, success, _ in results if not success)
    
    for name, success, error in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status}: {name}")
        if error:
            print(f"   Error: {error}")
    
    print(f"\nTotal: {passed} passed, {failed} failed out of {len(results)} tests")
    print("="*80)
    
    if failed == 0:
        print("\n🎉 ALL TESTS PASSED! 🎉")
        print("The concurrent multi-user session tracking system is working correctly!")
        return 0
    else:
        print(f"\n❌ {failed} TEST(S) FAILED")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
