import asyncio
import logging
import signal

from cunninghamworker.bll.config import Settings
from cunninghamworker.bll.job_processor import JobProcessor
from cunninghamworker.bll.concurrent_job_pool import ConcurrentJobPool
from cunninghamworker.bll.db_backed_session_tracker import DBBackedSessionTracker
from cunninghamworker.infrastructure.core_api_reporter import CoreApiResultReporter
from cunninghamworker.infrastructure.logging import configure_logging
from cunninghamworker.infrastructure.rabbitmq_consumer import RabbitMqJobConsumer
from cunninghamworker.infrastructure.telethon_executor import TelethonJobExecutor

logger = logging.getLogger(__name__)


async def main() -> None:
    settings = Settings.from_env()
    configure_logging(settings.log_level)

    executor = TelethonJobExecutor(settings)
    consumer = RabbitMqJobConsumer(settings)
    reporter = CoreApiResultReporter(settings)

    processor = JobProcessor(
        job_executor=executor,
        result_reporter=reporter,
        max_retries=settings.max_retries,
        retry_delay_seconds=settings.retry_delay_seconds,
    )

    shutdown_event = asyncio.Event()

    def handle_signal() -> None:
        logger.info("Shutdown signal received")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_signal)

    try:
        await executor.start()
        await consumer.connect()

        session_tracker = DBBackedSessionTracker()

        async def on_session_complete(session_id: str):
            logger.info("Session %s complete - notifying Core API", session_id)
            await reporter.report_session_complete(session_id)
            await executor.cleanup_session_lock(session_id)

        session_tracker.set_session_complete_callback(on_session_complete)

        job_pool = ConcurrentJobPool(
            job_processor=processor,
            session_tracker=session_tracker,
            max_concurrent_jobs=settings.max_concurrent_jobs,
            rate_limit_per_second=settings.telegram_rate_limit,
        )

        logger.info(
            "Worker started with concurrent job processing "
            "(max_concurrent=%s, rate_limit=%s/s, crash_recovery=enabled)",
            settings.max_concurrent_jobs,
            settings.telegram_rate_limit,
        )

        monitor_task = asyncio.create_task(
            _monitor_pool(job_pool, shutdown_event)
        )

        while not shutdown_event.is_set():
            job = await consumer.consume_job()

            if job is None:
                if job_pool.is_idle:
                    await asyncio.sleep(1)
                continue

            submitted = await job_pool.submit_job(job)
            if not submitted:
                logger.warning("Failed to submit job %s to pool", job.job_id)

        logger.info("Waiting for active jobs to complete...")
        await job_pool.shutdown(timeout=30.0)

    except KeyboardInterrupt:
        logger.info("Worker interrupted")
    except Exception as e:
        logger.exception("Worker error: %s", e)
    finally:
        logger.info("Shutting down worker...")
        if 'monitor_task' in locals():
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass
        await executor.stop()
        await consumer.disconnect()
        await reporter.close()
        logger.info("Worker stopped")


async def _monitor_pool(job_pool: ConcurrentJobPool, shutdown_event: asyncio.Event) -> None:
    while not shutdown_event.is_set():
        await asyncio.sleep(30)
        if not shutdown_event.is_set():
            logger.info(
                "Pool status: %s active jobs", job_pool.active_count
            )


def run_main() -> None:
    asyncio.run(main())


if __name__ == "__main__":
    run_main()
