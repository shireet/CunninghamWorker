import asyncio
import logging
import signal

from cunninghamworker.bll.config import Settings
from cunninghamworker.bll.job_processor import JobProcessor
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

        logger.info("Worker started, waiting for jobs...")

        while not shutdown_event.is_set():
            job = await consumer.consume_job()

            if job is None:
                await asyncio.sleep(1)
                continue

            await processor.process_job(job)

    except KeyboardInterrupt:
        logger.info("Worker interrupted")
    except Exception as e:
        logger.exception(f"Worker error: {e}")
    finally:
        logger.info("Shutting down worker...")
        await executor.stop()
        await consumer.disconnect()
        await reporter.close()
        logger.info("Worker stopped")


def run_main() -> None:
    asyncio.run(main())


if __name__ == "__main__":
    run_main()
