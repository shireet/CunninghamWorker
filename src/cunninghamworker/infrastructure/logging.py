import logging
import sys


def configure_logging(log_level: str) -> None:
    level = getattr(logging, log_level.upper(), logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    logging.getLogger("aio_pika").setLevel(logging.WARNING)
    logging.getLogger("aiormq").setLevel(logging.WARNING)
    logging.getLogger("telethon").setLevel(logging.WARNING)
