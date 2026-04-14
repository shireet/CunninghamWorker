import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass
class Settings:
    app_env: str = "development"
    log_level: str = "INFO"
    telegram_api_id: int = 0
    telegram_api_hash: str = ""
    telegram_phone: str = ""
    telegram_session_string: str = ""
    rabbitmq_host: str = "localhost"
    rabbitmq_port: int = 5672
    rabbitmq_username: str = "guest"
    rabbitmq_password: str = "guest"
    rabbitmq_queue_name: str = "execution_jobs"
    core_api_base_url: str = "http://localhost:8080"
    core_api_timeout_seconds: int = 120
    worker_poll_timeout_seconds: int = 10
    max_retries: int = 3
    retry_delay_seconds: int = 5
    max_concurrent_jobs: int = 10
    telegram_rate_limit: int = 30

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()

        return cls(
            app_env=os.getenv("APP_ENV", "development"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            telegram_api_id=int(os.getenv("TELEGRAM_API_ID", "0")),
            telegram_api_hash=os.getenv("TELEGRAM_API_HASH", ""),
            telegram_phone=os.getenv("TELEGRAM_PHONE", ""),
            telegram_session_string=os.getenv("TELEGRAM_SESSION_STRING", ""),
            rabbitmq_host=os.getenv("RABBITMQ_HOST", "localhost"),
            rabbitmq_port=int(os.getenv("RABBITMQ_PORT", "5672")),
            rabbitmq_username=os.getenv("RABBITMQ_USERNAME", "guest"),
            rabbitmq_password=os.getenv("RABBITMQ_PASSWORD", "guest"),
            rabbitmq_queue_name=os.getenv("RABBITMQ_QUEUE_NAME", "execution_jobs"),
            core_api_base_url=os.getenv("CORE_API_BASE_URL", "http://localhost:8080"),
            core_api_timeout_seconds=int(os.getenv("CORE_API_TIMEOUT_SECONDS", "120")),
            worker_poll_timeout_seconds=int(os.getenv("WORKER_POLL_TIMEOUT_SECONDS", "10")),
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            retry_delay_seconds=int(os.getenv("RETRY_DELAY_SECONDS", "5")),
            max_concurrent_jobs=int(os.getenv("MAX_CONCURRENT_JOBS", "10")),
            telegram_rate_limit=int(os.getenv("TELEGRAM_RATE_LIMIT", "30")),
        )
