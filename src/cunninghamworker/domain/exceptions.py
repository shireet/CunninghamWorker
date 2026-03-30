class DomainError(Exception):
    pass


class ExecutionError(DomainError):
    def __init__(self, message: str) -> None:
        super().__init__(f"Execution error: {message}")
        self.message = message


class TelegramError(DomainError):
    def __init__(self, message: str) -> None:
        super().__init__(f"Telegram error: {message}")
        self.message = message
