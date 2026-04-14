class DomainError(Exception):
    pass


class TelegramError(DomainError):
    def __init__(self, message: str) -> None:
        super().__init__(f"Telegram error: {message}")
        self.message = message
