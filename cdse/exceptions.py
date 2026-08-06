"""Исключения модуля поиска и загрузки данных."""
class CdseError(Exception):
    """Базовая ошибка CDSE downloader."""


class CdseAuthError(CdseError):
    """Ошибка получения или обновления токена."""


class CdseQueryError(CdseError):
    """Ошибка OData-запроса."""

    def __init__(
            self,
            message: str,
            *,
            status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code


class CdseDownloadError(CdseError):
    """Ошибка скачивания продукта."""


class CdseConfigError(CdseError):
    """Ошибка конфигурации."""
