"""Исключения модуля поиска и загрузки данных."""
class CdseError(Exception):
    """Базовая ошибка CDSE downloader."""


class CdseAuthError(CdseError):
    """Ошибка получения или обновления токена."""


class CdseQueryError(CdseError):
    """Ошибка OData-запроса."""


class CdseDownloadError(CdseError):
    """Ошибка скачивания продукта."""


class CdseConfigError(CdseError):
    """Ошибка конфигурации."""