"""Конфигурация и создание подключений к PostgreSQL."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class DatabaseConfig:
    """Параметры подключения без логики запросов."""

    dbname: str | None
    user: str | None
    password: str | None
    host: str = "localhost"
    port: int = 5432

    @classmethod
    def from_env(cls) -> DatabaseConfig:
        """Создаёт конфигурацию из переменных окружения."""
        return cls(
            dbname=os.environ.get("DB_NAME"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD"),
            host=os.environ.get("DB_HOST", "localhost"),
            port=int(os.environ.get("DB_PORT", "5432")),
        )

    def as_psycopg_kwargs(self) -> dict[str, Any]:
        """Преобразует конфигурацию в аргументы ``psycopg2.connect``."""
        return {
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
            "host": self.host,
            "port": self.port,
        }


def get_database_config() -> dict[str, Any]:
    """Возвращает параметры в формате ``psycopg2.connect``."""
    return DatabaseConfig.from_env().as_psycopg_kwargs()
