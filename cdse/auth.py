"""Модуль авторизации CDSE."""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Optional

import requests

from core.logging import get_logger
from core.settings import TOKEN_URL
from .exceptions import CdseAuthError
from .proxy import ProxySession

logger = get_logger("CdseAuth")


@dataclass(frozen=True)
class CdseCredentials:
    """Учётные данные."""
    username: str
    password: str
    client_id: str = "cdse-public"
    totp: Optional[str] = None


class CdseTokenProvider:
    """
    Получает и кэширует access token.
    """

    def __init__(
        self,
        credentials: CdseCredentials,
        session: Optional[requests.Session] = None,
        token_url: str = TOKEN_URL,
        refresh_margin_sec: int = 60,
    ) -> None:
        self.credentials = credentials
        self.session = session or ProxySession()
        self.token_url = token_url
        self.refresh_margin_sec = refresh_margin_sec
        self._lock = threading.Lock()
        self._access_token: Optional[str] = None
        self._expires_at: float = 0.0

    def get_token(self, force_refresh: bool = False) -> str:
        """Возвращает актуальный access token."""
        with self._lock:
            now = time.time()
            if (
                not force_refresh
                and self._access_token
                and now < (self._expires_at - self.refresh_margin_sec)
            ):
                return self._access_token

            data = {
                "client_id": self.credentials.client_id,
                "username": self.credentials.username,
                "password": self.credentials.password,
                "grant_type": "password",
            }
            if self.credentials.totp:
                data["totp"] = self.credentials.totp

            try:
                response = self.session.post(self.token_url, data=data, timeout=60)
                response.raise_for_status()
            except requests.RequestException as exc:
                raise CdseAuthError(f"Не удалось получить CDSE token: {exc}") from exc

            try:
                payload = response.json()
            except ValueError as exc:
                raise CdseAuthError("CDSE token endpoint вернул не-JSON ответ") from exc

            token = payload.get("access_token")
            if not token:
                raise CdseAuthError(f"CDSE token endpoint не вернул access_token: {payload}")

            expires_in = int(payload.get("expires_in", 900))
            self._access_token = token
            self._expires_at = time.time() + expires_in
            return token