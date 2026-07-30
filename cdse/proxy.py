"""Прокси."""
import os
import requests


def build_proxies():
    """Сборщик прокси."""
    proxy = os.getenv("CDSE_PROXY", "socks5h://127.0.0.1:10808")

    return {
        "http": proxy,
        "https": proxy,
    }


class ProxySession(requests.Session):
    """Класс сессии для работы через проксю."""
    def __init__(self):
        super().__init__()
        proxies = build_proxies()
        self.proxies.update(proxies)