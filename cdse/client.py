"""HTTP клиент Copernicus Data Space Ecosystem (OData)."""
from __future__ import annotations

from collections.abc import Iterable
from typing import Any
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from core.logging import get_logger

from .auth import CdseTokenProvider
from .exceptions import CdseQueryError

logger = get_logger("CdseODataClient")


class CdseODataClient:
    """
    Низкоуровневый HTTP-клиент для OData.
    """

    def __init__(
            self,
            token_provider: CdseTokenProvider,
            catalogue_base: str,
            download_base: str,
            session: requests.Session | None = None,
            timeout: int = 60,
    ) -> None:
        self.token_provider = token_provider
        self.session = session or token_provider.session
        self.catalogue_base = catalogue_base.rstrip("/")
        self.download_base = download_base.rstrip("/")
        self.timeout = timeout

        retry = Retry(
            total=5,
            connect=5,
            read=5,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset({"GET", "POST"}),
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=20,
                              pool_maxsize=20)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        self.session.headers.update({
            "User-Agent": "cdse-odata-client",
            "Accept": "application/json",
            "Connection": "keep-alive",
        })

    def request(
            self,
            method: str,
            url: str,
            *,
            authorized: bool = True,
            retry_auth: bool = True,
            **kwargs: Any,
    ) -> requests.Response:
        """
        Запрос с retry и refresh token на 401.
        """
        kwargs.setdefault("timeout", self.timeout)

        headers = dict(kwargs.pop("headers", {}) or {})
        if authorized:
            headers[
                "Authorization"] = f"Bearer {self.token_provider.get_token()}"
        kwargs["headers"] = headers

        try:
            response = self.session.request(method, url, **kwargs)
        except requests.RequestException as exc:
            raise CdseQueryError(f"HTTP ошибка {method} {url}: {exc}") from exc

        if response.status_code == 401 and authorized and retry_auth:
            headers[
                "Authorization"] = f"Bearer {self.token_provider.get_token(force_refresh=True)}"
            try:
                response = self.session.request(method, url, **kwargs)
            except requests.RequestException as exc:
                raise CdseQueryError(
                    f"HTTP ошибка после refresh {method} {url}: {exc}") from exc

        if response.status_code >= 400:
            raise CdseQueryError(
                f"CDSE HTTP {response.status_code} for {method} {url}: {response.text[:2000]}"
            )

        return response

    def iter_pages(
            self,
            url: str,
            *,
            params: dict[str, Any] | None = None,
            authorized: bool = True,
    ) -> Iterable[dict[str, Any]]:
        """
        Итерирует все страницы через @odata.nextLink.
        """
        next_url: str | None = url
        next_params = params

        while next_url:
            response = self.request(
                "GET",
                next_url,
                authorized=authorized,
                params=next_params,
            )
            try:
                data = response.json()
            except ValueError as exc:
                raise CdseQueryError(
                    f"CDSE вернул не-JSON ответ для {next_url}"
                ) from exc
            if not isinstance(data, dict):
                raise CdseQueryError(
                    f"CDSE вернул неожиданный JSON для {next_url}"
                )
            yield data

            next_link = data.get("@odata.nextLink") or data.get(
                "@OData.nextLink")
            if next_link:
                next_url = urljoin(self.catalogue_base.rstrip("/") + "/",
                                   next_link)
            else:
                next_url = None
            next_params = None

    def iter_products(
            self,
            *,
            filter_expr: str,
            top: int = 500,
            orderby: str | None = None,
            select: list[str] | None = None,
            expand: list[str] | None = None,
            authorized: bool = True,
    ) -> Iterable[dict[str, Any]]:
        """
        Итерирует все продукты по фильтру.
        """
        params: dict[str, Any] = {
            "$filter": filter_expr,
            "$top": top,
        }
        if orderby:
            params["$orderby"] = orderby
        if select:
            params["$select"] = ",".join(select)
        if expand:
            params["$expand"] = ",".join(expand)

        for page in self.iter_pages(
                f"{self.catalogue_base}/Products",
                params=params,
                authorized=authorized,
        ):
            for item in page.get("value", []) or []:
                if isinstance(item, dict):
                    yield item

    def list_products(
            self,
            *,
            filter_expr: str,
            top: int = 500,
            orderby: str | None = None,
            select: list[str] | None = None,
            expand: list[str] | None = None,
            authorized: bool = True,
    ) -> list[dict[str, Any]]:
        """Возвращает все продукты списком."""
        return list(
            self.iter_products(
                filter_expr=filter_expr,
                top=top,
                orderby=orderby,
                select=select,
                expand=expand,
                authorized=authorized,
            )
        )

    def download_stream(
            self, product_id: str, *, authorized: bool = True,
            headers=None
    ) -> requests.Response:
        """
        Стриминг скачивания продукта.
        """
        url = f"{self.download_base}/Products({product_id})/$value"
        return self.request(
            "GET", url, authorized=authorized, stream=True,
            headers=headers
        )
