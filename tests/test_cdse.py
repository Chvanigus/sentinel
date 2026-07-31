"""Тесты поиска, авторизации и возобновляемой загрузки CDSE."""

import io
import zipfile
from datetime import date

import pytest

from cdse import composition
from cdse.auth import CdseCredentials, CdseTokenProvider
from cdse.client import CdseODataClient
from cdse.download import ODataProductDownloader
from cdse.exceptions import CdseAuthError, CdseQueryError
from cdse.models import ProductRecord
from cdse.search import ODataProductSearcher
from cdse.service import CdseService
from cdse.utils import normalize_tile, split_date_range
from cli.commands.download import resolve_download_range
from core.settings import (
    L1C_COLLECTION,
    L1C_PRODUCT_TYPE,
    L2A_PRODUCT_TYPE,
)


def product(**overrides) -> ProductRecord:
    """Создаёт запись тестового продукта с переопределяемыми полями."""
    values = {
        "product_id": "product-id",
        "name": "S2A_MSIL2A_20260701T000000_T38ULA_20260701T000000",
        "tile": "38ULA",
        "date": date(2026, 7, 1).isoformat(),
        "cloud_cover": 3.5,
        "size_bytes": 100,
    }
    values.update(overrides)
    return ProductRecord(**values)


def test_download_range_defaults_to_three_calendar_days():
    """Ночной диапазон включает текущую дату и два предыдущих дня."""
    assert resolve_download_range(
        start=None,
        end=None,
        today=date(2026, 7, 31),
    ) == ("2026-07-29", "2026-07-31")


def test_download_range_uses_explicit_dates():
    """Ручной диапазон имеет приоритет над автоматическим окном."""
    assert resolve_download_range(
        start="2026-07-01",
        end="2026-07-15",
        lookback_days=7,
    ) == ("2026-07-01", "2026-07-15")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"start": None, "end": None, "lookback_days": 0},
        {"start": "2026-07-31", "end": "2026-07-01"},
    ],
)
def test_download_range_rejects_invalid_values(kwargs):
    """Нулевое окно и обратный диапазон отклоняются."""
    with pytest.raises(ValueError):
        resolve_download_range(**kwargs)


class FakeResponse:
    """Минимальный потоковый HTTP-ответ для тестов загрузчика."""

    def __init__(self, status_code: int, chunks: list[bytes]):
        self.status_code = status_code
        self._chunks = chunks
        self.closed = False

    def raise_for_status(self):
        """Имитирует успешную проверку HTTP-статуса."""
        return None

    def iter_content(self, _chunk_size):
        """Последовательно выдаёт настроенные части ответа."""
        yield from self._chunks

    def close(self):
        """Отмечает потоковый ответ закрытым."""
        self.closed = True


class FakeDownloadClient:
    """Клиент, всегда возвращающий один настроенный ответ."""

    def __init__(self, response: FakeResponse):
        self.response = response

    def download_stream(self, *_args, **_kwargs):
        """Возвращает тестовый потоковый ответ."""
        return self.response


def test_auth_error_does_not_expose_response_secrets():
    """Ошибка авторизации не раскрывает секретное значение ответа."""

    class Response:
        """Тестовый ответ с чувствительным полем."""

        def raise_for_status(self):
            """Имитирует успешный HTTP-статус."""
            return None

        def json(self):
            """Возвращает ошибку с секретным значением."""
            return {
                "error": "invalid_grant",
                "refresh_token": "sensitive-value",
            }

    class Session:
        """Тестовая HTTP-сессия авторизации."""

        def post(self, *_args, **_kwargs):
            """Возвращает ответ с ошибкой выдачи токена."""
            return Response()

    provider = CdseTokenProvider(
        CdseCredentials("user", "password"),
        session=Session(),
        token_url="https://identity.example/token",
    )

    with pytest.raises(CdseAuthError) as exc_info:
        provider.get_token()

    assert "sensitive-value" not in str(exc_info.value)
    assert "refresh_token" in str(exc_info.value)


def test_resume_overwrites_partial_file_when_server_ignores_range(tmp_path):
    """Ответ 200 перезаписывает временный файл вместо ошибочного append."""
    partial = tmp_path / "scene.zip.tmp"
    partial.write_bytes(b"partial-")
    response = FakeResponse(200, [b"complete"])

    ODataProductDownloader(
        FakeDownloadClient(response)
    )._download_with_resume("id", partial)

    assert partial.read_bytes() == b"complete"
    assert response.closed is True


def test_resume_appends_when_server_returns_partial_content(tmp_path):
    """Ответ 206 дописывает продолжение во временный файл."""
    partial = tmp_path / "scene.zip.tmp"
    partial.write_bytes(b"first-")
    response = FakeResponse(206, [b"second"])

    ODataProductDownloader(
        FakeDownloadClient(response)
    )._download_with_resume("id", partial)

    assert partial.read_bytes() == b"first-second"


def test_complete_temporary_zip_is_promoted_without_network_call(tmp_path):
    """Готовый временный ZIP атомарно повышается без запроса к сети."""

    class NoNetworkClient:
        """Клиент, запрещающий непредвиденный сетевой вызов."""

        def download_stream(self, *_args, **_kwargs):
            """Падает при любой попытке сетевого скачивания."""
            raise AssertionError("Сеть не должна использоваться")

    record = product(name="SCENE.SAFE")
    target_dir = tmp_path / "2026" / "38ULA"
    target_dir.mkdir(parents=True)
    temporary = target_dir / f"{record.archive_name}.tmp"
    with zipfile.ZipFile(temporary, "w") as archive:
        archive.writestr("manifest.safe", b"ok")

    result = ODataProductDownloader(NoNetworkClient()).download_product(
        record, archive_root=tmp_path
    )

    assert result == target_dir / "SCENE.zip"
    assert result.is_file()
    assert not temporary.exists()


def test_corrupt_final_zip_is_preserved_and_downloaded_again(tmp_path):
    """Повреждённый финальный ZIP не блокирует последующие ночные запуски."""
    record = product(name="SCENE.SAFE")
    target_dir = tmp_path / "2026" / "38ULA"
    target_dir.mkdir(parents=True)
    final = target_dir / record.archive_name
    final.write_bytes(b"broken")
    valid_zip = io.BytesIO()
    with zipfile.ZipFile(valid_zip, "w") as archive:
        archive.writestr("manifest.safe", b"ok")
    response = FakeResponse(200, [valid_zip.getvalue()])

    result = ODataProductDownloader(
        FakeDownloadClient(response)
    ).download_product(record, archive_root=tmp_path)

    assert result == final
    assert zipfile.is_zipfile(result)
    assert final.with_suffix(".zip.corrupt").read_bytes() == b"broken"


def test_client_closes_unauthorized_response_before_retry():
    """Ответ 401 освобождает соединение пула до повторного запроса."""

    class Response:
        """HTTP-ответ с отслеживанием закрытия."""

        text = ""

        def __init__(self, status_code):
            self.status_code = status_code
            self.closed = False

        def close(self):
            """Отмечает освобождение соединения."""
            self.closed = True

    class Tokens:
        """Возвращает разные токены до и после refresh."""

        def get_token(self, force_refresh=False):
            """Возвращает тестовый токен."""
            return "refreshed" if force_refresh else "initial"

    class Session:
        """Последовательно возвращает 401 и успешный ответ."""

        def __init__(self, responses):
            self.responses = responses

        def request(self, *_args, **_kwargs):
            """Возвращает следующий ответ."""
            return self.responses.pop(0)

    unauthorized = Response(401)
    successful = Response(200)
    client = CdseODataClient.__new__(CdseODataClient)
    client.token_provider = Tokens()
    client.session = Session([unauthorized, successful])
    client.timeout = 60

    assert client.request("GET", "https://example.test") is successful
    assert unauthorized.closed is True


def test_group_by_tile_normalizes_optional_t_prefix():
    """Группировка считает коды тайлов с префиксом T эквивалентными."""
    grouped = CdseService.group_by_tile(
        [product(tile="38ULA")],
        tiles=["T38ULA", "T38ULB"],
    )

    assert list(grouped) == ["38ULA", "38ULB"]
    assert grouped["38ULA"][0].product_id == "product-id"


def test_l2a_search_falls_back_to_l1c():
    """Пустой поиск L2A автоматически повторяется для L1C."""
    record = product()

    class Searcher:
        """Поисковик, возвращающий запись только со второй попытки."""

        def __init__(self):
            self.product_types = []

        def search(self, **kwargs):
            """Запоминает тип продукта и имитирует fallback-результат."""
            self.product_types.append(kwargs["product_type"])
            return [] if len(self.product_types) == 1 else [record]

    searcher = Searcher()
    service = CdseService(
        searcher=searcher,
        downloader=object(),
        fallback_collection=L1C_COLLECTION,
        preferred_product_type=L2A_PRODUCT_TYPE,
        fallback_product_type=L1C_PRODUCT_TYPE,
    )

    grouped = service.search(
        collection="SENTINEL-2",
        start="2026-07-01",
        end="2026-07-01",
        do_download=False,
        tiles=["T38ULA"],
        product_type=L2A_PRODUCT_TYPE,
    )

    assert searcher.product_types == [L2A_PRODUCT_TYPE, L1C_PRODUCT_TYPE]
    assert grouped["38ULA"] == [record]


def test_download_orchestrator_reports_failed_tasks():
    """Application service агрегирует ошибку фонового скачивания."""

    class Downloader:
        """Загрузчик, всегда завершающийся ошибкой."""

        def download_product(self, **_kwargs):
            """Имитирует сетевую ошибку загрузки."""
            raise RuntimeError("network failure")

    service = CdseService(
        searcher=object(),
        downloader=Downloader(),
        fallback_collection=L1C_COLLECTION,
        preferred_product_type=L2A_PRODUCT_TYPE,
        fallback_product_type=L1C_PRODUCT_TYPE,
    )

    with pytest.raises(RuntimeError, match="1 из 1"):
        service.download(
            {"38ULA": [product()]},
            archive_root="/tmp/archive",
            workers=1,
        )


def test_search_matches_legacy_archive_name_without_safe_suffix():
    """Поиск сопоставляет SAFE-имя с историческим ZIP без SAFE-суффикса."""

    class Client:
        """Клиент, возвращающий один OData-продукт."""

        def iter_products(self, **_kwargs):
            """Выдаёт тестовый продукт в формате OData."""
            yield {
                "Id": "id",
                "Name": "SCENE.SAFE",
                "ContentDate": {"Start": "2026-07-01T10:00:00Z"},
                "Attributes": [{"Name": "tileId", "Value": "T38ULA"}],
            }

    records = ODataProductSearcher(Client()).search(
        collection="SENTINEL-2",
        start="2026-07-01",
        end="2026-07-01",
        do_download=False,
        archive_index={"SCENE.zip"},
    )

    assert records[0].name == "SCENE.SAFE"
    assert records[0].archive_name == "SCENE.zip"
    assert records[0].exists is True


def test_client_wraps_invalid_json_response():
    """Клиент преобразует невалидный JSON в доменную ошибку запроса."""

    class Response:
        """Ответ с неразбираемым JSON-телом."""

        def json(self):
            """Имитирует ошибку декодирования JSON."""
            raise ValueError("invalid")

    client = CdseODataClient.__new__(CdseODataClient)
    client.catalogue_base = "https://catalogue.example/v1"
    client.request = lambda *_args, **_kwargs: Response()

    with pytest.raises(CdseQueryError, match="не-JSON"):
        list(client.iter_pages("https://catalogue.example/v1/Products"))


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("T38ula", "38ULA"),
        ("38ULB", "38ULB"),
        ("", ""),
    ],
)
def test_normalize_tile(value, expected):
    """Код тайла нормализуется к верхнему регистру без префикса T."""
    assert normalize_tile(value) == expected


def test_split_date_range_is_inclusive_for_input_end_date():
    """Пользовательская конечная дата включается в интервалы поиска."""
    assert split_date_range("2026-07-01", "2026-07-02") == [
        (
            "2026-07-01T00:00:00.000Z",
            "2026-07-02T00:00:00.000Z",
        ),
        (
            "2026-07-02T00:00:00.000Z",
            "2026-07-03T00:00:00.000Z",
        ),
    ]


@pytest.mark.parametrize("chunk_days", [0, -1])
def test_split_date_range_rejects_non_positive_chunk(chunk_days):
    """Неположительный размер временного чанка отклоняется."""
    with pytest.raises(ValueError, match="chunk_days"):
        split_date_range("2026-07-01", "2026-07-02", chunk_days)


def test_split_date_range_rejects_reversed_range():
    """Диапазон с началом после окончания отклоняется."""
    with pytest.raises(ValueError, match="начала"):
        split_date_range("2026-07-02", "2026-07-01")


def test_composition_configures_one_shared_proxy_session(monkeypatch):
    """Composition root передаёт одну proxy-сессию всем HTTP-адаптерам."""
    monkeypatch.setenv("CDSE_USERNAME", "user")
    monkeypatch.setenv("CDSE_PASSWORD", "password")
    monkeypatch.setattr(
        composition,
        "DEFAULT_PROXY_URL",
        "socks5://proxy.example:1080",
    )

    service = composition.build_cdse_service()

    client = service.searcher.client
    assert service.downloader.client is client
    assert client.token_provider.session is client.session
    assert client.session.proxies == {
        "http": "socks5://proxy.example:1080",
        "https": "socks5://proxy.example:1080",
    }


def test_composition_requires_cdse_credentials(monkeypatch):
    """Composition root явно отклоняет неполные учётные данные CDSE."""
    monkeypatch.delenv("CDSE_USERNAME", raising=False)
    monkeypatch.delenv("CDSE_PASSWORD", raising=False)

    with pytest.raises(RuntimeError, match="CDSE_USERNAME"):
        composition.build_cdse_service()
