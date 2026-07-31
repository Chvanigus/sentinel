"""Тесты process-lifetime cache PostGIS-адаптера полевых данных."""

from domain.models import Field
from processing.adapters.postgis import PostgisFieldDataProvider


class FakeConnection:
    """Минимальный connection context для теста адаптера."""

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        return None


class FakeFieldRepository:
    """Repository с наблюдаемыми вызовами сезонных справочников."""

    bounds_calls = []
    fields_calls = []
    geometry_calls = []

    def __init__(self, _gateway):
        pass

    def bounds(self, *, srid, year, agroid):
        """Возвращает фиксированные границы и запоминает ключ."""
        self.bounds_calls.append((year, agroid, srid))
        return 1.0, 2.0, 3.0, 4.0

    def list_for_agro(self, agroid, year):
        """Возвращает фиксированный список полей и запоминает ключ."""
        self.fields_calls.append((agroid, year))
        return [Field(id=10, name="field")]

    def geometries(self, field_ids, year):
        """Возвращает геометрии запрошенных полей."""
        self.geometry_calls.append((tuple(field_ids), year))
        return {
            field_id: f"geometry-{field_id}"
            for field_id in field_ids
        }


def test_adapter_caches_bounds_fields_and_geometries(monkeypatch):
    """Неизменяемые сезонные данные читаются из PostGIS только один раз."""
    connections = []

    def connect(**_kwargs):
        """Создаёт наблюдаемое тестовое подключение."""
        connection = FakeConnection()
        connections.append(connection)
        return connection

    FakeFieldRepository.bounds_calls = []
    FakeFieldRepository.fields_calls = []
    FakeFieldRepository.geometry_calls = []
    monkeypatch.setattr(
        "processing.adapters.postgis.psycopg2.connect",
        connect,
    )
    monkeypatch.setattr(
        "processing.adapters.postgis.SqlGateway",
        lambda connection: connection,
    )
    monkeypatch.setattr(
        "processing.adapters.postgis.FieldRepository",
        FakeFieldRepository,
    )
    provider = PostgisFieldDataProvider()

    assert provider.bounds(year=2026, agroid=3, srid=3857) == (
        1.0,
        2.0,
        3.0,
        4.0,
    )
    provider.bounds(year=2026, agroid=3, srid=3857)
    assert provider.fields(agroid=3, year=2026) == [
        Field(id=10, name="field")
    ]
    provider.fields(agroid=3, year=2026)
    assert provider.geometries(field_ids=[10, 20], year=2026) == {
        10: "geometry-10",
        20: "geometry-20",
    }
    assert provider.geometries(field_ids=[20, 30], year=2026) == {
        20: "geometry-20",
        30: "geometry-30",
    }

    assert FakeFieldRepository.bounds_calls == [(2026, 3, 3857)]
    assert FakeFieldRepository.fields_calls == [(3, 2026)]
    assert FakeFieldRepository.geometry_calls == [
        ((10, 20), 2026),
        ((30,), 2026),
    ]
    assert len(connections) == 4
