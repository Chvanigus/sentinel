"""Тесты преобразования доменных данных и поведения repositories."""

import json
from datetime import UTC, date, datetime

from db.gateway import SqlGateway
from db.models import NdviRecord
from db.repositories import FieldRepository, LayerRepository, NdviRepository
from domain.models import (
    Field,
    LayerMetadataUpdate,
    NdviStatistics,
    PublishedLayer,
)


def ndvi_value(field_id: int) -> NdviStatistics:
    """Создаёт тестовое значение статистики для поля."""
    return NdviStatistics(
        acquired_on=date(2026, 7, 1),
        field_id=field_id,
        mean=0.5,
        maximum=0.8,
        minimum=0.2,
        growth_percent=0.0,
        coefficient_of_variation=10.0,
        is_uniform=True,
    )


def test_ndvi_repository_saves_one_batch():
    """Repository сохраняет набор NDVI одной пакетной операцией."""

    class Gateway:
        """Тестовый gateway, записывающий вызовы вставки."""

        def __init__(self):
            self.calls = []

        def insert_many(self, *args, **kwargs):
            """Запоминает аргументы пакетной вставки."""
            self.calls.append((args, kwargs))

    gateway = Gateway()
    repository = NdviRepository(gateway)

    repository.add_many([ndvi_value(1), ndvi_value(2)])

    assert len(gateway.calls) == 1
    assert len(gateway.calls[0][0][1]) == 2


def test_ndvi_repository_atomically_replaces_selected_fields():
    """Полная замена удаляет старые строки до пакетной вставки."""

    class Gateway:
        """Фиксирует изменяющие запросы и пакетную вставку."""

        def __init__(self):
            self.calls = []

        def execute(self, query, params, **options):
            """Запоминает удаление старой статистики."""
            self.calls.append(("execute", query, params, options))

        def insert_many(self, *args, **kwargs):
            """Запоминает вставку пересчитанной статистики."""
            self.calls.append(("insert_many", args, kwargs))

    gateway = Gateway()

    NdviRepository(gateway).replace_many(
        [ndvi_value(2)],
        field_ids=[2, 1, 2],
        acquired_on=date(2026, 7, 1),
    )

    assert gateway.calls[0][0] == "execute"
    assert gateway.calls[0][2] == (
        date(2026, 7, 1),
        [1, 2],
    )
    assert gateway.calls[0][3] == {"commit": False}
    assert gateway.calls[1][0] == "insert_many"


def test_ndvi_completeness_requires_every_field():
    """Статистика считается полной только при наличии каждого поля."""

    class Gateway:
        """Тестовый gateway с изменяемым результатом чтения."""

        rows_result = [{"fieldid": 1}]

        def rows(self, *_args, **_kwargs):
            """Возвращает настроенный набор строк."""
            return self.rows_result

    gateway = Gateway()
    repository = NdviRepository(gateway)
    fields = [
        Field(id=1, name="one"),
        Field(id=2, name="two"),
    ]

    assert repository.is_complete(
        fields,
        date(2026, 7, 1),
    ) is False

    gateway.rows_result = [{"fieldid": 1}, {"fieldid": 2}]
    assert repository.is_complete(
        fields,
        date(2026, 7, 1),
    ) is True


def test_missing_agroids_requires_all_published_layer_types():
    """Хозяйство остаётся незавершённым без любого обязательного слоя."""

    class Gateway:
        """Тестовый gateway опубликованных слоёв."""

        def rows(self, *_args, **_kwargs):
            """Возвращает частично опубликованные типы слоёв."""
            return [
                {"date": date(2026, 7, 1), "agroid": 1, "set": "ndvi"},
                {"date": date(2026, 7, 1), "agroid": 1, "set": "ndwi"},
                {"date": date(2026, 7, 1), "agroid": 1, "set": "tci"},
                {"date": date(2026, 7, 1), "agroid": 3, "set": "ndvi"},
            ]

    repository = LayerRepository(Gateway())

    assert repository.missing_agroids(
        date(2026, 7, 1)
    ) == [3, 4, 5, 6]


def test_layer_add_many_relies_on_single_idempotent_insert():
    """Пакет слоёв сохраняется одним INSERT без предварительного SELECT."""

    class Gateway:
        """Записывает вставку и запрещает предварительное чтение."""

        def __init__(self):
            self.calls = []

        def row(self, *_args, **_kwargs):
            """Сообщает о запрещённом предварительном чтении."""
            raise AssertionError("Предварительный SELECT не требуется")

        def execute(self, query, params):
            """Запоминает идемпотентную вставку с обновлением метаданных."""
            self.calls.append((query, params))

    gateway = Gateway()
    LayerRepository(gateway).add_many([
        PublishedLayer(
            name="sentinel:a3_ndvi_2026-07-01",
            acquired_on=date(2026, 7, 1),
            product="ndvi",
            agroid=3,
        ),
        PublishedLayer(
            name="sentinel:a3_tci_2026-07-01",
            acquired_on=date(2026, 7, 1),
            product="tci",
            agroid=3,
        ),
    ])

    assert len(gateway.calls) == 1
    assert "ON CONFLICT (name) DO UPDATE" in gateway.calls[0][0]
    payload = json.loads(gateway.calls[0][1][0])
    assert [item["name"] for item in payload] == [
        "sentinel:a3_ndvi_2026-07-01",
        "sentinel:a3_tci_2026-07-01",
    ]


def test_layer_metadata_refresh_uses_one_aggregate_update():
    """Метаданные слоёв и NDVI-качество обновляются одним SQL-запросом."""

    class Gateway:
        """Фиксирует пакетный запрос обновления метаданных."""

        def __init__(self):
            self.calls = []

        def row(self, query, params):
            """Запоминает запрос и возвращает число обновлённых слоёв."""
            self.calls.append((query, params))
            return {"updated_count": 4}

    gateway = Gateway()
    updated = LayerRepository(gateway).refresh_metadata(
        [
            LayerMetadataUpdate(
                acquired_on=date(2026, 7, 1),
                agroid=3,
                acquired_at=datetime(2026, 7, 1, 8, 16, tzinfo=UTC),
                satellite="S2A",
                source_level="L2A",
                processing_baseline=511,
                source_tiles=("T38ULA",),
            )
        ]
    )

    assert updated == 4
    assert len(gateway.calls) == 1
    query, params = gateway.calls[0]
    payload = json.loads(params[0])
    assert payload[0]["source_tiles"] == ["T38ULA"]
    assert payload[0]["fallback_algorithm_version"] == "legacy"
    assert "SUM(ndvi.cloud_pixel_count)" in query
    assert "COALESCE(" in query
    assert "generated_at =" not in query


def test_field_repository_reads_geometry_batch_in_one_scope():
    """Repository возвращает геометрии нескольких полей одним объектом."""

    class Gateway:
        """Тестовый gateway геометрий полей."""

        def __init__(self):
            self.calls = []

        def rows(self, query, params):
            """Возвращает геометрии всех запрошенных полей."""
            self.calls.append((query, params))
            return [
                (field_id, f"geometry-{field_id}")
                for field_id in params[0]
            ]

    gateway = Gateway()

    result = FieldRepository(gateway).geometries([10, 20], 2026)

    assert result == {10: "geometry-10", 20: "geometry-20"}
    assert len(gateway.calls) == 1
    query, params = gateway.calls[0]
    assert "unnest(%s::bigint[])" in query
    assert "CROSS JOIN LATERAL" in query
    assert params == ([10, 20], 2026)


def test_dataclass_batch_excludes_generated_id():
    """Пакетная вставка исключает генерируемый первичный ключ."""
    gateway = SqlGateway.__new__(SqlGateway)

    rows = gateway.tuples_for_insert(
        NdviRecord,
        [
            NdviRecord(
                date=date(2026, 7, 1),
                fieldid=42,
                ndvimean=0.5,
                ndvimax=0.8,
                ndvimin=0.2,
                growth_percent=0.0,
                ndvi_cv=10.0,
                is_uniform=True,
            )
        ],
        include_id=False,
    )

    assert rows[0][:8] == (
        date(2026, 7, 1),
        42,
        0.5,
        0.8,
        0.2,
        0.0,
        10.0,
        True,
    )
    assert len(rows[0]) == 23
