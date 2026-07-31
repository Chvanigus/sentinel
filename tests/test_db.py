"""Тесты преобразования доменных данных и поведения repositories."""

from datetime import date

from db.gateway import SqlGateway
from db.models import NdviRecord
from db.repositories import FieldRepository, LayerRepository, NdviRepository
from domain.models import Field, NdviStatistics, PublishedLayer


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
                {"agroid": 1, "set": "ndvi"},
                {"agroid": 1, "set": "ndwi"},
                {"agroid": 1, "set": "tci"},
                {"agroid": 3, "set": "ndvi"},
            ]

    repository = LayerRepository(Gateway())

    assert repository.missing_agroids(
        date(2026, 7, 1)
    ) == [3, 4, 5, 6]


def test_layer_add_relies_on_single_idempotent_insert():
    """Добавление слоя не выполняет лишний SELECT перед ON CONFLICT."""

    class Gateway:
        """Записывает вставку и запрещает предварительное чтение."""

        def __init__(self):
            self.calls = []

        def row(self, *_args, **_kwargs):
            """Сообщает о запрещённом предварительном чтении."""
            raise AssertionError("Предварительный SELECT не требуется")

        def insert_one(self, *args, **kwargs):
            """Запоминает идемпотентную вставку."""
            self.calls.append((args, kwargs))

    gateway = Gateway()
    LayerRepository(gateway).add(
        PublishedLayer(
            name="sentinel:a3_ndvi_2026-07-01",
            acquired_on=date(2026, 7, 1),
            product="ndvi",
            agroid=3,
        )
    )

    assert len(gateway.calls) == 1
    assert gateway.calls[0][1]["conflict_fields"] == "name"


def test_field_repository_reads_geometry_batch_in_one_scope():
    """Repository возвращает геометрии нескольких полей одним объектом."""

    class Gateway:
        """Тестовый gateway геометрий полей."""

        def __init__(self):
            self.calls = []

        def row(self, _query, params):
            """Возвращает геометрию, связанную с идентификатором поля."""
            self.calls.append(params)
            return [f"geometry-{params[0]}"]

    gateway = Gateway()

    result = FieldRepository(gateway).geometries([10, 20], 2026)

    assert result == {10: "geometry-10", 20: "geometry-20"}
    assert gateway.calls == [(10, 2026), (20, 2026)]


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

    assert rows == [
        (
            date(2026, 7, 1),
            42,
            0.5,
            0.8,
            0.2,
            0.0,
            10.0,
            True,
        )
    ]
