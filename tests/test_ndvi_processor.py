"""Тесты orchestration расчёта NDVI-статистики полей."""

from datetime import UTC, date, datetime
from pathlib import Path

import numpy as np
import pytest

from domain.models import Field, NdviStatistics
from processing.domain import ProductLevel, SceneContext
from processing.processors.ndvistat import NdviStatisticsProcessor
from processing.raster import RasterClip


class StatisticsPaths:
    """Формирует изолированные пути NDVI и GeoJSON."""

    def __init__(self, root: Path):
        """Сохраняет корень тестового рабочего пространства."""
        self.root = root

    def ndvi_source(self, agroid: int) -> str:
        """Возвращает исходный NDVI хозяйства."""
        return str(self.root / f"a{agroid}_ndvi.tif")

    def field_geojson(self, agroid: int, field_code: str) -> str:
        """Возвращает путь GeoJSON-маски поля."""
        return str(self.root / f"a{agroid}_{field_code}.geojson")

    def scl_source(self, agroid: int) -> str:
        """Возвращает тестовую SCL-маску хозяйства."""
        return str(self.root / f"a{agroid}_scl.tif")


class RecordingFieldData:
    """Имитирует единый порт полей, геометрий и NDVI-записей."""

    def __init__(self, fields, geometries, *, complete=False):
        """Сохраняет тестовые ответы и создаёт журналы вызовов."""
        self.field_values = fields
        self.geometry_values = geometries
        self.complete = complete
        self.complete_calls = []
        self.field_calls = []
        self.geometry_calls = []
        self.saved_values = []

    def ndvi_is_complete(self, **parameters):
        """Возвращает признак завершённости и фиксирует параметры."""
        self.complete_calls.append(parameters)
        return self.complete

    def fields(self, **parameters):
        """Возвращает поля и фиксирует параметры выборки."""
        self.field_calls.append(parameters)
        return self.field_values

    def geometries(self, **parameters):
        """Возвращает геометрии и фиксирует пакетный запрос."""
        self.geometry_calls.append(parameters)
        return self.geometry_values

    def save_ndvi(self, values, **options):
        """Запоминает пакет и режим сохранения NDVI-статистики."""
        self.saved_values.append((values, options))


class WritingGeometryExporter:
    """Имитирует экспорт геометрии созданием GeoJSON-файла."""

    def __init__(self):
        """Создаёт пустой журнал экспортов."""
        self.exports = []

    def export(self, geometry, destination):
        """Записывает тестовую маску и фиксирует экспорт."""
        path = Path(destination)
        path.write_text(str(geometry), encoding="utf-8")
        self.exports.append((geometry, path))
        return path


RASTER_READERS = []


class RecordingRasterReader:
    """Имитирует открытый один раз источник NDVI/SCL хозяйства."""

    def __init__(self, source, **options):
        """Фиксирует исходники и создаёт журнал вырезок."""
        self.source = source
        self.options = options
        self.masks = []
        RASTER_READERS.append(self)

    def __enter__(self):
        """Возвращает тестовый reader."""
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        """Завершает тестовый контекст без подавления ошибок."""
        return None

    def clip(self, mask):
        """Фиксирует маску и возвращает NDVI вместе с SCL."""
        self.masks.append(mask)
        return RasterClip(
            values=np.array([[0.5]], dtype=np.float32),
            coverage=np.array([[True]], dtype=bool),
            scl=np.array([[4.0]], dtype=np.float32),
        )


class RecordingAnalyzer:
    """Возвращает детерминированную статистику и фиксирует анализ."""

    def __init__(self):
        """Создаёт пустой журнал анализируемых полей."""
        self.calls = []

    def analyze(self, *, ndvi, acquired_on, field_id, **metadata):
        """Фиксирует входы и возвращает статистику тестового поля."""
        self.calls.append((ndvi.copy(), acquired_on, field_id, metadata))
        return NdviStatistics(
            acquired_on=acquired_on,
            field_id=field_id,
            mean=float(field_id),
            maximum=1.0,
            minimum=0.0,
            growth_percent=0.0,
            coefficient_of_variation=1.0,
            is_uniform=True,
        )


def make_scene() -> SceneContext:
    """Создаёт L2A-сцену для одного хозяйства."""
    return SceneContext(
        archive_path=Path("scene.zip"),
        tile="t38ula",
        acquired_on=date(2026, 7, 1),
        satellite="s2a",
        level=ProductLevel.L2A,
        agroids=(3,),
        acquired_at=datetime(2026, 7, 1, 8, 16, 11, tzinfo=UTC),
    )


def test_run_batches_missing_geometry_and_saves_statistics(
        tmp_path,
        monkeypatch,
):
    """Процессор пакетно получает маски, вырезает поля и сохраняет результат."""
    paths = StatisticsPaths(tmp_path)
    Path(paths.ndvi_source(3)).write_bytes(b"ndvi")
    Path(paths.scl_source(3)).write_bytes(b"scl")
    existing_geojson = Path(paths.field_geojson(3, "10"))
    existing_geojson.write_text("existing", encoding="utf-8")
    fields = [
        Field(id=10, name="10", fieldcode="F100а"),
        Field(id=11, name="11", fieldcode="F100б"),
    ]
    field_data = RecordingFieldData(
        fields,
        {11: "geometry-11"},
    )
    exporter = WritingGeometryExporter()
    analyzer = RecordingAnalyzer()
    RASTER_READERS.clear()
    monkeypatch.setattr(
        "processing.processors.ndvistat.FieldRasterReader",
        RecordingRasterReader,
    )
    processor = NdviStatisticsProcessor(
        make_scene(),
        paths,
        field_data,
        exporter,
        nodata=-9999.0,
    )
    processor.analyzer = analyzer

    processor.run()

    assert field_data.complete_calls == [
        {
            "agroid": 3,
            "year": 2026,
            "acquired_on": date(2026, 7, 1),
        }
    ]
    assert field_data.field_calls == [{"agroid": 3, "year": 2026}]
    assert field_data.geometry_calls == [
        {"field_ids": [11], "year": 2026}
    ]
    assert exporter.exports == [
        ("geometry-11", Path(paths.field_geojson(3, "11")))
    ]
    assert len(RASTER_READERS) == 1
    assert RASTER_READERS[0].source == paths.ndvi_source(3)
    assert RASTER_READERS[0].options == {
        "scl_path": paths.scl_source(3),
        "nodata": -9999.0,
    }
    assert RASTER_READERS[0].masks == [
        paths.field_geojson(3, "10"),
        paths.field_geojson(3, "11"),
    ]
    assert [call[2] for call in analyzer.calls] == [10, 11]
    assert all(
        call[3]["acquired_at"]
        == datetime(2026, 7, 1, 8, 16, 11, tzinfo=UTC)
        for call in analyzer.calls
    )
    assert len(field_data.saved_values) == 1
    assert [
        value.field_id
        for value in field_data.saved_values[0][0]
    ] == [10, 11]
    assert field_data.saved_values[0][1] == {
        "field_ids": [10, 11],
        "acquired_on": date(2026, 7, 1),
        "overwrite": False,
    }


def test_run_skips_completed_agro_before_loading_fields(tmp_path):
    """Полностью рассчитанное хозяйство не загружает поля и геометрии."""
    paths = StatisticsPaths(tmp_path)
    Path(paths.ndvi_source(3)).write_bytes(b"ndvi")
    Path(paths.scl_source(3)).write_bytes(b"scl")
    field_data = RecordingFieldData([], {}, complete=True)

    NdviStatisticsProcessor(
        make_scene(),
        paths,
        field_data,
        WritingGeometryExporter(),
        nodata=-9999.0,
    ).run()

    assert field_data.complete_calls
    assert field_data.field_calls == []
    assert field_data.geometry_calls == []
    assert field_data.saved_values == []


def test_run_overwrites_completed_statistics_with_quality_metadata(
        tmp_path,
        monkeypatch,
):
    """Принудительный режим заменяет даже ранее завершённую статистику."""
    paths = StatisticsPaths(tmp_path)
    Path(paths.ndvi_source(3)).write_bytes(b"ndvi")
    Path(paths.scl_source(3)).write_bytes(b"scl")
    field = Field(id=10, name="10")
    Path(paths.field_geojson(3, field.name)).write_text(
        "geometry",
        encoding="utf-8",
    )
    field_data = RecordingFieldData(
        [field],
        {},
        complete=True,
    )
    class EmptyRasterReader(RecordingRasterReader):
        """Возвращает поле без валидного NDVI."""

        def clip(self, mask):
            """Возвращает nodata NDVI и валидную SCL-маску."""
            self.masks.append(mask)
            return RasterClip(
                values=np.array([[-9999.0]], dtype=np.float32),
                coverage=np.array([[True]], dtype=bool),
                scl=np.array([[4.0]], dtype=np.float32),
            )

    monkeypatch.setattr(
        "processing.processors.ndvistat.FieldRasterReader",
        EmptyRasterReader,
    )

    NdviStatisticsProcessor(
        make_scene(),
        paths,
        field_data,
        WritingGeometryExporter(),
        nodata=-9999.0,
        overwrite=True,
    ).run()

    assert field_data.complete_calls == []
    assert len(field_data.saved_values) == 1
    values, options = field_data.saved_values[0]
    assert len(values) == 1
    assert values[0].mean is None
    assert values[0].valid_pixel_count == 0
    assert values[0].total_pixel_count == 1
    assert options == {
        "field_ids": [10],
        "acquired_on": date(2026, 7, 1),
        "overwrite": True,
    }


def test_run_overwrites_only_selected_field(tmp_path, monkeypatch):
    """Выборочный перерасчёт анализирует и заменяет только заданное поле."""
    paths = StatisticsPaths(tmp_path)
    Path(paths.ndvi_source(3)).write_bytes(b"ndvi")
    Path(paths.scl_source(3)).write_bytes(b"scl")
    fields = [
        Field(id=10, name="10", fieldcode="F100а"),
        Field(id=11, name="11", fieldcode="F100б"),
    ]
    for field in fields:
        Path(paths.field_geojson(3, field.name)).write_text(
            "geometry",
            encoding="utf-8",
        )
    field_data = RecordingFieldData(fields, {}, complete=True)
    analyzer = RecordingAnalyzer()
    RASTER_READERS.clear()
    monkeypatch.setattr(
        "processing.processors.ndvistat.FieldRasterReader",
        RecordingRasterReader,
    )
    processor = NdviStatisticsProcessor(
        make_scene(),
        paths,
        field_data,
        WritingGeometryExporter(),
        nodata=-9999.0,
        overwrite=True,
        target_fieldcodes=("f100Б",),
    )
    processor.analyzer = analyzer

    processor.run()

    assert [call[2] for call in analyzer.calls] == [11]
    values, options = field_data.saved_values[0]
    assert [value.field_id for value in values] == [11]
    assert options["field_ids"] == [11]


def test_run_rejects_field_from_another_agro(tmp_path):
    """Поле вне выбранного хозяйства отклоняется без изменения статистики."""
    paths = StatisticsPaths(tmp_path)
    Path(paths.ndvi_source(3)).write_bytes(b"ndvi")
    Path(paths.scl_source(3)).write_bytes(b"scl")
    field_data = RecordingFieldData(
        [Field(id=10, name="10", fieldcode="F100а")],
        {},
    )

    with pytest.raises(
            LookupError,
            match="Fieldcode не относятся к агро 3: F100б",
    ):
        NdviStatisticsProcessor(
            make_scene(),
            paths,
            field_data,
            WritingGeometryExporter(),
            nodata=-9999.0,
            overwrite=True,
            target_fieldcodes=("F100б",),
        ).run()

    assert field_data.saved_values == []


def test_save_geojsons_rejects_field_without_id(tmp_path):
    """Экспорт масок отклоняет поле без идентификатора БД."""
    processor = NdviStatisticsProcessor(
        make_scene(),
        StatisticsPaths(tmp_path),
        RecordingFieldData([], {}),
        WritingGeometryExporter(),
        nodata=-9999.0,
    )

    with pytest.raises(ValueError, match="отсутствует id"):
        processor._save_field_geojsons(
            [Field(id=None, name="missing")],
            3,
        )


def test_save_geojsons_reports_missing_geometry(tmp_path):
    """Отсутствующая в пакетном ответе геометрия даёт понятную ошибку."""
    processor = NdviStatisticsProcessor(
        make_scene(),
        StatisticsPaths(tmp_path),
        RecordingFieldData([], {}),
        WritingGeometryExporter(),
        nodata=-9999.0,
    )

    with pytest.raises(LookupError, match="геометрия поля 42"):
        processor._save_field_geojsons(
            [Field(id=42, name="42")],
            3,
        )
