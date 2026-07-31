"""Тесты orchestration расчёта NDVI-статистики полей."""

from datetime import date
from pathlib import Path

import numpy as np
import pytest

from domain.models import Field, NdviStatistics
from processing.domain import ProductLevel, SceneContext
from processing.processors.ndvistat import NdviStatisticsProcessor


class StatisticsPaths:
    """Формирует изолированные пути NDVI, GeoJSON и полевых растров."""

    def __init__(self, root: Path):
        """Сохраняет корень тестового рабочего пространства."""
        self.root = root

    def ndvi_source(self, agroid: int) -> str:
        """Возвращает исходный NDVI хозяйства."""
        return str(self.root / f"a{agroid}_ndvi.tif")

    def field_geojson(self, agroid: int, field_code: str) -> str:
        """Возвращает путь GeoJSON-маски поля."""
        return str(self.root / f"a{agroid}_{field_code}.geojson")

    def field_ndvi_tif(self, agroid: int, field_code: str) -> str:
        """Возвращает путь вырезанного полевого NDVI."""
        return str(self.root / f"a{agroid}_{field_code}.tif")


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
        self.added_values = []

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

    def add_ndvi(self, values):
        """Запоминает пакет сохраняемой NDVI-статистики."""
        self.added_values.append(values)


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


class RecordingRasterProcessor:
    """Имитирует вырезку NDVI по GeoJSON-маске."""

    clips = []

    def __init__(self, source, destination):
        """Сохраняет пути исходного и выходного растра."""
        self.source = source
        self.destination = destination

    def clip_by_mask(self, mask, **options):
        """Фиксирует параметры вырезки и создаёт выходной файл."""
        Path(self.destination).write_bytes(b"field-raster")
        self.clips.append(
            (self.source, self.destination, mask, options)
        )


class RecordingAnalyzer:
    """Возвращает детерминированную статистику и фиксирует анализ."""

    def __init__(self):
        """Создаёт пустой журнал анализируемых полей."""
        self.calls = []

    def analyze(self, *, ndvi, acquired_on, field_id):
        """Фиксирует входы и возвращает статистику тестового поля."""
        self.calls.append((ndvi.copy(), acquired_on, field_id))
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
    )


def test_run_batches_missing_geometry_and_saves_statistics(
        tmp_path,
        monkeypatch,
):
    """Процессор пакетно получает маски, вырезает поля и сохраняет результат."""
    paths = StatisticsPaths(tmp_path)
    Path(paths.ndvi_source(3)).write_bytes(b"ndvi")
    existing_geojson = Path(paths.field_geojson(3, "10"))
    existing_geojson.write_text("existing", encoding="utf-8")
    fields = [Field(id=10, name="10"), Field(id=11, name="11")]
    field_data = RecordingFieldData(
        fields,
        {11: "geometry-11"},
    )
    exporter = WritingGeometryExporter()
    analyzer = RecordingAnalyzer()
    RecordingRasterProcessor.clips = []
    monkeypatch.setattr(
        "processing.processors.ndvistat.RasterProcessor",
        RecordingRasterProcessor,
    )
    processor = NdviStatisticsProcessor(
        make_scene(),
        paths,
        field_data,
        exporter,
        nodata=-9999.0,
    )
    processor.analyzer = analyzer
    monkeypatch.setattr(
        processor,
        "_load_ndvi_array",
        lambda _path: np.array([[0.5]], dtype=np.float32),
    )

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
    assert len(RecordingRasterProcessor.clips) == 2
    assert [call[2] for call in analyzer.calls] == [10, 11]
    assert len(field_data.added_values) == 1
    assert [
        value.field_id
        for value in field_data.added_values[0]
    ] == [10, 11]


def test_run_skips_completed_agro_before_loading_fields(tmp_path):
    """Полностью рассчитанное хозяйство не загружает поля и геометрии."""
    paths = StatisticsPaths(tmp_path)
    Path(paths.ndvi_source(3)).write_bytes(b"ndvi")
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
    assert field_data.added_values == []


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
