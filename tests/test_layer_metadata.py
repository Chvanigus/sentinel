"""Тесты быстрого обновления метаданных опубликованных снимков."""
from datetime import UTC, datetime
from pathlib import Path

from processing.domain import ArchivePair, ProductLevel
from processing.layer_metadata import LayerMetadataRefreshService


def archive_pair(day: int) -> ArchivePair:
    """Создаёт тестовую пару архивов за указанный день."""
    return ArchivePair(
        acquired_at=datetime(2026, 7, day, 8, 16, 11),
        prefix="t38",
        ula=Path(f"{day}-ula.zip"),
        ulb=Path(f"{day}-ulb.zip"),
        level=ProductLevel.L2A,
        processing_baseline=511,
        satellite="s2b",
    )


def test_refresh_metadata_filters_period_and_builds_agro_updates():
    """Сервис выбирает период и не запускает обработку содержимого ZIP."""

    class Finder:
        """Возвращает две локальные пары разных дат."""

        def find(self, _root, **_options):
            """Возвращает фиксированный архивный набор."""
            return [archive_pair(1), archive_pair(2)]

    class Writer:
        """Запоминает единый пакет обновлений."""

        def __init__(self):
            self.updates = []

        def refresh_metadata(self, updates):
            """Сохраняет пакет и имитирует обновление двенадцати слоёв."""
            self.updates = updates
            return 12

    writer = Writer()
    summary = LayerMetadataRefreshService(
        archive_root="/archive",
        pair_finder=Finder(),
        writer=writer,
    ).run(
        start_date=datetime(2026, 7, 2),
        end_date=datetime(2026, 7, 3),
    )

    assert summary.discovered_pairs == 2
    assert summary.selected_pairs == 1
    assert summary.prepared_updates == 5
    assert summary.updated_layers == 12
    by_agroid = {item.agroid: item for item in writer.updates}
    assert by_agroid[1].source_tiles == ("T38ULA", "T38ULB")
    assert by_agroid[3].source_tiles == ("T38ULA",)
    assert by_agroid[5].source_tiles == ("T38ULB",)
    assert by_agroid[3].acquired_at == datetime(
        2026,
        7,
        2,
        8,
        16,
        11,
        tzinfo=UTC,
    )
    assert by_agroid[3].satellite == "S2B"
    assert by_agroid[3].source_level == "L2A"
    assert by_agroid[3].processing_baseline == 511
    assert by_agroid[3].fallback_algorithm_version == "legacy"
    assert by_agroid[3].resolution_m == 10
    assert by_agroid[3].is_cloud_masked is False


def test_refresh_metadata_handles_empty_period_without_updates():
    """Пустой период передаёт writer пустой пакет и возвращает нулевой итог."""

    class Finder:
        """Возвращает одну пару вне периода."""

        def find(self, _root, **_options):
            """Возвращает фиксированную пару."""
            return [archive_pair(1)]

    class Writer:
        """Проверяет отсутствие подготовленных обновлений."""

        def refresh_metadata(self, updates):
            """Возвращает ноль только для пустого пакета."""
            assert updates == []
            return 0

    summary = LayerMetadataRefreshService(
        archive_root="/archive",
        pair_finder=Finder(),
        writer=Writer(),
    ).run(
        start_date=datetime(2026, 8, 1),
        end_date=datetime(2026, 9, 1),
    )

    assert summary.selected_pairs == 0
    assert summary.updated_layers == 0
