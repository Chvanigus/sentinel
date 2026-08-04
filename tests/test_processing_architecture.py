"""Архитектурные и orchestration-тесты processing application layer."""

import ast
import re
from datetime import date, datetime
from pathlib import Path

import pytest

from processing import pair_processor as pair_processor_module
from processing.archive import ArchiveMetadata
from processing.discovery import ArchiveName, ArchivePairFinder
from processing.domain import (
    ArchivePair,
    ProductLevel,
    SceneContext,
)
from processing.exceptions import ProcessingRunError
from processing.service import ProcessingService
from processing.workspace import ProcessingOptions, WorkspacePaths


def pair(day: int) -> ArchivePair:
    """Создаёт тестовую пару архивов за день июля 2026 года."""
    return ArchivePair(
        acquired_at=datetime(2026, 7, day, 8, 16, 11),
        prefix="t38",
        ula=Path(f"{day}-ula.zip"),
        ulb=Path(f"{day}-ulb.zip"),
    )


def test_archive_pair_finder_has_single_discovery_responsibility():
    """Finder формирует только полные пары и игнорирует невалидные архивы."""
    archives = [
        "a.zip",
        "b.zip",
        "unpaired.zip",
        "invalid.zip",
    ]
    parsed = {
        "a.zip": ArchiveName(
            "s2a", datetime(2026, 7, 1, 8, 16, 11), "t38ula"
        ),
        "b.zip": ArchiveName(
            "s2a", datetime(2026, 7, 1, 8, 16, 11), "t38ulb"
        ),
        "unpaired.zip": ArchiveName(
            "s2a", datetime(2026, 7, 2, 8, 16, 11), "t38ula"
        ),
        "invalid.zip": None,
    }
    finder = ArchivePairFinder(
        zip_iterator=lambda _root, **_options: archives,
        name_parser=parsed.get,
    )

    assert finder.find("/archive") == [
        ArchivePair(
            acquired_at=datetime(2026, 7, 1, 8, 16, 11),
            prefix="t38",
            ula=Path("a.zip"),
            ulb=Path("b.zip"),
        )
    ]


def test_archive_pair_finder_never_mixes_different_acquisitions():
    """Finder не объединяет тайлы с различным временем съёмки."""
    archives = ["first-ula.zip", "second-ulb.zip"]
    parsed = {
        "first-ula.zip": ArchiveName(
            "s2a", datetime(2026, 7, 1, 8, 0), "t38ula"
        ),
        "second-ulb.zip": ArchiveName(
            "s2a", datetime(2026, 7, 1, 10, 0), "t38ulb"
        ),
    }

    pairs = ArchivePairFinder(
        zip_iterator=lambda _root, **_options: archives,
        name_parser=parsed.get,
    ).find("/archive")

    assert pairs == []


def test_scene_context_is_created_from_archive_metadata():
    """Фабрика контекста корректно вызывается через класс."""
    context = SceneContext.from_zip_info(
        "scene.zip",
        ArchiveMetadata(
            satellite="S2A",
            date=date(2026, 7, 1),
            tile="T38ULA",
            level="MSIL2A",
        ),
    )

    assert context.tile == "t38ula"
    assert context.level is ProductLevel.L2A
    assert context.agroids == (1, 3, 4)


def test_pair_processor_runs_shared_steps_once(monkeypatch):
    """Общие этапы даты выполняются один раз после подготовки двух тайлов."""
    events = []

    class Archive:
        """Имитирует чтение и распаковку архива Sentinel."""

        def __init__(self, path):
            self.path = Path(path)
            tile = "T38ULA" if "ula" in self.path.name else "T38ULB"
            self.metadata = ArchiveMetadata(
                satellite="S2A",
                date=date(2026, 7, 1),
                tile=tile,
                level="MSIL2A",
            )

        def read_band_offsets(self):
            """Возвращает нулевые radiometric offsets."""
            return None

        def extract(self, _root, _bands):
            """Фиксирует распаковку тестового архива."""
            events.append(("extract", self.metadata.tile.lower()))
            return Path("extracted")

    def processor(step_name):
        """Создаёт тестовый процессор, фиксирующий запуск этапа."""

        class Processor:
            """Запоминает контекст выполненного этапа."""

            def __init__(self, scene_context, *_args, **_kwargs):
                self.scene = scene_context

            def run(self):
                """Добавляет запуск этапа в журнал."""
                events.append((step_name, self.scene.tile))

        return Processor

    monkeypatch.setattr(pair_processor_module, "SentinelArchive", Archive)
    monkeypatch.setattr(
        pair_processor_module,
        "TileImageProcessor",
        processor("tile"),
    )
    monkeypatch.setattr(
        pair_processor_module,
        "AgroCropProcessor",
        processor("crop"),
    )
    monkeypatch.setattr(
        pair_processor_module,
        "MosaicProcessor",
        processor("combine"),
    )
    monkeypatch.setattr(
        pair_processor_module,
        "RescaleSCLProcessor",
        processor("rescale-scl"),
    )
    monkeypatch.setattr(
        pair_processor_module,
        "NdviStatisticsProcessor",
        processor("statistics"),
    )
    workspace = WorkspacePaths(*(Path(name) for name in (
        "temporary",
        "intermediate",
        "processed",
        "ndvi",
    )))
    pair_processor = pair_processor_module.SentinelPairProcessor(
        temporary_root="temporary",
        workspace=workspace,
        options=ProcessingOptions(3857, -9999.0),
        field_data=object(),
        geometry_exporter=object(),
    )
    pair_processor.process(pair(1))

    assert events == [
        ("extract", "t38ula"),
        ("tile", "t38ula"),
        ("crop", "t38ula"),
        ("extract", "t38ulb"),
        ("tile", "t38ulb"),
        ("crop", "t38ulb"),
        ("combine", "t38ula"),
        ("rescale-scl", "t38ula"),
        ("statistics", "t38ula"),
    ]

    events.clear()
    pair_processor.process(pair(1), target_agroids=(3,))

    assert events == [
        ("extract", "t38ula"),
        ("tile", "t38ula"),
        ("crop", "t38ula"),
        ("rescale-scl", "t38ula"),
        ("statistics", "t38ula"),
    ]

def test_processing_service_coordinates_ports_without_infrastructure():
    """Service координирует порты, не требуя реальной инфраструктуры."""

    class Finder:
        """Возвращает фиксированный список пар."""

        def find(self, _root, **_options):
            """Возвращает две тестовые пары."""
            return [pair(1), pair(2)]

    class Status:
        """Имитирует состояние публикации по дате."""

        def get_missing_agroids_many(self, acquired_dates):
            """Помечает первый день завершённым, а второй незавершённым."""
            return {
                acquired_on: ([] if acquired_on.day == 1 else [3])
                for acquired_on in acquired_dates
            }

    class Processor:
        """Запоминает переданные в обработку архивы."""

        def __init__(self):
            self.archives = []

        def process(self, archive_pair, target_agroids=None):
            """Регистрирует обработанную пару архивов."""
            self.archives.append((archive_pair, target_agroids))

    class Publisher:
        """Считает вызовы публикации."""

        calls = 0

        def publish_date(self, _acquired_on, _source):
            """Увеличивает счётчик публикаций."""
            self.calls += 1

    class Cleaner:
        """Считает вызовы очистки."""

        calls = 0

        def clean(self, _acquired_on):
            """Увеличивает счётчик очисток."""
            self.calls += 1

    processor = Processor()
    publisher = Publisher()
    cleaner = Cleaner()
    service = ProcessingService(
        archive_root="/archive",
        pair_finder=Finder(),
        status_reader=Status(),
        pair_processor=processor,
        publisher=publisher,
        cleaner=cleaner,
    )

    summary = service.run()

    assert processor.archives == [(pair(2), (3,))]
    assert publisher.calls == 1
    assert cleaner.calls == 1
    assert summary.discovered == 2
    assert summary.processed == 1
    assert summary.skipped == 1


def test_processing_service_preserves_failed_date_and_cleans_successful_date():
    """Service сохраняет staging упавшей даты и очищает успешную дату."""

    class Finder:
        """Возвращает две тестовые пары."""

        def find(self, _root, **_options):
            """Возвращает пары успешной и ошибочной дат."""
            return [pair(1), pair(2)]

    class Status:
        """Помечает все пары требующими обработки."""

        def get_missing_agroids_many(self, acquired_dates):
            """Возвращает незавершённое хозяйство для каждой даты."""
            return {acquired_on: [1] for acquired_on in acquired_dates}

    class Processor:
        """Имитирует ошибку первой даты."""

        def process(self, archive_pair, target_agroids=None):
            """Падает на паре архивов первого дня."""
            assert target_agroids == (1,)
            if archive_pair.acquired_on.day == 1:
                raise RuntimeError("broken scene")

    class Publisher:
        """Считает успешные публикации."""

        calls = 0

        def publish_date(self, _acquired_on, _source):
            """Увеличивает счётчик публикаций."""
            self.calls += 1

    class Cleaner:
        """Считает очистки после попыток."""

        calls = 0

        def clean(self, _acquired_on):
            """Увеличивает счётчик очисток успешных дат."""
            self.calls += 1

    publisher = Publisher()
    cleaner = Cleaner()
    service = ProcessingService(
        archive_root="/archive",
        pair_finder=Finder(),
        status_reader=Status(),
        pair_processor=Processor(),
        publisher=publisher,
        cleaner=cleaner,
    )

    with pytest.raises(ProcessingRunError, match="2026-07-01"):
        service.run()

    assert publisher.calls == 1
    assert cleaner.calls == 1


def test_recalculation_processes_completed_dates_and_cleans_before_work():
    """Перерасчёт не доверяет статусу публикации и начинает с чистого workspace."""

    class Finder:
        """Возвращает одну завершённую пару."""

        def find(self, _root, **_options):
            """Возвращает тестовую пару."""
            return [pair(1)]

    class Status:
        """Запрещает обращаться к статусу в принудительном режиме."""

        def get_missing_agroids_many(self, _acquired_dates):
            """Сообщает о недопустимом вызове."""
            raise AssertionError("Статус не должен ограничивать перерасчёт")

    events = []

    class Processor:
        """Фиксирует обработку архивов."""

        def process(self, archive_pair, target_agroids=None):
            """Добавляет пару архивов в журнал."""
            assert target_agroids is None
            events.append(("process", archive_pair))

    class Publisher:
        """Фиксирует публикацию даты."""

        def publish_date(self, acquired_on, _source):
            """Добавляет публикацию в журнал."""
            events.append(("publish", acquired_on))

    class Cleaner:
        """Фиксирует очистку до и после обработки."""

        def clean(self, acquired_on):
            """Добавляет очистку в журнал."""
            events.append(("clean", acquired_on))

    service = ProcessingService(
        archive_root="/archive",
        pair_finder=Finder(),
        status_reader=Status(),
        pair_processor=Processor(),
        publisher=Publisher(),
        cleaner=Cleaner(),
        process_completed=True,
        clean_before_each=True,
    )

    summary = service.run()

    assert events == [
        ("clean", date(2026, 7, 1)),
        ("process", pair(1)),
        ("publish", date(2026, 7, 1)),
        ("clean", date(2026, 7, 1)),
    ]
    assert summary.processed == 1
    assert summary.skipped == 0


def test_application_modules_do_not_import_infrastructure():
    """Чистые application-модули не импортируют инфраструктурные пакеты."""
    root = Path(__file__).parents[1]
    modules = [
        root / "processing" / "domain.py",
        root / "processing" / "discovery.py",
        root / "processing" / "layer_metadata.py",
        root / "processing" / "pair_processor.py",
        root / "processing" / "service.py",
        root / "processing" / "paths.py",
    ]
    forbidden = ("db", "osgeo", "psycopg2", "satgeo")

    for module in modules:
        tree = ast.parse(module.read_text(encoding="utf-8"))
        imports = [
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        ]
        imports.extend(
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        )
        assert not any(
            name == prefix or name.startswith(f"{prefix}.")
            for name in imports
            for prefix in forbidden
        ), module


def test_removed_facades_do_not_return():
    """Удалённые тонкие фасады не появляются в проекте повторно."""
    root = Path(__file__).parents[1]
    removed = [
        root / "db" / "db_class.py",
        root / "db" / "data_class.py",
        root / "db" / "connect_data.py",
        root / "processing" / "orchestrator.py",
        root / "processing" / "pipeline.py",
        root / "processing" / "processors" / "base.py",
        root / "satgeo" / "public.py",
        root / "satgeo" / "utils.py",
        root / "cdse" / "orchestrator.py",
        root / "core" / "utils.py",
        root / "core" / "const.py",
        root / "processing" / "coord.py",
        root / "processing" / "rastr.py",
        root / "processing" / "vector.py",
        root / "core" / "management" / "commands" / "clearprocessing.py",
        root / "core" / "management" / "commands" / "download.py",
        root / "core" / "management" / "commands" / "processing.py",
    ]

    assert not any(path.exists() for path in removed)


def test_application_settings_are_read_only_in_composition_roots():
    """Прикладные пакеты читают настройки только в composition roots."""
    root = Path(__file__).parents[1]
    allowed = {
        root / "cdse" / "composition.py",
        root / "processing" / "composition.py",
        root / "satgeo" / "composition.py",
    }

    offenders = []
    for package in ("cdse", "processing", "satgeo", "db", "domain"):
        for module in (root / package).rglob("*.py"):
            if module in allowed:
                continue
            source = module.read_text(encoding="utf-8")
            if "core.settings" in source or "from core import settings" in source:
                offenders.append(module)

    assert offenders == []


def test_core_does_not_depend_on_application_packages():
    """Общее ядро не импортирует прикладные и инфраструктурные пакеты."""
    root = Path(__file__).parents[1]
    forbidden = ("cdse", "db", "domain", "processing", "satgeo")
    offenders = []

    for module in (root / "core").rglob("*.py"):
        tree = ast.parse(module.read_text(encoding="utf-8"))
        imports = [
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        ]
        imports.extend(
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        )
        if any(
                name == prefix or name.startswith(f"{prefix}.")
                for name in imports
                for prefix in forbidden
        ):
            offenders.append(module)

    assert offenders == []


def test_dependencies_target_python313_and_numpy2():
    """Runtime-зависимости закрепляют единый стек Python 3.13 и NumPy 2."""
    root = Path(__file__).parents[1]
    requirements = (root / "requirements.txt").read_text(encoding="utf-8")
    project = (root / "pyproject.toml").read_text(encoding="utf-8")

    for dependency in (
            "numpy>=2.1,<3",
            "opencv-python-headless>=4.12,<5",
    ):
        assert dependency in requirements
        assert f'"{dependency}"' in project
    assert 'requires-python = ">=3.13"' in project
    assert "scipy" not in requirements
    assert "scipy" not in project


def test_python_sources_have_russian_docstrings():
    """Каждый модуль, класс и именованная функция имеют русский docstring."""
    root = Path(__file__).parents[1]
    sources = [root / "manage.py"]
    sources.extend((root / "scripts").rglob("*.py"))
    for package in (
            "core",
            "cdse",
            "cli",
            "db",
            "domain",
            "processing",
            "satgeo",
            "tests",
    ):
        sources.extend((root / package).rglob("*.py"))

    missing = []
    for source_path in sources:
        tree = ast.parse(source_path.read_text(encoding="utf-8"))
        objects = [("модуль", 1, source_path.name, tree)]
        objects.extend(
            (
                "класс" if isinstance(node, ast.ClassDef) else "функция",
                node.lineno,
                node.name,
                node,
            )
            for node in ast.walk(tree)
            if isinstance(
                node,
                (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef),
            )
            and not (
                node.name.startswith("__")
                and node.name.endswith("__")
            )
        )
        for kind, line, name, node in objects:
            docstring = ast.get_docstring(node, clean=False)
            if docstring is None or not re.search(r"[А-Яа-яЁё]", docstring):
                missing.append(
                    f"{source_path.relative_to(root)}:{line} "
                    f"{kind} {name}"
                )

    assert missing == []
