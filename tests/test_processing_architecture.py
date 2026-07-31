"""Архитектурные и orchestration-тесты processing application layer."""

import ast
import re
from datetime import date, datetime
from pathlib import Path

import pytest

from processing.archive import ArchiveMetadata
from processing.discovery import ArchiveName, ArchivePairFinder
from processing.domain import (
    ArchivePair,
    ProductLevel,
    SceneContext,
)
from processing.exceptions import ProcessingRunError, ProcessingStepError
from processing.pipeline import ScenePipeline, SceneStep
from processing.service import ProcessingService


def scene() -> SceneContext:
    """Создаёт тестовый контекст L2A-сцены."""
    return SceneContext(
        archive_path=Path("scene.zip"),
        tile="t38ula",
        acquired_on=date(2026, 7, 1),
        satellite="s2a",
        level=ProductLevel.L2A,
        agroids=(1, 3, 4),
    )


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
        zip_iterator=lambda _root: archives,
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
        zip_iterator=lambda _root: archives,
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


def test_scene_pipeline_preserves_order_and_names_failure():
    """Pipeline сохраняет порядок и указывает имя упавшего шага."""
    calls = []
    pipeline = ScenePipeline(
        [
            SceneStep("first", lambda _scene: calls.append("first")),
            SceneStep("second", lambda _scene: False),
        ]
    )

    with pytest.raises(ProcessingStepError, match="second"):
        pipeline.run(scene())

    assert calls == ["first"]


def test_processing_service_coordinates_ports_without_infrastructure():
    """Service координирует порты, не требуя реальной инфраструктуры."""

    class Finder:
        """Возвращает фиксированный список пар."""

        def find(self, _root):
            """Возвращает две тестовые пары."""
            return [pair(1), pair(2)]

    class Status:
        """Имитирует состояние публикации по дате."""

        def get_missing_agroids(self, acquired_on):
            """Помечает первый день завершённым, а второй незавершённым."""
            return [] if acquired_on.day == 1 else [3]

    class Processor:
        """Запоминает переданные в обработку архивы."""

        def __init__(self):
            self.archives = []

        def process(self, archive):
            """Регистрирует обработанный архив."""
            self.archives.append(archive)

    class Publisher:
        """Считает вызовы публикации."""

        calls = 0

        def publish_date(self, _acquired_on):
            """Увеличивает счётчик публикаций."""
            self.calls += 1

    class Cleaner:
        """Считает вызовы очистки."""

        calls = 0

        def clean(self):
            """Увеличивает счётчик очисток."""
            self.calls += 1

    processor = Processor()
    publisher = Publisher()
    cleaner = Cleaner()
    service = ProcessingService(
        archive_root="/archive",
        pair_finder=Finder(),
        status_reader=Status(),
        scene_processor=processor,
        publisher=publisher,
        cleaner=cleaner,
    )

    summary = service.run()

    assert processor.archives == [Path("2-ula.zip"), Path("2-ulb.zip")]
    assert publisher.calls == 1
    assert cleaner.calls == 1
    assert summary.discovered == 2
    assert summary.processed == 1
    assert summary.skipped == 1


def test_processing_service_collects_failures_and_cleans_each_date():
    """Service агрегирует ошибки и очищает workspace после каждой даты."""

    class Finder:
        """Возвращает две тестовые пары."""

        def find(self, _root):
            """Возвращает пары успешной и ошибочной дат."""
            return [pair(1), pair(2)]

    class Status:
        """Помечает все пары требующими обработки."""

        def get_missing_agroids(self, _acquired_on):
            """Возвращает незавершённое хозяйство."""
            return [1]

    class Processor:
        """Имитирует ошибку первой даты."""

        def process(self, archive):
            """Падает на архивах первого дня."""
            if archive.name.startswith("1-"):
                raise RuntimeError("broken scene")

    class Publisher:
        """Считает успешные публикации."""

        calls = 0

        def publish_date(self, _acquired_on):
            """Увеличивает счётчик публикаций."""
            self.calls += 1

    class Cleaner:
        """Считает очистки после попыток."""

        calls = 0

        def clean(self):
            """Увеличивает счётчик очисток."""
            self.calls += 1

    publisher = Publisher()
    cleaner = Cleaner()
    service = ProcessingService(
        archive_root="/archive",
        pair_finder=Finder(),
        status_reader=Status(),
        scene_processor=Processor(),
        publisher=publisher,
        cleaner=cleaner,
    )

    with pytest.raises(ProcessingRunError, match="2026-07-01"):
        service.run()

    assert publisher.calls == 1
    assert cleaner.calls == 2


def test_application_modules_do_not_import_infrastructure():
    """Чистые application-модули не импортируют инфраструктурные пакеты."""
    root = Path(__file__).parents[1]
    modules = [
        root / "processing" / "domain.py",
        root / "processing" / "discovery.py",
        root / "processing" / "pipeline.py",
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


def test_python39_union_annotations_are_deferred():
    """PEP 604-аннотации не вычисляются при импорте модулей в Python 3.9."""
    root = Path(__file__).parents[1]
    offenders = []

    for package in (
            "cdse",
            "cli",
            "core",
            "db",
            "domain",
            "processing",
            "satgeo",
    ):
        for module in (root / package).rglob("*.py"):
            tree = ast.parse(module.read_text(encoding="utf-8"))
            has_future_annotations = any(
                isinstance(node, ast.ImportFrom)
                and node.module == "__future__"
                and any(
                    alias.name == "annotations"
                    for alias in node.names
                )
                for node in tree.body
            )
            uses_union = any(
                isinstance(part, ast.BinOp)
                and isinstance(part.op, ast.BitOr)
                for node in ast.walk(tree)
                for annotation in _annotations(node)
                for part in ast.walk(annotation)
            )
            if uses_union and not has_future_annotations:
                offenders.append(module)

    assert offenders == []


def test_python39_module_type_aliases_do_not_evaluate_pep604():
    """Module-level type aliases не вычисляют PEP 604 union в Python 3.9."""
    root = Path(__file__).parents[1]
    offenders = []

    for package in (
            "cdse",
            "cli",
            "core",
            "db",
            "domain",
            "processing",
            "satgeo",
    ):
        for module in (root / package).rglob("*.py"):
            tree = ast.parse(module.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, ast.Assign):
                    values = [node.value]
                elif (
                        isinstance(node, ast.AnnAssign)
                        and node.value is not None
                ):
                    values = [node.value]
                else:
                    values = []
                if any(
                        isinstance(part, ast.BinOp)
                        and isinstance(part.op, ast.BitOr)
                        for value in values
                        for part in ast.walk(value)
                ):
                    offenders.append(f"{module}:{node.lineno}")

    assert offenders == []


def _annotations(node: ast.AST) -> list[ast.expr]:
    """Возвращает аннотации узла, вычисляемые при импорте модуля."""
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        result = [
            argument.annotation
            for argument in (
                *node.args.posonlyargs,
                *node.args.args,
                *node.args.kwonlyargs,
            )
            if argument.annotation is not None
        ]
        if node.args.vararg and node.args.vararg.annotation:
            result.append(node.args.vararg.annotation)
        if node.args.kwarg and node.args.kwarg.annotation:
            result.append(node.args.kwarg.annotation)
        if node.returns:
            result.append(node.returns)
        return result
    if isinstance(node, ast.AnnAssign):
        return [node.annotation]
    return []


def test_dependencies_preserve_server_gdal_numpy_abi():
    """Runtime-зависимости сохраняют совместимость с серверным GDAL ABI."""
    root = Path(__file__).parents[1]
    requirements = (root / "requirements.txt").read_text(encoding="utf-8")
    project = (root / "pyproject.toml").read_text(encoding="utf-8")

    for dependency in (
            "numpy>=1.26,<2",
            "opencv-python-headless>=4.8,<4.12",
    ):
        assert dependency in requirements
        assert f'"{dependency}"' in project


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
