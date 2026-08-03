"""Небольшие безопасные операции с файловой системой."""
from __future__ import annotations

import shutil
from pathlib import Path

from core.logging import get_logger

logger = get_logger(__name__)


def clear_directory_contents(*directories: str | Path) -> None:
    """Удаляет содержимое явно перечисленных каталогов, сохраняя сами корни."""
    for value in directories:
        directory = Path(value).resolve()
        if directory == Path(directory.anchor):
            raise ValueError(f"Отказ очищать корень файловой системы: {value}")
        if not directory.exists():
            continue
        if not directory.is_dir():
            raise NotADirectoryError(directory)

        for child in directory.iterdir():
            if child.is_dir() and not child.is_symlink():
                shutil.rmtree(child)
            else:
                child.unlink()
            logger.info("Удалено: %s", child)


def clear_directory_entries_matching(
        directory: str | Path,
        *name_fragments: str,
) -> None:
    """Удаляет из каталога только элементы, содержащие заданные фрагменты имени."""
    root = Path(directory).resolve()
    if root == Path(root.anchor):
        raise ValueError(f"Отказ очищать корень файловой системы: {directory}")
    if not root.exists():
        return
    if not root.is_dir():
        raise NotADirectoryError(root)
    fragments = tuple(fragment for fragment in name_fragments if fragment)
    if not fragments:
        raise ValueError("Для выборочной очистки требуется фрагмент имени")

    for child in root.iterdir():
        if not any(fragment in child.name for fragment in fragments):
            continue
        if child.is_dir() and not child.is_symlink():
            shutil.rmtree(child)
        else:
            child.unlink()
        logger.info("Удалено: %s", child)
