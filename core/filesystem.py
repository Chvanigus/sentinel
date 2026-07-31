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
