"""Поиск пар архивов в локальном хранилище."""
from __future__ import annotations

import os
import re
from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from .domain import ArchivePair

ZipIterator = Callable[[str], Iterable[str]]

_ARCHIVE_NAME = re.compile(
    r"^(?P<satellite>S[1-9][A-Z])_"
    r".*?_(?P<acquired>\d{8}T\d{6})"
    r".*?_(?P<tile>T\d{2}[A-Z]{3})_.*\.zip$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ArchiveName:
    """Части имени архива, необходимые для формирования пары тайлов."""

    satellite: str
    acquired_at: datetime
    tile: str


ZipNameParser = Callable[[str], Optional[ArchiveName]]


def parse_archive_name(path: str) -> ArchiveName | None:
    """Разбирает поддерживаемое имя Sentinel ZIP."""
    match = _ARCHIVE_NAME.fullmatch(Path(path).name)
    if match is None:
        return None
    return ArchiveName(
        satellite=match.group("satellite").lower(),
        acquired_at=datetime.strptime(
            match.group("acquired"),
            "%Y%m%dT%H%M%S",
        ),
        tile=match.group("tile").lower(),
    )


def iter_archive_files(root: str) -> Iterable[str]:
    """Рекурсивно перечисляет ZIP-файлы архива снимков."""
    for current_root, _, files in os.walk(root):
        for filename in files:
            if filename.lower().endswith(".zip"):
                yield str(Path(current_root) / filename)


class ArchivePairFinder:
    """Находит полные пары ULA/ULB и не знает ничего о БД или GDAL."""

    def __init__(
            self,
            zip_iterator: ZipIterator = iter_archive_files,
            name_parser: ZipNameParser = parse_archive_name,
    ):
        self._zip_iterator = zip_iterator
        self._name_parser = name_parser

    def find(self, archive_root: str | Path) -> list[ArchivePair]:
        """Возвращает полные пары одной съёмки, не смешивая спутники и время."""
        grouped: dict[
            tuple[str, datetime, str],
            dict[str, list[Path]],
        ] = defaultdict(lambda: defaultdict(list))

        for zip_path in self._zip_iterator(str(archive_root)):
            parsed = self._name_parser(zip_path)
            if parsed is None:
                continue

            tile = parsed.tile
            if tile.endswith("ula"):
                side = "ula"
            elif tile.endswith("ulb"):
                side = "ulb"
            else:
                continue

            prefix = tile[:-3]
            grouped[
                (parsed.satellite, parsed.acquired_at, prefix)
            ][side].append(Path(zip_path))

        pairs = [
            ArchivePair(
                acquired_at=acquired_at,
                prefix=prefix,
                ula=max(sides["ula"], key=lambda path: path.name),
                ulb=max(sides["ulb"], key=lambda path: path.name),
            )
            for (_satellite, acquired_at, prefix), sides in grouped.items()
            if "ula" in sides and "ulb" in sides
        ]
        return sorted(pairs, key=lambda pair: pair.acquired_at)
