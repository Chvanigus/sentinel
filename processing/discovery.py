"""Поиск пар архивов в локальном хранилище."""
from __future__ import annotations

import os
import re
from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .domain import ArchivePair, ProductLevel

ZipIterator = Callable[..., Iterable[str]]

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
    level: ProductLevel = ProductLevel.L2A
    processing_baseline: int | None = None


ZipNameParser = Callable[[str], ArchiveName | None]


def parse_archive_name(path: str) -> ArchiveName | None:
    """Разбирает поддерживаемое имя Sentinel ZIP."""
    match = _ARCHIVE_NAME.fullmatch(Path(path).name)
    if match is None:
        return None
    filename = Path(path).name
    level_match = re.search(r"MSIL[12][AC]", filename, re.IGNORECASE)
    if level_match is None:
        return None
    baseline_match = re.search(
        r"_N(?P<baseline>\d{4})_",
        filename,
        re.IGNORECASE,
    )
    return ArchiveName(
        satellite=match.group("satellite").lower(),
        acquired_at=datetime.strptime(
            match.group("acquired"),
            "%Y%m%dT%H%M%S",
        ),
        tile=match.group("tile").lower(),
        level=ProductLevel.parse(level_match.group()),
        processing_baseline=(
            int(baseline_match.group("baseline"))
            if baseline_match is not None
            else None
        ),
    )


def iter_archive_files(
        root: str,
        *,
        years: tuple[int, ...] = (),
) -> Iterable[str]:
    """Перечисляет ZIP-файлы архива, ограничиваясь нужными годами."""
    archive_root = Path(root)
    scan_roots = (
        [archive_root / str(year) for year in years]
        if years
        else [archive_root]
    )
    for scan_root in scan_roots:
        if not scan_root.exists():
            continue
        for current_root, _, files in os.walk(scan_root):
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

    def find(
            self,
            archive_root: str | Path,
            *,
            start_date: datetime | None = None,
            end_date: datetime | None = None,
    ) -> list[ArchivePair]:
        """Возвращает крупнейшие полные пары выбранного периода."""
        grouped: dict[
            tuple[str, datetime, str, ProductLevel, int | None],
            dict[str, list[Path]],
        ] = defaultdict(lambda: defaultdict(list))

        years = self._period_years(start_date, end_date)
        for zip_path in self._zip_iterator(
                str(archive_root),
                years=years,
        ):
            parsed = self._name_parser(zip_path)
            if parsed is None:
                continue
            acquired_on = parsed.acquired_at.replace(
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
                tzinfo=None,
            )
            if start_date is not None and acquired_on < start_date:
                continue
            if end_date is not None and acquired_on >= end_date:
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
                (
                    parsed.satellite,
                    parsed.acquired_at,
                    prefix,
                    parsed.level,
                    parsed.processing_baseline,
                )
            ][side].append(Path(zip_path))

        candidates = [
            ArchivePair(
                acquired_at=acquired_at,
                prefix=prefix,
                ula=max(sides["ula"], key=self._archive_rank),
                ulb=max(sides["ulb"], key=self._archive_rank),
                level=level,
                processing_baseline=baseline,
                satellite=satellite,
            )
            for (
                satellite,
                acquired_at,
                prefix,
                level,
                baseline,
            ), sides in grouped.items()
            if "ula" in sides and "ulb" in sides
        ]

        best: dict[tuple[object, str], ArchivePair] = {}
        for pair in candidates:
            key = (pair.acquired_on, pair.prefix)
            current = best.get(key)
            if current is None or self._pair_rank(pair) > self._pair_rank(current):
                best[key] = pair
        return sorted(best.values(), key=lambda pair: pair.acquired_at)

    @staticmethod
    def _period_years(
            start_date: datetime | None,
            end_date: datetime | None,
    ) -> tuple[int, ...]:
        """Возвращает каталоги лет, которые могут пересекать период."""
        if start_date is None or end_date is None:
            return ()
        return tuple(range(start_date.year, end_date.year + 1))

    @staticmethod
    def _archive_rank(path: Path) -> tuple[int, str]:
        """Сравнивает повторные публикации по размеру и стабильному имени."""
        try:
            size = path.stat().st_size
        except OSError:
            size = 0
        return size, path.name

    @classmethod
    def _pair_rank(cls, pair: ArchivePair) -> tuple[int, int, int, datetime]:
        """Предпочитает L2A, затем крупнейший и наиболее новый комплект."""
        return (
            int(pair.level is ProductLevel.L2A),
            cls._archive_rank(pair.ula)[0] + cls._archive_rank(pair.ulb)[0],
            pair.processing_baseline or -1,
            pair.acquired_at,
        )
