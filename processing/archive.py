"""Чтение метаданных и безопасная распаковка Sentinel SAFE ZIP."""
from __future__ import annotations

import re
import shutil
import zipfile
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path, PurePosixPath

from core.logging import get_logger


class ArchiveError(RuntimeError):
    """Ошибка чтения или распаковки Sentinel-архива."""


@dataclass(frozen=True)
class ArchiveMetadata:
    """Метаданные, извлечённые из стандартного имени Sentinel-архива."""

    satellite: str
    date: date
    tile: str
    level: str


class SentinelArchive:
    """Один Sentinel ZIP: метаданные и атомарная выборочная распаковка."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.logger = get_logger(self.__class__.__name__)
        self.metadata = self._parse_metadata(self.path.name)

    @staticmethod
    def _required_match(filename: str, pattern: str, label: str) -> str:
        """Возвращает обязательный фрагмент имени или сообщает об ошибке."""
        match = re.search(pattern, filename)
        if match is None:
            raise ValueError(
                f"Не удалось определить {label} из имени {filename!r}"
            )
        return match.group()

    @classmethod
    def _parse_metadata(cls, filename: str) -> ArchiveMetadata:
        """Разбирает спутник, дату, тайл и уровень обработки из имени ZIP."""
        satellite = cls._required_match(
            filename, r"^S[1-9][A-Z]", "спутник"
        )
        acquired_on = datetime.strptime(
            cls._required_match(filename, r"20\d{6}", "дату"),
            "%Y%m%d",
        ).date()
        tile = cls._required_match(
            filename, r"T\d{2}[A-Z]{3}", "тайл"
        )
        level = cls._required_match(
            filename, r"MSIL[1-2][A-C]", "уровень"
        )
        return ArchiveMetadata(satellite, acquired_on, tile, level)

    @staticmethod
    def _matches_band(
            member_name: str,
            band: str,
            level: str,
    ) -> bool:
        """Проверяет, соответствует ли JP2 требуемому каналу и разрешению."""
        name = Path(member_name).name.lower()
        member = member_name.lower()
        target = band.lower()
        if not (
                f"_{target}_" in name
                or name.endswith(f"_{target}.jp2")
                or f"_{target}_" in member
        ):
            return False
        if level == "L2A":
            expected_resolution = "r20m" if target == "scl" else "r10m"
            return expected_resolution in member
        return target != "scl"

    def _existing_is_complete(
            self,
            destination: Path,
            required_bands: tuple[str, ...],
            level: str,
    ) -> bool:
        """Проверяет полноту ранее распакованного каталога."""
        files = [
            path.as_posix()
            for path in destination.rglob("*.jp2")
            if path.is_file()
        ]
        return bool(files) and all(
            any(self._matches_band(path, band, level) for path in files)
            for band in required_bands
        )

    def extract(
            self,
            destination_root: str | Path,
            required_bands: tuple[str, ...],
    ) -> Path:
        """Безопасно и атомарно распаковывает необходимые JP2-каналы."""
        if not self.path.is_file() or self.path.stat().st_size == 0:
            raise ArchiveError(f"ZIP не найден или пуст: {self.path}")
        if not zipfile.is_zipfile(self.path):
            raise ArchiveError(f"Файл повреждён или не является ZIP: {self.path}")

        level = "L1C" if self.metadata.level == "MSIL1C" else "L2A"
        stem = self.path.stem
        safe_name = stem if stem.upper().endswith(".SAFE") else f"{stem}.SAFE"
        destination = Path(destination_root) / safe_name
        required = tuple(required_bands)

        if destination.exists():
            if self._existing_is_complete(destination, required, level):
                return destination
            self.logger.warning(
                "Удаляется неполная распаковка: %s",
                destination,
            )
            shutil.rmtree(destination)

        partial = destination.with_name(f"{destination.name}.partial")
        if partial.exists():
            shutil.rmtree(partial)
        partial.mkdir(parents=True)
        extracted = 0

        try:
            with zipfile.ZipFile(self.path) as source_zip:
                for member in source_zip.infolist():
                    parts = PurePosixPath(member.filename).parts
                    if ".." in parts:
                        raise ArchiveError(
                            f"Небезопасный путь внутри ZIP: {member.filename}"
                        )
                    if (
                            "IMG_DATA" not in member.filename
                            or not member.filename.lower().endswith(".jp2")
                    ):
                        continue
                    if not any(
                            self._matches_band(
                                member.filename,
                                band,
                                level,
                            )
                            for band in required
                    ):
                        continue

                    if parts and parts[0].upper().endswith(".SAFE"):
                        parts = parts[1:]
                    target = (partial / Path(*parts)).resolve()
                    partial_root = partial.resolve()
                    if partial_root not in target.parents:
                        raise ArchiveError(
                            f"Небезопасный путь внутри ZIP: {member.filename}"
                        )

                    target.parent.mkdir(parents=True, exist_ok=True)
                    with source_zip.open(member) as source, target.open(
                            "wb"
                    ) as output:
                        shutil.copyfileobj(
                            source,
                            output,
                            length=16 * 1024 * 1024,
                        )
                    extracted += 1

            if extracted == 0:
                raise ArchiveError(
                    f"В архиве не найдены требуемые каналы: {required}"
                )
            partial.replace(destination)
            return destination
        except Exception:
            shutil.rmtree(partial, ignore_errors=True)
            raise
