"""Тесты безопасной и атомарной распаковки Sentinel-архивов."""

import zipfile
from pathlib import Path

import pytest

from processing.archive import ArchiveError, SentinelArchive

ARCHIVE_NAME = (
    "S2A_MSIL2A_20260701T081611_N0511_R121_T38ULA_"
    "20260701T120000.zip"
)
MEMBER_NAME = (
    "PRODUCT.SAFE/GRANULE/L2A_T38ULA/IMG_DATA/R10m/"
    "T38ULA_20260701T081611_TCI_10m.jp2"
)


def make_zip(path: Path, member: str = MEMBER_NAME) -> None:
    """Создаёт минимальный ZIP с указанным участником."""
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(member, b"jp2-content")


def test_archive_extracts_selected_jp2_atomically(tmp_path):
    """Архив извлекает выбранный канал без остаточного partial-каталога."""
    archive_path = tmp_path / ARCHIVE_NAME
    make_zip(archive_path)
    output_root = tmp_path / "output"

    extracted = SentinelArchive(archive_path).extract(
        output_root,
        ("TCI",),
    )

    assert extracted.is_dir()
    assert list(extracted.rglob("*.jp2"))
    assert not Path(f"{extracted}.partial").exists()


def test_archive_does_not_keep_empty_partial_extraction(tmp_path):
    """Неудачная распаковка удаляет пустой временный каталог."""
    archive_path = tmp_path / ARCHIVE_NAME
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("PRODUCT.SAFE/manifest.safe", b"metadata")
    output_root = tmp_path / "output"

    with pytest.raises(ArchiveError, match="требуемые каналы"):
        SentinelArchive(archive_path).extract(output_root, ("TCI",))

    expected = output_root / f"{archive_path.stem}.SAFE"
    assert not expected.exists()
    assert not Path(f"{expected}.partial").exists()


def test_archive_rejects_path_traversal(tmp_path):
    """Распаковщик блокирует выход пути участника за целевой каталог."""
    archive_path = tmp_path / ARCHIVE_NAME
    unsafe_member = (
        "PRODUCT.SAFE/IMG_DATA/../../../"
        "T38ULA_20260701T081611_TCI_10m.jp2"
    )
    make_zip(archive_path, unsafe_member)
    output_root = tmp_path / "output"

    with pytest.raises(ArchiveError, match="Небезопасный путь"):
        SentinelArchive(archive_path).extract(output_root, ("TCI",))

    assert not (tmp_path / "T38ULA_20260701T081611_TCI_10m.jp2").exists()


def test_archive_replaces_incomplete_destination(tmp_path):
    """Неполный старый результат заменяется новой корректной распаковкой."""
    archive_path = tmp_path / ARCHIVE_NAME
    make_zip(archive_path)
    output_root = tmp_path / "output"
    stale = output_root / f"{archive_path.stem}.SAFE"
    stale.mkdir(parents=True)

    extracted = SentinelArchive(archive_path).extract(
        output_root,
        ("TCI",),
    )

    assert extracted.is_dir()
    assert list(extracted.rglob("*.jp2"))


def test_archive_reports_invalid_name():
    """Некорректное имя архива даёт понятную ошибку метаданных."""
    with pytest.raises(ValueError, match="спутник"):
        SentinelArchive("not-a-sentinel-product.zip")
