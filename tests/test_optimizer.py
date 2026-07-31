"""Тесты атомарной COG-оптимизации растров."""

import subprocess
from pathlib import Path

import pytest

from satgeo.optimizer import optimize_geotiff


def test_optimizer_writes_temporary_cog_and_replaces_destination(
        tmp_path,
        monkeypatch,
):
    """Оптимизатор создаёт COG во временном файле и заменяет результат."""
    source = tmp_path / "source.tif"
    destination = tmp_path / "result.tif"
    source.write_bytes(b"source")
    calls = []

    def run(command):
        """Имитирует успешный gdal_translate."""
        calls.append(command)
        Path(command[-1]).write_bytes(b"optimized")

    monkeypatch.setattr(subprocess, "check_call", run)

    optimize_geotiff(source, destination)

    assert destination.read_bytes() == b"optimized"
    assert "NUM_THREADS=ALL_CPUS" in calls[0]
    assert not destination.with_suffix(".tif.tmp").exists()


def test_optimizer_retries_failed_gdal_call(tmp_path, monkeypatch):
    """Временная ошибка gdal_translate приводит к повторной попытке."""
    source = tmp_path / "source.tif"
    destination = tmp_path / "result.tif"
    source.write_bytes(b"source")
    attempts = []
    delays = []

    def run(command):
        """Падает один раз, затем создаёт оптимизированный файл."""
        attempts.append(command)
        if len(attempts) == 1:
            raise subprocess.CalledProcessError(1, command)
        Path(command[-1]).write_bytes(b"optimized")

    monkeypatch.setattr(subprocess, "check_call", run)
    monkeypatch.setattr("satgeo.optimizer.time.sleep", delays.append)

    optimize_geotiff(source, destination, retries=2, delay=0.1)

    assert len(attempts) == 2
    assert delays == [0.1]
    assert destination.is_file()


def test_optimizer_rejects_non_positive_retries(tmp_path):
    """Неположительное число попыток отклоняется до запуска GDAL."""
    with pytest.raises(ValueError, match="положительным"):
        optimize_geotiff(
            tmp_path / "source.tif",
            tmp_path / "result.tif",
            retries=0,
        )
