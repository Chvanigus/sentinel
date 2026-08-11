"""Тесты ночного Sentinel pipeline и heartbeat."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import patch

from scripts.nightly import run_nightly, write_heartbeat


def test_write_heartbeat_replaces_json_atomically(tmp_path: Path):
    """Heartbeat сохраняется валидным JSON без временного файла."""
    heartbeat = tmp_path / "runtime" / "sentinel.json"

    write_heartbeat(heartbeat, {"status": "running", "stage": "download"})

    assert json.loads(heartbeat.read_text("utf-8")) == {
        "stage": "download",
        "status": "running",
    }
    assert list(heartbeat.parent.glob("*.tmp")) == []


def test_run_nightly_reports_successful_stages(tmp_path: Path):
    """Успешный запуск последовательно выполняет download и processing."""
    heartbeat = tmp_path / "sentinel.json"
    project_root = tmp_path / "sentinel"
    project_root.mkdir()

    with patch("scripts.nightly.subprocess.run") as run:
        exit_code = run_nightly(
            project_root=project_root,
            heartbeat_path=heartbeat,
            python="python",
        )

    assert exit_code == 0
    assert [call.args[0][2] for call in run.call_args_list] == [
        "download",
        "processing",
    ]
    payload = json.loads(heartbeat.read_text("utf-8"))
    assert payload["status"] == "ok"
    assert payload["stage"] == "completed"
    assert payload["finished_at"]


def test_run_nightly_persists_failed_stage_and_exit_code(tmp_path: Path):
    """Ошибка команды остаётся в heartbeat и возвращается systemd."""
    heartbeat = tmp_path / "sentinel.json"
    project_root = tmp_path / "sentinel"
    project_root.mkdir()

    with patch(
        "scripts.nightly.subprocess.run",
        side_effect=subprocess.CalledProcessError(7, ["python"]),
    ):
        exit_code = run_nightly(
            project_root=project_root,
            heartbeat_path=heartbeat,
            python="python",
        )

    payload = json.loads(heartbeat.read_text("utf-8"))
    assert exit_code == 7
    assert payload["status"] == "error"
    assert payload["stage"] == "download"
    assert payload["exit_code"] == 7
