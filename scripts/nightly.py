"""Запуск ночного Sentinel pipeline с атомарным heartbeat."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DEFAULT_HEARTBEAT_FILE = Path("runtime/monitoring/sentinel.json")


def _utc_now() -> str:
    """Возвращает текущее время UTC в ISO 8601."""
    return datetime.now(UTC).isoformat()


def write_heartbeat(path: Path, payload: dict[str, Any]) -> None:
    """Атомарно заменяет heartbeat, не оставляя частично записанный JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    temporary.chmod(0o644)
    temporary.replace(path)


def run_nightly(
    *,
    project_root: Path,
    heartbeat_path: Path,
    python: str = sys.executable,
) -> int:
    """Выполняет download и processing, фиксируя этап и результат запуска."""
    started_at = _utc_now()
    commands: tuple[tuple[str, Sequence[str]], ...] = (
        (
            "download",
            (
                python,
                str(project_root / "manage.py"),
                "download",
                "--lookback-days",
                "3",
                "--download",
            ),
        ),
        (
            "processing",
            (python, str(project_root / "manage.py"), "processing"),
        ),
    )

    for stage, command in commands:
        write_heartbeat(
            heartbeat_path,
            {
                "status": "running",
                "stage": stage,
                "started_at": started_at,
            },
        )
        try:
            subprocess.run(command, cwd=project_root, check=True)
        except (OSError, subprocess.CalledProcessError) as exc:
            exit_code = getattr(exc, "returncode", 1) or 1
            write_heartbeat(
                heartbeat_path,
                {
                    "status": "error",
                    "stage": stage,
                    "started_at": started_at,
                    "finished_at": _utc_now(),
                    "exit_code": exit_code,
                },
            )
            return exit_code

    write_heartbeat(
        heartbeat_path,
        {
            "status": "ok",
            "stage": "completed",
            "started_at": started_at,
            "finished_at": _utc_now(),
        },
    )
    return 0


def main() -> None:
    """Запускает pipeline из корня текущего checkout и завершает процесс кодом результата."""
    project_root = Path(__file__).resolve().parents[1]
    configured_path = Path(
        os.environ.get("SENTINEL_HEARTBEAT_FILE", DEFAULT_HEARTBEAT_FILE)
    )
    heartbeat_path = (
        configured_path
        if configured_path.is_absolute()
        else project_root / configured_path
    )
    raise SystemExit(
        run_nightly(
            project_root=project_root,
            heartbeat_path=heartbeat_path,
        )
    )


if __name__ == "__main__":
    main()
