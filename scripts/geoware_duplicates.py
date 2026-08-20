"""Аудит и безопасная очистка дублей месячных каталогов geoware."""
from __future__ import annotations

import argparse
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path, PurePosixPath
from urllib.parse import quote, unquote

import requests

_RASTER_NAME = re.compile(
    r"^a(?P<agroid>\d+)_(?P<product>ndvi|ndwi|scl|tci)_"
    r"(?P<acquired_on>\d{4}-\d{2}-\d{2})[.]tif$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class DuplicateGroup:
    """Копии одного логического растра в разных каталогах месяца."""

    store_name: str
    paths: tuple[Path, ...]


@dataclass(frozen=True)
class StoreReference:
    """URL coverage store либо безопасно зафиксированная ошибка чтения."""

    url: str | None = None
    error: str | None = None


@dataclass(frozen=True)
class CleanupDecision:
    """Решение о сохраняемой и удаляемых копиях одного растра."""

    store_name: str
    active_url: str | None
    keep: str | None
    remove: tuple[str, ...]
    status: str
    reason: str | None = None


def discover_duplicate_groups(root: Path) -> list[DuplicateGroup]:
    """Находит TIFF, различающиеся только записью месяца ``7``/``07``."""
    grouped: dict[tuple[str, str, str, int, str], list[Path]] = {}
    for path in root.glob("*/*/*/*/*.tif"):
        if not path.is_file() or path.is_symlink():
            continue
        relative = path.relative_to(root)
        if len(relative.parts) != 5:
            continue
        year_dir, agro_dir, product_dir, month_dir, filename = relative.parts
        match = _RASTER_NAME.fullmatch(filename)
        if match is None or not month_dir.isdigit():
            continue
        try:
            acquired_on = date.fromisoformat(match.group("acquired_on"))
            month = int(month_dir)
        except ValueError:
            continue
        if not (
                year_dir == str(acquired_on.year)
                and month == acquired_on.month
                and agro_dir.casefold() == f"a{match.group('agroid')}"
                and product_dir.casefold() == match.group("product").casefold()
        ):
            continue
        key = (
            year_dir,
            agro_dir.casefold(),
            product_dir.casefold(),
            month,
            filename.casefold(),
        )
        grouped.setdefault(key, []).append(path)

    result = []
    for paths in grouped.values():
        unique = tuple(sorted(set(paths), key=lambda item: item.as_posix()))
        month_names = {path.parent.name for path in unique}
        if len(unique) < 2 or len(month_names) < 2:
            continue
        result.append(DuplicateGroup(
            store_name=f"{unique[0].stem}_store",
            paths=unique,
        ))
    return sorted(result, key=lambda item: item.store_name)


def fetch_store_reference(
        store_name: str,
        *,
        base_url: str,
        workspace: str,
        username: str,
        password: str,
        timeout: float,
) -> StoreReference:
    """Читает URL одного coverage store через GeoServer REST API."""
    endpoint = (
        f"{base_url}/rest/workspaces/{quote(workspace, safe='')}"
        f"/coveragestores/{quote(store_name, safe='')}.json"
    )
    try:
        response = requests.get(
            endpoint,
            auth=(username, password),
            timeout=timeout,
        )
        response.raise_for_status()
        url = response.json().get("coverageStore", {}).get("url")
        if not isinstance(url, str) or not url.strip():
            return StoreReference(error="coverage_store_url_missing")
        return StoreReference(url=url)
    except Exception as exc:
        return StoreReference(error=f"{type(exc).__name__}: {exc}")


def fetch_store_references(
        store_names: list[str],
        *,
        base_url: str,
        workspace: str,
        username: str,
        password: str,
        timeout: float = 15.0,
        workers: int = 8,
) -> dict[str, StoreReference]:
    """Параллельно получает ссылки только для найденных групп дублей."""
    references: dict[str, StoreReference] = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                fetch_store_reference,
                store_name,
                base_url=base_url,
                workspace=workspace,
                username=username,
                password=password,
                timeout=timeout,
            ): store_name
            for store_name in store_names
        }
        for future in as_completed(futures):
            references[futures[future]] = future.result()
    return references


def store_url_to_host_path(
        url: str,
        *,
        host_root: Path,
        container_root: str | Path,
) -> Path:
    """Преобразует GeoServer ``file:`` URL в путь хоста внутри geoware."""
    decoded = unquote(url.strip())
    if not decoded.startswith("file:"):
        raise ValueError("coverage store использует не file: URL")
    container_path = PurePosixPath(decoded.removeprefix("file:"))
    configured_root = PurePosixPath(str(container_root).replace("\\", "/"))
    try:
        relative = container_path.relative_to(configured_root)
    except ValueError as exc:
        raise ValueError("coverage store указывает вне GS_DATA_DIR") from exc
    return host_root.joinpath(*relative.parts)


def _path_key(path: Path) -> str:
    """Возвращает нормализованный абсолютный ключ пути без чтения файла."""
    return os.path.normcase(os.path.abspath(path))


def build_cleanup_decisions(
        groups: list[DuplicateGroup],
        references: dict[str, StoreReference],
        *,
        host_root: Path,
        container_root: str | Path,
) -> list[CleanupDecision]:
    """Оставляет только копию, на которую явно ссылается GeoServer."""
    decisions = []
    for group in groups:
        reference = references.get(
            group.store_name,
            StoreReference(error="coverage_store_not_checked"),
        )
        if reference.error or reference.url is None:
            decisions.append(CleanupDecision(
                store_name=group.store_name,
                active_url=None,
                keep=None,
                remove=(),
                status="skipped",
                reason=reference.error,
            ))
            continue
        try:
            active_path = store_url_to_host_path(
                reference.url,
                host_root=host_root,
                container_root=container_root,
            )
        except ValueError as exc:
            decisions.append(CleanupDecision(
                store_name=group.store_name,
                active_url=reference.url,
                keep=None,
                remove=(),
                status="skipped",
                reason=str(exc),
            ))
            continue
        by_key = {_path_key(path): path for path in group.paths}
        keep = by_key.get(_path_key(active_path))
        if keep is None:
            decisions.append(CleanupDecision(
                store_name=group.store_name,
                active_url=reference.url,
                keep=None,
                remove=(),
                status="skipped",
                reason="active_store_path_not_found_among_duplicates",
            ))
            continue
        decisions.append(CleanupDecision(
            store_name=group.store_name,
            active_url=reference.url,
            keep=str(keep),
            remove=tuple(str(path) for path in group.paths if path != keep),
            status="ready",
        ))
    return decisions


def apply_cleanup(decisions: list[CleanupDecision], root: Path) -> int:
    """Удаляет только проверенные неактивные копии и пустые каталоги."""
    root_key = _path_key(root)
    removed = 0
    for decision in decisions:
        if decision.status != "ready" or decision.keep is None:
            continue
        keep = Path(decision.keep)
        if not keep.is_file():
            raise RuntimeError(f"Активный TIFF исчез перед очисткой: {keep}")
        for value in decision.remove:
            path = Path(value)
            if (
                    os.path.commonpath((root_key, _path_key(path))) != root_key
                    or path.suffix.casefold() != ".tif"
                    or path.is_symlink()
                    or not path.is_file()
            ):
                raise RuntimeError(f"Небезопасная цель удаления: {path}")
            path.unlink()
            removed += 1
            try:
                path.parent.rmdir()
            except OSError:
                pass
    return removed


def write_report(
        destination: Path,
        *,
        root: Path,
        apply: bool,
        decisions: list[CleanupDecision],
) -> None:
    """Атомарно записывает JSON-манифест аудита и очистки."""
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "root": str(root),
        "apply": apply,
        "duplicate_groups": len(decisions),
        "ready_groups": sum(item.status == "ready" for item in decisions),
        "skipped_groups": sum(item.status == "skipped" for item in decisions),
        "files_to_remove": sum(len(item.remove) for item in decisions),
        "decisions": [asdict(item) for item in decisions],
    }
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary.replace(destination)


def build_parser() -> argparse.ArgumentParser:
    """Создаёт CLI с безопасным dry-run по умолчанию."""
    parser = argparse.ArgumentParser(
        description=(
            "Удаляет только дубли TIFF, не используемые coverage store "
            "GeoServer. Без --apply выполняется dry-run."
        ),
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--timeout", type=float, default=15.0)
    parser.add_argument("--report", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Аудитирует geoware и при подтверждении удаляет неактивные копии."""
    from core import settings

    options = build_parser().parse_args(argv)
    if options.workers <= 0 or options.timeout <= 0:
        raise ValueError("--workers и --timeout должны быть положительными")
    root = Path(settings.GS_DATA_ROOT)
    groups = discover_duplicate_groups(root)
    references = fetch_store_references(
        [group.store_name for group in groups],
        base_url=f"http://{settings.GS_HOST}/geoserver",
        workspace=settings.GS_WORKSPACE,
        username=settings.GS_USERNAME,
        password=settings.GS_PASSWORD,
        timeout=options.timeout,
        workers=options.workers,
    )
    decisions = build_cleanup_decisions(
        groups,
        references,
        host_root=root,
        container_root=settings.GS_DATA_DIR,
    )
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    report = options.report or (
        Path("logs") / f"geoware-duplicates-{timestamp}.json"
    )
    write_report(
        report,
        root=root,
        apply=options.apply,
        decisions=decisions,
    )
    removed = apply_cleanup(decisions, root) if options.apply else 0
    print(f"Групп дублей: {len(decisions)}")
    print(f"Безопасно подтверждено: {sum(d.status == 'ready' for d in decisions)}")
    print(f"Пропущено без удаления: {sum(d.status == 'skipped' for d in decisions)}")
    print(f"Файлов к удалению: {sum(len(d.remove) for d in decisions)}")
    print(f"Фактически удалено: {removed}")
    print(f"Отчёт: {report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
