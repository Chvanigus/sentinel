"""Оптимизация GeoTIFF для публикации."""
from __future__ import annotations

import subprocess
import time
from pathlib import Path
from time import perf_counter

from core.logging import get_logger

logger = get_logger(__name__)


def optimize_geotiff(
        src: Path,
        dst: Path,
        retries: int = 5,
        delay: float = 5.0,
) -> None:
    """Создаёт COG во временном файле и атомарно заменяет результат."""
    if retries < 1:
        raise ValueError("retries должен быть положительным")

    dst.parent.mkdir(parents=True, exist_ok=True)
    temporary = dst.with_suffix(dst.suffix + ".tmp")

    for attempt in range(1, retries + 1):
        attempt_started = perf_counter()
        try:
            subprocess.check_call(
                [
                    "gdal_translate",
                    "-of",
                    "COG",
                    "-co",
                    "COMPRESS=DEFLATE",
                    "-co",
                    "PREDICTOR=2",
                    "-co",
                    "BLOCKSIZE=256",
                    "-co",
                    "OVERVIEWS=IGNORE_EXISTING",
                    "-co",
                    "NUM_THREADS=ALL_CPUS",
                    str(src),
                    str(temporary),
                ]
            )
            temporary.replace(dst)
            logger.info(
                "COG TIFF записан: %s | %.1f → %.1f МиБ | %.2f сек.",
                dst,
                src.stat().st_size / 1024 / 1024,
                dst.stat().st_size / 1024 / 1024,
                perf_counter() - attempt_started,
            )
            return
        except subprocess.CalledProcessError:
            logger.exception(
                "gdal_translate: неудачная попытка %d/%d",
                attempt,
                retries,
            )
            if attempt == retries:
                raise
            time.sleep(delay)
        except OSError as exc:
            if exc.errno != 16 or attempt == retries:
                raise
            logger.warning(
                "Файл %s занят, повтор через %s сек.",
                temporary,
                delay,
            )
            time.sleep(delay)
        finally:
            if temporary.exists():
                temporary.unlink(missing_ok=True)
