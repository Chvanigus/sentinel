"""Утилиты модуля satgeo."""
import re
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from core.logging import get_logger
from core.utils import get_basename

logger = get_logger()


@dataclass
class FileInfo:
    """Структура с разобранными полями имени слоя."""
    date_str: str  # '21_10_2024'
    img_type: str  # 'ndvi' / 'ndwi' / 'rgb' / 'scl' и т.п.
    resolution: Optional[int]  # 10 / 20 / None
    agroid: Optional[str]  # '1' / 'A1' / None (как строка)
    field_id: Optional[str]  # дополнительное поле, если нужно
    layer_name: str  # имя слоя без расширения
    satellite: Optional[str]  # 's2a' / 's2b' и т.д.

    def date(self):
        """Возвращает datetime.date из date_str."""
        import datetime
        return datetime.datetime.strptime(self.date_str, "%d_%m_%Y").date()


def split_file_name(layer_name: str) -> FileInfo:
    """Разбирает имя файла спутникового слоя на составляющие."""
    base = get_basename(layer_name)
    name = base.rsplit(".", 1)[0]
    parts = name.split("_")

    if len(parts) < 6:
        raise ValueError(f"Слишком короткое имя файла: {layer_name}")

    # 1. Спутник
    satellite = parts[0]

    # 2. Дата (строго dd_mm_yyyy)
    date_match = re.search(r"\d{2}_\d{2}_\d{4}", name)
    if not date_match:
        raise ValueError(f"Не удалось найти дату в имени файла: {layer_name}")
    date_str = date_match.group()

    date_parts = date_str.split("_")
    date_index = parts.index(date_parts[0])  # индекс '21'

    # 3. Агро сразу после даты
    agroid = None
    agroid_part = parts[date_index + 3]  # после dd mm yyyy
    m = re.fullmatch(r"a(\d+)", agroid_part, re.IGNORECASE)
    if m:
        agroid = m.group(1)

    # 4. После агро может быть field_id (на будущее)
    cursor = date_index + 4

    field_id = None
    if re.fullmatch(r"f\d+", parts[cursor], re.IGNORECASE):
        field_id = parts[cursor]
        cursor += 1

    # 5. Тип изображения
    known_types = {"ndvi", "ndwi", "scl", "tci"}
    img_type = parts[cursor].lower()
    if img_type not in known_types:
        raise ValueError(f"Неизвестный тип изображения: {img_type}")
    cursor += 1

    # 6. Разрешение
    resolution = None
    m = re.fullmatch(r"(\d+)m", parts[cursor], re.IGNORECASE)
    if m:
        resolution = int(m.group(1))
        cursor += 1

    return FileInfo(
        date_str=date_str,
        img_type=img_type,
        resolution=resolution,
        agroid=agroid,
        field_id=field_id,
        layer_name=name,
        satellite=satellite,
    )


def optimize_geotiff(src: Path, dst: Path, retries: int = 5,
                     delay: float = 5.0) -> None:
    """
    Оптимизация GeoTIFF под COG с retry:
    - gdal_translate с COMPRESS=DEFLATE, PREDICTOR=2, BLOCKSIZE=256
    - Временный файл создаётся в той же директории для атомарной замены
    - При Errno 16 (Device busy) повторяем до retries раз
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".tmp")

    attempt = 0
    while attempt < retries:
        try:
            cmd_cog = [
                "gdal_translate",
                "-of", "COG",
                "-co", "COMPRESS=DEFLATE",
                "-co", "PREDICTOR=2",
                "-co", "BLOCKSIZE=256",
                "-co", "OVERVIEWS=IGNORE_EXISTING",
                str(src),
                str(tmp),
            ]
            logger.info(
                "[attempt %s] Запуск gdal_translate для COG: %s"
                , attempt + 1, ' '.join(cmd_cog)
            )
            subprocess.check_call(cmd_cog)

            # атомарная замена
            tmp.replace(dst)
            logger.info("COG TIFF записан: %s", dst)
            return

        except subprocess.CalledProcessError as exc:
            logger.error("gdal_translate завершился с ошибкой: %s", exc)
            attempt += 1
            if attempt < retries:
                logger.warning("Повтор через %s сек...", delay)
                time.sleep(delay)
            else:
                raise

        except OSError as exc:
            if exc.errno == 16:
                attempt += 1
                if attempt < retries:
                    logger.warning(
                        "Файл %s занят, повтор через %s сек...",
                        tmp, delay
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        "Файл %s так и не освободился после %s попыток",
                        tmp, retries
                    )
                    raise
            else:
                raise

        finally:
            if tmp.exists():
                tmp.unlink(missing_ok=True)
