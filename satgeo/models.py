"""Модели и разбор имён файлов для публикации."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

_LAYER_NAME_RE = re.compile(
    r"^(?P<satellite>s\d[a-z])_"
    r"(?P<date>\d{2}_\d{2}_\d{4})_"
    r"a(?P<agroid>\d+)"
    r"(?:_(?P<field_id>f\d+))?_"
    r"(?P<img_type>ndvi|ndwi|scl|tci)"
    r"(?:_(?P<resolution>\d+)m)?"
    r"(?:_.*)?$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class FileInfo:
    """Разобранное имя готового растра."""

    date_str: str
    img_type: str
    resolution: int | None
    agroid: str
    field_id: str | None
    layer_name: str
    satellite: str

    def date(self) -> date:
        """Преобразует дату из имени файла в календарное значение."""
        return datetime.strptime(self.date_str, "%d_%m_%Y").date()

    @property
    def agroid_number(self) -> int:
        """Возвращает числовой идентификатор хозяйства."""
        return int(self.agroid.removeprefix("a").removeprefix("A"))


@dataclass(frozen=True)
class PublicationPlan:
    """Неизменяемый план публикации одного растра."""

    source: Path
    destination: Path
    container_path: str
    layer_name: str
    store_name: str
    style_name: str | None
    info: FileInfo


def split_file_name(layer_name: str) -> FileInfo:
    """Строго разбирает имя обработанного слоя."""
    name = Path(layer_name).name.rsplit(".", 1)[0]
    match = _LAYER_NAME_RE.fullmatch(name)
    if match is None:
        raise ValueError(f"Некорректное имя слоя: {layer_name}")

    values = match.groupdict()
    date_str = values["date"]
    try:
        datetime.strptime(date_str, "%d_%m_%Y")
    except ValueError as exc:
        raise ValueError(
            f"Некорректная дата в имени слоя: {layer_name}"
        ) from exc

    return FileInfo(
        date_str=date_str,
        img_type=values["img_type"].lower(),
        resolution=(
            int(values["resolution"]) if values["resolution"] else None
        ),
        agroid=values["agroid"],
        field_id=values["field_id"],
        layer_name=name,
        satellite=values["satellite"].lower(),
    )
