"""Дата классы для данных из БД."""
import dataclasses
import datetime
from dataclasses import dataclass, field


@dataclass
class MainMixin:
    """Основной миксин."""
    id: int = field(default='')

    def to_tuple(self) -> tuple:
        """Возвращает кортеж."""
        return dataclasses.astuple(self)

    def to_dict(self) -> dict:
        """Возвращает словарь."""
        return dataclasses.asdict(self)

    @staticmethod
    def TableName():
        """Возвращает название таблицы со схемой в базе данных."""
        raise NotImplementedError(
            "Данный метод должен быть реализован в дочернем классе."
        )

    def __str__(self):
        """Возвращает строковый формат представления объекта."""
        raise NotImplementedError(
            "Данный метод должен быть реализован в дочернем классе."
        )


@dataclass
class FieldidMixin:
    """Миксин поля fieldid."""
    fieldid: int = field(default="")


@dataclass
class DateMixin:
    """Миксин поля date."""
    date: datetime = field(default="")


@dataclass
class Layer(MainMixin, FieldidMixin, DateMixin):
    """Модель таблицы gpgeo.Layer."""
    set: str = field(default="")
    resolution: int = field(default="")
    agroid: int = field(default="")
    name: str = field(default="")
    satellite: str = field(default="")
    isgrouplayer: bool = field(default=False)

    @staticmethod
    def TableName() -> str:
        return "maps_layer"

    def __str__(self):
        return f"Layer: {self.name}"


@dataclass
class NdviValues(MainMixin, FieldidMixin, DateMixin):
    """Модель таблицы gpgeo.NdviValues."""
    ndvimean: float = field(default=0.0)
    ndvimax: float = field(default=0.0)
    ndvimin: float = field(default=0.0)
    growth_percent: float = field(default=0.0)
    ndvi_cv: float = field(default=0.0)
    is_uniform: bool = field(default=True)

    @staticmethod
    def TableName() -> str:
        return "maps_ndvi_values"

    def __str__(self):
        return (
            f"NdviValues | Mean: {self.ndvimean} | Max: {self.ndvimax} | "
            f"Min: {self.ndvimin} | Growth: {self.growth_percent}"
        )


@dataclass
class Field(MainMixin):
    """Модель таблицы gpgeo.Field."""
    name: str = field(default="")

    @staticmethod
    def TableName() -> str:
        return "maps_field"

    def __str__(self):
        return f"Поле: {self.id}"
