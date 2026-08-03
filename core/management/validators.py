"""Валидаторы и нормализация аргументов консольных команд."""
import argparse
from datetime import date, datetime, timedelta


def valid_date(value: str) -> date:
    """Проверяет дату формата YYYY-MM-DD и возвращает объект ``date``."""
    msg = f"Недействительная дата: {value}"
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(msg) from exc


def resolve_date_range(
        *,
        year: int | None = None,
        month: int | None = None,
        start: str | None = None,
        end: str | None = None,
) -> tuple[datetime | None, datetime | None]:
    """Возвращает полуоткрытый диапазон дат ``[start, end)``."""
    if month is not None and year is None:
        raise ValueError("--month можно использовать только вместе с --year")
    if (year is not None or month is not None) and (start or end):
        raise ValueError("--year/--month нельзя смешивать с --start/--end")

    if start or end:
        start_date = datetime.strptime(start, "%Y-%m-%d") if start else None
        end_date = (
            datetime.strptime(end, "%Y-%m-%d") + timedelta(days=1)
            if end
            else None
        )
    elif year is not None:
        start_date = datetime(year, month or 1, 1)
        if month == 12:
            end_date = datetime(year + 1, 1, 1)
        elif month is not None:
            end_date = datetime(year, month + 1, 1)
        else:
            end_date = datetime(year + 1, 1, 1)
    else:
        start_date = end_date = None

    if start_date and end_date and start_date >= end_date:
        raise ValueError("Дата начала должна быть не позже даты окончания")
    return start_date, end_date
