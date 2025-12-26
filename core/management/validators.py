"""Валидаторы для аргументов команд."""
import argparse
from datetime import datetime


def valid_date(s: any) -> datetime.date:
    """
    Валидация параметра даты.
    :param s: Входное значение даты в виде строки формата YYYY-MM-DD
    :return: datetime объект даты.
    """
    msg = f"Недействительная дата: {s}"
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(msg)
