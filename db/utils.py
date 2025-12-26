"""Утилиты для работы с базой данных."""

import dataclasses


def get_count_s(table: dataclasses.dataclass, id_flag: bool = True) -> str:
    """
    Возвращает количество атрибутов таблицы описанной как Dataclass.
    :param table: Таблица, описанная как Dataclass.
    :param id_flag: True если надо учитывать поле ID. Если не надо - False.
    :return: Количество атрибутов таблицы описанных в строке '%s,%s...'
    """
    c = len(list(table.__dict__["__dataclass_fields__"]))
    if not id_flag:
        c -= 1
    return ",".join(["%s"] * c)
