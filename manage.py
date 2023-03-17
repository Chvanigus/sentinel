""" Основной управляющий файл проектом sentinel"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from os import remove
from os.path import basename, join

from glob2 import glob

import dboperator as db
import settings
import utils as ut
from obtain_data import obtain_data
from processing import cloudmask, combining, ndvivalue, sen2cor, sentinel
from public import public


logger = logging.getLogger('__name__')


def set_args() -> argparse.ArgumentParser:
    """ Парсинг аргумента запуска скрипта"""

    def valid_date(s: any) -> datetime.date:
        """ Проверка введённой даты

        :param s:
            Входное значение даты
        :return:
            Дата
        """
        msg = f'Недействительная дата: {s}'
        try:
            return datetime.strptime(s, '%Y-%m-%d').date()
        except ValueError:
            raise argparse.ArgumentTypeError(msg)

    parser = argparse.ArgumentParser(prog='main')
    parser.add_argument('-d', '--date', help='Дата необходимого снимка', type=valid_date, metavar='YYYY-MM-DD')
    parser.add_argument('-dw', '--download', help='Поиск и скачивание снимков. По умолчанию - просто поиск снимков.',
                        action='store_true')

    return parser


def main() -> None:
    """Основная функция управления всем проектом SENTINEL"""
    parser = set_args()
    args = parser.parse_args()
    args.date = datetime.now().date() if not args.date else args.date

    if db.check_layer_date(new_date=args.date):
        print(f'Снимков с данной датой ({args.date}) нет в нашей базе данных.\n'
              f'Запущена процедура скачивания и обработки снимков...')

        if args.download:
            flag = True
        else:
            flag = False
        # Запуск поиска и скачивания снимков
        obtain_data(start_date=args.date, end_date=args.date + timedelta(days=1), download=flag)

        files = glob(join(settings.DOWNLOADS_DIR, '*.zip'))
        if files:
            for file in files:
                print(f'Начата обработка файла: {basename(file)}')

                if ut.get_data_from_archive_sentinel(file_path=file)[2] == 'T38ULA':
                    field_groups = [1, 3, 4]
                else:
                    field_groups = [1, 5, 6]

                # Обработчик Sen2Cor для корректировки изображения с учётом атмосферных помех
                sen2cor()

                # Нарезка изображений по каждому Агро
                sentinel(field_groups=field_groups)

                # Объединение изображений по Агро 1
                combine_1 = glob(join(settings.COMBINE_DIR, 'combine1*.tif'))
                combine_2 = glob(join(settings.COMBINE_DIR, 'combine2*.tif'))
                if combine_1 and combine_2:
                    combining()

                # Получение маски облачности для всех полей
                cloudmask()

                # Получение значений NDVI для каждого поля
                ndvivalue()

                print(f'Основные операции по обработке {basename(file)} завершены')
                if not settings.DEBUG:
                    remove(file)

                if not settings.DEBUG:
                    print('Удаление отработанных файлов...')
                    ut.remove_processed_files(settings.NDVI_DIR,
                                              settings.TEMP_PROCESSING_DIR,
                                              settings.INPUT_DIR)
            # Публикация снимков
            if not settings.DEBUG:
                public()
        else:
            print('Снимков нет. Скрипт отключен')
            sys.exit(0)
    else:
        print(f'Снимки с данной датой ({args.date}) уже обработаны и загружены на Geo. Скрипт отключен')
        sys.exit(0)

    if not settings.DEBUG:
        # Удаление отработанных файлов во всем проекте
        print('Глобальное удаление всех отработанных файлов в проекте')
        ut.remove_processed_files(settings.NDVI_DIR,
                                  settings.PROCESSED_DIR,
                                  settings.DOWNLOADS_DIR,
                                  settings.TEMP_PROCESSING_DIR,
                                  settings.INPUT_DIR,
                                  settings.COMBINE_DIR)


if __name__ == '__main__':
    main()
