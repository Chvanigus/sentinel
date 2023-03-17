""" Объединение изображения из двух снимков в одно по Агро 1"""

__author__ = 'Maxim Ilmenskiy'
__date__ = 'November 2021'

from datetime import datetime
from os.path import join

from glob2 import glob
from osgeo import gdal, osr

import settings
import utils as ut


def combine_image(img_list: list, dst_tiff_path: str, set_: bool = False) -> None:
    """ Комбинирование списка tiff изображений в одно большое

    :param img_list:
        Список изображений, который необходимо соединить
    :param dst_tiff_path:
        Выходное изображение
    :param set_:
        Булевая переменная. Если стоит True - размеры пикселей в выходном изображении будут 20м метров
    """
    dst_vrt_path = join(settings.COMBINE_DIR, 'merged.vrt')
    vrt = gdal.BuildVRT(dst_vrt_path, img_list)
    if set_:
        xRes, yRes = 20, 20
    else:
        xRes, yRes = 10, 10
    gdal.Translate(dst_tiff_path, vrt, xRes=xRes, yRes=yRes)


def combining() -> None:
    """ Основная функция управления скриптом combining"""
    ndvi_files = glob(join(settings.COMBINE_DIR, '*ndvi*'))
    rgb_files = glob(join(settings.COMBINE_DIR, '*rgb*'))
    scl_files = glob(join(settings.COMBINE_DIR, '*scl*'))

    print('[combining]: Объединение изображений по Агро 1...')
    if ndvi_files and rgb_files and scl_files:
        if len(ndvi_files) == 2 and len(rgb_files) == 2 and len(scl_files) == 2:
            archives = glob(join(settings.DOWNLOADS_DIR, '*.zip'))
            image_date = datetime.strftime(ut.get_data_from_archive_sentinel(file_path=archives[0])[1], '%d_%m_%Y')
            satellite = ut.get_data_from_archive_sentinel(file_path=archives[0])[0].lower()

            dst_ndvi = join(settings.INPUT_DIR, f'{satellite}_{image_date}_a1_ndvi_10m_3857.tif')
            combine_image(ndvi_files, dst_tiff_path=dst_ndvi)

            dst_rgb = join(settings.INPUT_DIR, f'{satellite}_{image_date}_a1_rgb_10m_3857.tif')
            combine_image(rgb_files, dst_tiff_path=dst_rgb)

            dst_scl = join(settings.INPUT_DIR, f'{satellite}_{image_date}_a1_scl_10m_3857.tif')
            combine_image(scl_files, dst_tiff_path=dst_scl, set_=True)

            dst_srs = osr.SpatialReference()
            dst_srs.ImportFromEPSG(settings.DESTSRID)

            print('[combining]: Объединение проведено успешно')
        else:
            print('[combining]: Пока что нечего объединять')
    else:
        print('[combining]: Снимки для объединения не были обнаружены. Пропуск')


if __name__ == '__main__':
    combining()
