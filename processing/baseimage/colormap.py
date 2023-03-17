# -*- coding: utf-8 -*-
"""
@author: IKlitochenko
@co-author: MIlmensky
"""
import sys
from xml.dom import minidom

import numpy as np
from osgeo import gdal, gdal_array

import processing.baseimage.baseimage as bi
from processing.baseimage.baseimage import OpenImage

gdal.UseExceptions()


class ColorMap:
    """Класс анализирует файл qml с расцветками и применяет схему цветов к набору данных GDAL"""
    MULTIPL = 1000
    STEP = 1
    MINVAL = 0
    MAXVAL = 255

    def __init__(self, color_map_path, nodata):

        self.color_map_path = color_map_path
        self.nodata = nodata
        self.cm = {}
        self.min = None
        self.max = None

    def _rgb(self, triplet):
        _NUMERALS = '0123456789abcdefABCDEF'
        _HEXDEC = {v: int(v, 16) for v in (x + y for x in _NUMERALS for y in _NUMERALS)}

        return [_HEXDEC[triplet[0:2]], _HEXDEC[triplet[2:4]], _HEXDEC[triplet[4:6]]]

    def _rescale_formula(self, initial_value, _initial_max, _initial_min, _rescale_max, _rescale_min, _reverse):
        _initial_interval = _initial_max - _initial_min
        _rescale_interval = _rescale_max - _rescale_min

        if _reverse:
            return round(
                    _rescale_max - (
                                float((initial_value - _initial_min) * _rescale_interval) / float(_initial_interval)))
        else:
            return round(
                    (float((
                                       initial_value - _initial_min) * _rescale_interval) / float(_initial_interval)) + _rescale_min)

    def _rescale_band(self, _initial_max, _initial_min, _rescale_end, _rescale_start, initial_value):
        _rescale_nodata = self.MINVAL
        _initial_nodata = self.nodata

        if initial_value == _initial_nodata:
            return _rescale_nodata
        elif _rescale_end == _rescale_start:
            return _rescale_end
        else:
            _rescale_min = min(_rescale_end, _rescale_start)
            _rescale_max = max(_rescale_end, _rescale_start)
            _reverse = False if _rescale_min == _rescale_start else True
            return self._rescale_formula(initial_value, _initial_max, _initial_min, _rescale_max, _rescale_min,
                                         _reverse)

    def _get_rgb(self, value, band):
        if value < self.min:
            return self.cm[self.min][band]
        elif value > self.max:
            return self.cm[self.max][band]
        return self.cm[value][band]

    def parse_color_map(self):

        colormap = []

        dom = minidom.parse(self.color_map_path)
        colorrampshader = dom.getElementsByTagName("colorrampshader")[0]

        for item in colorrampshader.getElementsByTagName("item"):
            colormap.append({'mult_value': round(float(item.getAttribute('value')) * self.MULTIPL),
                             'rgb_color': self._rgb(item.getAttribute('color')[1:])})

        for i in range(0, len(colormap) - 1):
            self.cm[colormap[i]['mult_value']] = colormap[i]['rgb_color']

            dif = abs(colormap[i + 1]['mult_value'] - colormap[i]['mult_value'])
            if dif > self.STEP:
                for j in range(colormap[i]['mult_value'] + self.STEP, colormap[i + 1]['mult_value'], self.STEP):
                    self.cm[j] = [self._rescale_band(colormap[i + 1]['mult_value'], colormap[i]['mult_value'],
                                                     colormap[i + 1]['rgb_color'][0], colormap[i]['rgb_color'][0], j),
                                  self._rescale_band(colormap[i + 1]['mult_value'], colormap[i]['mult_value'],
                                                     colormap[i + 1]['rgb_color'][1], colormap[i]['rgb_color'][1], j),
                                  self._rescale_band(colormap[i + 1]['mult_value'], colormap[i]['mult_value'],
                                                     colormap[i + 1]['rgb_color'][2], colormap[i]['rgb_color'][2], j)]

        self.cm[colormap[i + 1]['mult_value']] = [colormap[i + 1]['rgb_color'][0], colormap[i + 1]['rgb_color'][1],
                                                  colormap[i + 1]['rgb_color'][2]]

        self.min = min(self.cm.keys())
        self.max = max(self.cm.keys())

        self.cm[self.nodata * self.MULTIPL] = [self.MINVAL, self.MINVAL, self.MINVAL]

    def create_rgba(self, src_path, dst_path):

        with OpenImage(src_path) as src_ds:

            try:
                # Prepare output file and dataset.
                dst_ds = bi.create_output_ds(src_ds=src_ds, dst_path=dst_path, nband=4, data_type=gdal.GDT_Byte,
                                             format_file=bi.FORMAT_GEOTIFF)

                for line in range(0, src_ds.RasterYSize):
                    line_array = gdal_array.BandReadAsArray(src_ds.GetRasterBand(1),
                                                            yoff=line,
                                                            win_xsize=src_ds.RasterXSize,
                                                            win_ysize=1)

                    band_array_unt8 = (np.around(line_array * self.MULTIPL)).astype(int)
                    to_rgb = np.vectorize(self._get_rgb, otypes=[np.uint8])
                    red = to_rgb(band_array_unt8, 0)
                    green = to_rgb(band_array_unt8, 1)
                    blue = to_rgb(band_array_unt8, 2)

                    alpha = np.where(line_array == self.nodata, self.MINVAL, self.MAXVAL)

                    # Write bands to the output file.
                    gdal_array.BandWriteArray(dst_ds.GetRasterBand(1), red, yoff=line)
                    gdal_array.BandWriteArray(dst_ds.GetRasterBand(2), green, yoff=line)
                    gdal_array.BandWriteArray(dst_ds.GetRasterBand(3), blue, yoff=line)
                    gdal_array.BandWriteArray(dst_ds.GetRasterBand(4), alpha, yoff=line)

            except RuntimeError as e:
                print('Unable to create rgda NDVI: "{}"'.format(e))
                sys.exit(-1)
            finally:
                src_ds = None
                dst_ds = None
                line_array = None
                band_array_unt8 = None
                red = None
                green = None
                blue = None
                alpha = None

            return dst_path
