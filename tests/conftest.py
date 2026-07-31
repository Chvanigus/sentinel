"""Общие настройки изолированного запуска unit-тестов."""

import sys
from importlib.util import find_spec
from types import ModuleType

if find_spec("osgeo") is None:
    osgeo_stub = ModuleType("osgeo")
    gdal_stub = ModuleType("gdal")
    osr_stub = ModuleType("osr")
    gdal_stub.GA_ReadOnly = 0
    gdal_stub.UseExceptions = lambda: None
    osgeo_stub.gdal = gdal_stub
    osgeo_stub.osr = osr_stub
    sys.modules["osgeo"] = osgeo_stub
    sys.modules["osgeo.gdal"] = gdal_stub
    sys.modules["osgeo.osr"] = osr_stub
