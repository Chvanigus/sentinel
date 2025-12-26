"""Модуль для работы с файлами изображений."""

from .coord import CoordProcessing
from .processors import (
    execute_ndvi_statistics_image_processor,
    execute_zip_files_processor,
    execute_cloud_mask_image_processor,
    execute_combine_image_processor,
    execute_sentinel_image_processor,
    execute_color_parse_image_processor,
)
