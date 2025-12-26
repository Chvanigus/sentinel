"""Модуль с классами для обработки спутниковых изображений."""

from .cloudmask import execute_cloud_mask_image_processor
from .combine import execute_combine_image_processor
from .sentinel import execute_sentinel_image_processor
from .zipfiles import execute_zip_files_processor
from .ndvistat import execute_ndvi_statistics_image_processor
from .colorparse import execute_color_parse_image_processor

__all__ = [
    "execute_cloud_mask_image_processor", "execute_combine_image_processor",
    "execute_sentinel_image_processor", "execute_zip_files_processor",
    "execute_ndvi_statistics_image_processor",
    "execute_color_parse_image_processor",
]
