-- Документирует обе таблицы после применения миграций метаданных.
BEGIN;

COMMENT ON TABLE gpgeo.maps_ndvi_values IS
    'Статистика NDVI и показатели качества по одному полю за дату съёмки';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.id IS
    'Уникальный идентификатор записи статистики';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.fieldid IS
    'Поле, для которого рассчитаны NDVI и показатели качества';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.date IS
    'Календарная дата спутниковой съёмки';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.acquired_at IS
    'Точное время начала спутниковой съёмки в UTC';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.ndvimean IS
    'Среднее NDVI по валидным пикселям поля';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.ndvimax IS
    'Максимальное NDVI среди валидных пикселей поля';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.ndvimin IS
    'Минимальное NDVI среди валидных пикселей поля';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.growth_percent IS
    'Процент изменения NDVI относительно выбранного предыдущего периода; NULL, если сравнение не выполнялось';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.ndvi_cv IS
    'Коэффициент вариации NDVI в процентах';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.is_uniform IS
    'Признак статистической и пространственной однородности валидной части поля';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.valid_pixel_count IS
    'Количество пикселей поля, использованных при расчёте NDVI';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.total_pixel_count IS
    'Общее количество пикселей внутри геометрии поля';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.cloud_pixel_count IS
    'Количество облачных пикселей по SCL: классы 8, 9 и 10';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.nodata_pixel_count IS
    'Количество пикселей поля без данных или с некорректным NDVI';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.shadow_pixel_count IS
    'Количество пикселей тени по SCL: классы 2 и 3';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.snow_pixel_count IS
    'Количество пикселей снега или льда по SCL: класс 11';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.valid_coverage_percent IS
    'Доля валидных пикселей поля, проценты';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.cloud_coverage_percent IS
    'Доля облачных пикселей поля по SCL, проценты; NULL для L1C';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.ndvi_stddev IS
    'Стандартное отклонение NDVI';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.ndvi_median IS
    'Медиана NDVI';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.ndvi_p10 IS
    '10-й перцентиль NDVI';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.ndvi_p90 IS
    '90-й перцентиль NDVI';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.source_level IS
    'Уровень исходного продукта Sentinel: MSIL1C или MSIL2A';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.algorithm_version IS
    'Версия алгоритма расчёта статистики';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.calculated_at IS
    'Время фактического выполнения расчёта';

COMMENT ON TABLE gpgeo.maps_layer IS
    'Растровые слои спутниковых снимков для фронтенда, опубликованные в GeoServer';
COMMENT ON COLUMN gpgeo.maps_layer.id IS
    'Уникальный идентификатор записи слоя';
COMMENT ON COLUMN gpgeo.maps_layer.date IS
    'Календарная дата спутниковой съёмки';
COMMENT ON COLUMN gpgeo.maps_layer.set IS
    'Тип визуального слоя: tci, ndvi, ndwi или scl';
COMMENT ON COLUMN gpgeo.maps_layer.agroid IS
    'Хозяйство, для территории которого подготовлен слой';
COMMENT ON COLUMN gpgeo.maps_layer.fieldid IS
    'Поле, если слой опубликован отдельно; NULL для слоя хозяйства';
COMMENT ON COLUMN gpgeo.maps_layer.name IS
    'Уникальное полное имя слоя в GeoServer';
COMMENT ON COLUMN gpgeo.maps_layer.acquired_at IS
    'Точное время начала спутниковой съёмки в UTC';
COMMENT ON COLUMN gpgeo.maps_layer.satellite IS
    'Спутник-источник: S2A, S2B, S2C или следующий аппарат Sentinel-2';
COMMENT ON COLUMN gpgeo.maps_layer.source_level IS
    'Уровень обработки исходного продукта: L1C или L2A';
COMMENT ON COLUMN gpgeo.maps_layer.processing_baseline IS
    'Числовая версия Processing Baseline исходного продукта Sentinel-2';
COMMENT ON COLUMN gpgeo.maps_layer.source_tiles IS
    'Коды исходных тайлов Sentinel-2, из которых собран слой';
COMMENT ON COLUMN gpgeo.maps_layer.cloud_coverage_percent IS
    'Доля облачных пикселей на объединённой территории полей хозяйства, проценты';
COMMENT ON COLUMN gpgeo.maps_layer.valid_coverage_percent IS
    'Доля валидных пикселей на объединённой территории полей хозяйства, проценты';
COMMENT ON COLUMN gpgeo.maps_layer.resolution_m IS
    'Пространственное разрешение растра в метрах на пиксель';
COMMENT ON COLUMN gpgeo.maps_layer.is_cloud_masked IS
    'Применена ли облачная маска к отображаемому растру';
COMMENT ON COLUMN gpgeo.maps_layer.algorithm_version IS
    'Версия алгоритма, которым сформирован слой и его метаданные';
COMMENT ON COLUMN gpgeo.maps_layer.generated_at IS
    'Время последнего формирования или обновления слоя';

COMMIT;
