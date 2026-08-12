-- Расширяет статистику NDVI показателями качества и воспроизводимости.
BEGIN;

ALTER TABLE gpgeo.maps_ndvi_values
    ADD COLUMN acquired_at timestamptz,
    ADD COLUMN valid_pixel_count bigint,
    ADD COLUMN total_pixel_count bigint,
    ADD COLUMN cloud_pixel_count bigint,
    ADD COLUMN nodata_pixel_count bigint,
    ADD COLUMN shadow_pixel_count bigint,
    ADD COLUMN snow_pixel_count bigint,
    ADD COLUMN valid_coverage_percent double precision,
    ADD COLUMN cloud_coverage_percent double precision,
    ADD COLUMN ndvi_stddev double precision,
    ADD COLUMN ndvi_median double precision,
    ADD COLUMN ndvi_p10 double precision,
    ADD COLUMN ndvi_p90 double precision,
    ADD COLUMN source_level varchar(8),
    ADD COLUMN algorithm_version varchar(32),
    ADD COLUMN calculated_at timestamptz;

ALTER TABLE gpgeo.maps_ndvi_values
    ADD CONSTRAINT maps_ndvi_values_valid_pixel_count_check
        CHECK (valid_pixel_count IS NULL OR valid_pixel_count >= 0),
    ADD CONSTRAINT maps_ndvi_values_total_pixel_count_check
        CHECK (total_pixel_count IS NULL OR total_pixel_count >= 0),
    ADD CONSTRAINT maps_ndvi_values_cloud_pixel_count_check
        CHECK (cloud_pixel_count IS NULL OR cloud_pixel_count >= 0),
    ADD CONSTRAINT maps_ndvi_values_nodata_pixel_count_check
        CHECK (nodata_pixel_count IS NULL OR nodata_pixel_count >= 0),
    ADD CONSTRAINT maps_ndvi_values_shadow_pixel_count_check
        CHECK (shadow_pixel_count IS NULL OR shadow_pixel_count >= 0),
    ADD CONSTRAINT maps_ndvi_values_snow_pixel_count_check
        CHECK (snow_pixel_count IS NULL OR snow_pixel_count >= 0),
    ADD CONSTRAINT maps_ndvi_values_pixel_counts_check
        CHECK (
            total_pixel_count IS NULL
            OR (
                COALESCE(valid_pixel_count, 0)
                + COALESCE(cloud_pixel_count, 0)
                + COALESCE(nodata_pixel_count, 0)
                + COALESCE(shadow_pixel_count, 0)
                + COALESCE(snow_pixel_count, 0)
                <= total_pixel_count
            )
        ),
    ADD CONSTRAINT maps_ndvi_values_valid_coverage_percent_check
        CHECK (
            valid_coverage_percent IS NULL
            OR valid_coverage_percent BETWEEN 0 AND 100
        ),
    ADD CONSTRAINT maps_ndvi_values_cloud_coverage_percent_check
        CHECK (
            cloud_coverage_percent IS NULL
            OR cloud_coverage_percent BETWEEN 0 AND 100
        ),
    ADD CONSTRAINT maps_ndvi_values_source_level_check
        CHECK (
            source_level IS NULL
            OR source_level IN ('MSIL1C', 'MSIL2A')
        );

COMMENT ON TABLE gpgeo.maps_ndvi_values
    IS 'Статистика NDVI и показатели качества по одному полю за дату съёмки';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.id
    IS 'Уникальный идентификатор записи статистики';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.fieldid
    IS 'Поле, для которого рассчитаны NDVI и показатели качества';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.date
    IS 'Календарная дата спутниковой съёмки';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.acquired_at
    IS 'Точное время начала спутниковой съёмки в UTC';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.ndvimean
    IS 'Среднее NDVI по валидным пикселям поля';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.ndvimax
    IS 'Максимальное NDVI среди валидных пикселей поля';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.ndvimin
    IS 'Минимальное NDVI среди валидных пикселей поля';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.growth_percent
    IS 'Процент изменения NDVI относительно выбранного предыдущего периода; NULL, если сравнение не выполнялось';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.ndvi_cv
    IS 'Коэффициент вариации NDVI в процентах: стандартное отклонение относительно среднего';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.is_uniform
    IS 'Признак статистической и пространственной однородности валидной части поля';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.valid_pixel_count
    IS 'Количество пикселей поля, использованных при расчёте NDVI';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.total_pixel_count
    IS 'Общее количество пикселей внутри геометрии поля';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.cloud_pixel_count
    IS 'Количество облачных пикселей по SCL: классы 8, 9 и 10';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.nodata_pixel_count
    IS 'Количество пикселей поля без данных или с некорректным NDVI';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.shadow_pixel_count
    IS 'Количество пикселей cast/cloud shadow по SCL: классы 2 и 3';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.snow_pixel_count
    IS 'Количество пикселей снега или льда по SCL: класс 11';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.valid_coverage_percent
    IS 'Доля валидных пикселей поля, %';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.cloud_coverage_percent
    IS 'Доля облачных пикселей поля по SCL, %; NULL для L1C';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.ndvi_stddev
    IS 'Стандартное отклонение NDVI';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.ndvi_median
    IS 'Медиана NDVI';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.ndvi_p10
    IS '10-й перцентиль NDVI';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.ndvi_p90
    IS '90-й перцентиль NDVI';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.source_level
    IS 'Уровень исходного продукта Sentinel: MSIL1C или MSIL2A';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.algorithm_version
    IS 'Версия алгоритма расчёта статистики';
COMMENT ON COLUMN gpgeo.maps_ndvi_values.calculated_at
    IS 'Время фактического выполнения расчёта';

COMMIT;
