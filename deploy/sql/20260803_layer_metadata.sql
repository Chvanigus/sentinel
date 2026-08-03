-- Добавляет метаданные визуальных слоёв для интерактивной карты.
BEGIN;

ALTER TABLE gpgeo.maps_layer
    ADD COLUMN acquired_at timestamptz,
    ADD COLUMN satellite varchar(3),
    ADD COLUMN source_level varchar(10),
    ADD COLUMN processing_baseline integer,
    ADD COLUMN source_tiles text[],
    ADD COLUMN cloud_coverage_percent double precision,
    ADD COLUMN valid_coverage_percent double precision,
    ADD COLUMN resolution_m smallint,
    ADD COLUMN is_cloud_masked boolean NOT NULL DEFAULT false,
    ADD COLUMN algorithm_version varchar(32),
    ADD COLUMN generated_at timestamptz NOT NULL DEFAULT now(),
    ADD CONSTRAINT ck_maps_layer_cloud_percent
        CHECK (
            cloud_coverage_percent IS NULL
            OR cloud_coverage_percent BETWEEN 0 AND 100
        ),
    ADD CONSTRAINT ck_maps_layer_valid_percent
        CHECK (
            valid_coverage_percent IS NULL
            OR valid_coverage_percent BETWEEN 0 AND 100
        ),
    ADD CONSTRAINT ck_maps_layer_resolution
        CHECK (resolution_m IS NULL OR resolution_m > 0);

COMMENT ON TABLE gpgeo.maps_layer IS
    'Растровые слои спутниковых снимков, доступные фронтенду и опубликованные в GeoServer';
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
    'Применена ли облачная маска к отображаемому растру; визуальные NDVI и NDWI сохраняются без маски';
COMMENT ON COLUMN gpgeo.maps_layer.algorithm_version IS
    'Версия алгоритма, которым сформирован слой и его метаданные';
COMMENT ON COLUMN gpgeo.maps_layer.generated_at IS
    'Время последнего формирования или обновления слоя';

COMMIT;
