-- Добавляет точное время съёмки, сохраняя прежнее календарное поле date.
BEGIN;

ALTER TABLE gpgeo.maps_ndvi_values
    ADD COLUMN IF NOT EXISTS acquired_at timestamptz;

-- Для ранее рассчитанных значений используем время уже опубликованного слоя.
-- MIN детерминированно разрешает редкий случай нескольких слоёв за одну дату.
WITH acquisition AS (
    SELECT
        date,
        MIN(acquired_at) AS acquired_at
    FROM gpgeo.maps_layer
    WHERE acquired_at IS NOT NULL
    GROUP BY date
)
UPDATE gpgeo.maps_ndvi_values AS ndvi
SET acquired_at = acquisition.acquired_at
FROM acquisition
WHERE ndvi.date = acquisition.date
  AND ndvi.acquired_at IS NULL;

COMMENT ON COLUMN gpgeo.maps_ndvi_values.acquired_at
    IS 'Точное время начала спутниковой съёмки в UTC';

CREATE INDEX IF NOT EXISTS maps_ndvi_values_acquired_at_index
    ON gpgeo.maps_ndvi_values (acquired_at);

COMMIT;
