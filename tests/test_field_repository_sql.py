"""Тесты SQL-контракта репозитория полей без legacy-функций БД."""

from db.repositories import FieldRepository


class RecordingGateway:
    """Фиксирует запросы и возвращает заранее заданные строки."""

    def __init__(self, rows):
        """Сохраняет результат, который вернётся из метода rows."""
        self.rows_result = rows
        self.calls = []

    def rows(self, query, params):
        """Запоминает SQL и параметры запроса."""
        self.calls.append((query, params))
        return self.rows_result


def test_field_list_uses_current_tables_without_legacy_function():
    """Список полей строится прямым соединением актуальных таблиц."""
    gateway = RecordingGateway([{"id": 7, "name": "Поле 7"}])

    result = FieldRepository(gateway).list_for_agro(3, 2026)

    query, params = gateway.calls[0]
    assert result[0].id == 7
    assert "gpgeo.maps_field AS field" in query
    assert "gpgeo.maps_field_shape AS shape" in query
    assert "__geo_get_fieldnames_for_agro_year" not in query
    assert params == (3, 2026)


def test_field_bounds_use_postgis_extent_without_legacy_function():
    """Границы вычисляются агрегатом PostGIS без хранимой функции."""
    gateway = RecordingGateway([(1.0, 2.0, 3.0, 4.0)])

    result = FieldRepository(gateway).bounds(
        srid=3857,
        year=2026,
        agroid=3,
    )

    query, params = gateway.calls[0]
    assert result == (1.0, 2.0, 3.0, 4.0)
    assert "ST_Extent" in query
    assert "__geostl_get_boundpoints" not in query
    assert params == (3857, 2026, None, None, 3, 3)


def test_field_geometries_use_postgis_geojson_without_legacy_function():
    """Геометрии пакетом преобразуются в GeoJSON прямым запросом."""
    geometry = {"type": "Feature", "geometry": {"type": "Polygon"}}
    gateway = RecordingGateway([(10, geometry), (20, geometry)])

    result = FieldRepository(gateway).geometries([10, 20], 2026)

    query, params = gateway.calls[0]
    assert result == {10: geometry, 20: geometry}
    assert "ST_AsGeoJSON" in query
    assert "__geo_get_field_shape" not in query
    assert params == ([10, 20], 2026)
