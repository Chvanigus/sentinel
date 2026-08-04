# Архитектура Sentinel

Проект разделён по направлению зависимостей: предметная логика не знает о
PostGIS, GDAL, GeoServer, HTTP-клиентах и переменных окружения.

```text
CLI
 └─ composition roots
     ├─ application services
     │   ├─ domain models
     │   ├─ ports
     │   └─ pure path/planning rules
     └─ infrastructure adapters
         ├─ PostGIS repositories
         ├─ GDAL processors
         ├─ CDSE HTTP client
         └─ GeoServer client
```

## Ответственность пакетов

- `domain/` — общие предметные сущности: поля, NDVI-статистика и
  опубликованные слои. Здесь нет persistence-аннотаций.
- `cdse/` — HTTP-интеграция, поиск, загрузка и CDSE application service.
- `processing/` — поиск пар архивов, безопасная распаковка SAFE, use case
  обработки, чистые правила путей и GDAL-процессоры.
- `db/` — конфигурация подключения, низкоуровневый SQL gateway и отдельные
  repositories. Они преобразуют domain entities в records таблиц.
- `satgeo/` — план публикации, оптимизация COG, GeoServer adapter и publisher.
- `cli/` — внешние консольные адаптеры application-сценариев.
- `core/` — механизм запуска команд, logging, настройки и нейтральные
  filesystem-операции; пакет не зависит от прикладных модулей.

## Composition roots

`cdse/composition.py`, `processing/composition.py` и
`satgeo/composition.py` связывают concrete adapters. Настройки также могут
читать внешние CLI-адаптеры, но не application services и не доменные
модули. Application services получают зависимости через конструкторы и
тестируются без сети, GDAL и базы данных.

## Processing lifecycle

1. `ArchivePairFinder` находит полные пары ULA/ULB.
2. `ProcessingService` фильтрует даты через status port.
3. `SentinelArchive` атомарно извлекает только требуемые SAFE-каналы.
4. `ScenePipeline` выполняет именованные шаги одной сцены.
5. После обеих сцен `RasterPublisher` публикует результаты пары.
6. Workspace очищается в `finally`; ошибки нескольких дат агрегируются.

## Наблюдаемость и производительность

Серверный лог содержит маркеры `EXTRACT`, `PIPELINE`, `STEP`, `ARCHIVE`,
`PUBLISH`, `CLEANUP` и `RUN` с длительностью операции. По ним можно отделить
затраты распаковки, GDAL-этапов, публикации и очистки без профилировщика.

- спектральные индексы и облачная фильтрация выполняются окнами, кратными
  физическим блокам GDAL; полноразмерные каналы Sentinel не загружаются в
  память;
- общий B08 читается один раз на окно и используется одновременно для NDVI
  и NDWI;
- SCL выравнивается по точной сетке NDVI одним nearest-neighbour warp, после
  чего NDVI не подвергается повторной интерполяции; категориальный SCL также
  вырезается только nearest-neighbour;
- полевые NDVI-фрагменты анализируются через GDAL `MEM`, без записи и
  повторного чтения отдельных временных TIFF;
- растровые результаты сначала записываются как `.partial` и становятся
  видимыми только после успешного закрытия GDAL dataset;
- tile-level результаты существуют только в рабочем каталоге обработки и
  удаляются после успешной даты; в долговременном `geoware` хранятся только
  вырезанные по хозяйствам растры;
- границы хозяйств, списки полей и геометрии кешируются PostGIS-адаптером
  только на время текущего процесса;
- геометрии нескольких полей читаются в одном connection scope;
- COG-компрессия и GDAL Warp используют доступные CPU;
- publisher обрабатывает только результаты текущей даты, что исключает
  повторную публикацию накопленных файлов в debug-режиме.

## Правила изменений

- domain/application-модули не импортируют `db`, `psycopg2`, `osgeo` или
  `satgeo`;
- настройки окружения не читаются вне composition roots и CLI;
- path resolvers не размещаются в GDAL processor-модулях;
- один use case имеет одну публичную точку входа, без compatibility-фасадов;
- внешние эффекты находятся за ports/repositories и покрываются fake-based
  тестами.
