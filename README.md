# Sentinel — обработка снимков Sentinel-2

Проект автоматизирует загрузку и обработку спутниковых снимков Sentinel-2:
поиск в Copernicus Data Space Ecosystem, распаковку, расчёт индексов,
вырезку по контурам полей, расчёт статистики и публикацию в GeoServer.

---

## Описание

Проект принимает ZIP-архивы продуктов `.SAFE`, извлекает TCI и спектральные
каналы, рассчитывает спектральные индексы, вырезает участки по геометриям из
PostGIS, собирает статистику и публикует слои через GeoServer.

Ключевая цель — иметь «почти реальный» поток данных для агрономического мониторинга хозяйств.

---

## Возможности

- Разархивация `.SAFE` и извлечение необходимых каналов
- Построение индексов: NDVI, SCL, NDWI и др.
- Вырезка снимков по контурам полей (из PostgreSQL/PostGIS)
- Анализ статистики по участкам (облака — SCL)
- Конвертация одноканальных слоёв в RGB там, где нужно
- Публикация слоёв в локальном GeoServer

---

## Технологии

- Python 3.13
- GDAL
- PostgreSQL + PostGIS
- GeoServer (локально)
- Copernicus Data Space Ecosystem OData API

## Структура

- `domain/` — предметные сущности без зависимости от инфраструктуры.
- `cdse/` — авторизация, поиск, загрузка и CDSE application service.
- `processing/` — discovery архивов, SAFE extraction и растровый pipeline.
- `db/` — SQL gateway и repositories PostGIS.
- `satgeo/` — планирование, COG-оптимизация и публикация в GeoServer.
- `cli/` — консольные адаптеры сценариев проекта.
- `core/` — механизм команд, настройки, логирование и filesystem primitives.

Подробные границы пакетов и направление зависимостей описаны в
[`docs/architecture.md`](docs/architecture.md).

## Локальная установка

GDAL должен быть установлен в системе вместе с Python bindings той же версии.
Требуется GDAL 3.9+ с bindings, собранными против NumPy 2.
После этого:

```bash
python -m venv .venv
# Windows
.venv\Scripts\python -m pip install --upgrade pip
.venv\Scripts\python -m pip install -r requirements-dev.txt
# Linux
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements-dev.txt
```

Скопируйте `.env.example` в `.env` и заполните параметры подключений. Секреты
не должны попадать в Git.

## Команды

```bash
python manage.py help
python manage.py download --start 2026-07-01 --end 2026-07-31
python manage.py download --start 2026-07-01 --end 2026-07-31 --download
python manage.py download --lookback-days 3 --download
python manage.py download --lookback-days 3 --download --workers 2
python manage.py processing --year 2026 --month 7
```

Если `--start` не указан, команда загрузки ищет продукты за последние три
календарных дня, включая текущую или переданную через `--end` дату. Повторный
поиск безопасен: уже находящиеся в архиве ZIP-файлы не скачиваются заново.
Индекс ночного поиска ограничивается выбранными годами, датами и тайлами,
поэтому не перепроверяет все ZIP-файлы многолетнего архива.

Для ежедневной работы предусмотрен `Makefile`:

```bash
make help
make search DAYS=3
make download START=2026-07-01 END=2026-07-31
make download DAYS=3 WORKERS=2
make process
make process YEAR=2026 MONTH=7
make process DEBUG=1
make recalculate-ndvi YEAR=2026
make recalculate-ndvi START=2026-07-01 END=2026-07-31
make recalculate-ndvi YEAR=2026 AGRO=3,4
make recalculate-ndvi YEAR=2026 FIELD=A3/F100б
make refresh-metadata YEAR=2026
make refresh-metadata
```

Пути архива и рабочих директорий задаются через `.env`. Во всех компонентах
используется единый `ARCHIVE_ROOT`; регистр имени каталога важен на Linux.

`recalculate-ndvi` не обращается к CDSE и не скачивает снимки. Команда заново
читает локальные ZIP-пары из `ARCHIVE_ROOT`, пересчитывает только NDVI и
необходимую для L2A облачную маску, атомарно заменяет статистику
`maps_ndvi_values`, опубликованные NDVI-растры и запускает `reseed` GWC-кэша.
Для защиты от случайного перерасчёта всего архива обязательно передать
`YEAR` либо `START`; режим `DEBUG=1` с этой командой не используется.
Без селектора команда пересчитывает все хозяйства. `AGRO=3,4` ограничивает
перерасчёт перечисленными хозяйствами, а `FIELD=A3/F100б` — одним полем по
`fieldcode`; Unicode-коды, включая кириллицу, поддерживаются. Префикс `A3`
выбирает хозяйство, и принадлежность поля проверяется до изменения статистики.

`refresh-metadata` не открывает растры и не обращается к GeoServer. Команда
читает время, спутник, уровень, processing baseline и тайлы из локальных
ZIP-пар, агрегирует уже рассчитанное качество из `maps_ndvi_values` и одним
пакетным запросом обновляет существующие строки `maps_layer`. Без параметров
обрабатывается весь локальный архив; также доступны `YEAR`, `MONTH`, `START`
и `END`.

Прямой вызов без Makefile эквивалентен:

```bash
python manage.py metadata --year 2026
python manage.py metadata --start 2026-01-01 --end 2026-07-31
python manage.py metadata
```

Обычная `processing` обрабатывает только хозяйства, для которых отсутствует
полный набор опубликованных слоёв. Полные tile-level TIFF используются только
как рабочие файлы и не сохраняются в `GS_DATA_ROOT`: в `geoware/<год>` остаются
только итоговые растры хозяйств `a<agroid>`. После успешной даты рабочие файлы
очищаются; при повторе упавшей даты уже созданные файлы рабочего каталога
используются без повторного расчёта.

Расчёт сохраняет количество пикселей внутри поля, валидное покрытие, облака,
тени, снег, nodata, распределение NDVI и версию алгоритма. Для L2A облаками
считаются классы SCL 8–10; у L1C облачные показатели остаются `NULL`.
Продукты Sentinel с processing baseline 04.00 и новее рассчитываются с учётом
`RADIO_ADD_OFFSET`/`BOA_ADD_OFFSET` из User Product Metadata. Каноническая
миграция таблицы находится в
[`deploy/sql/20260803_ndvi_metadata.sql`](deploy/sql/20260803_ndvi_metadata.sql).
Для уже обновлённой БД точное время съёмки добавляет отдельная миграция
[`deploy/sql/20260804_ndvi_acquired_at.sql`](deploy/sql/20260804_ndvi_acquired_at.sql):
поле `date` сохраняется, а UTC-метка записывается в `acquired_at`.
Метаданные визуальных слоёв добавляет
[`deploy/sql/20260803_layer_metadata.sql`](deploy/sql/20260803_layer_metadata.sql),
а независимые русские комментарии ко всем столбцам обеих таблиц собраны в
[`deploy/sql/20260803_table_comments.sql`](deploy/sql/20260803_table_comments.sql).

## Автоматизация

Готовые unit-файлы для последовательного ночного запуска загрузки и обработки
находятся в [`deploy/systemd`](deploy/systemd/README.md). Таймер срабатывает
ежедневно в 03:00 и требует запущенный `xray.service`.

Автоматическое развёртывание проверенного `main` через GitHub Actions описано в
[`deploy/README.md`](deploy/README.md). До добавления SSH-secrets и переменной
`AUTO_DEPLOY_ENABLED=true` deploy-job не выполняется.

## Проверки

```bash
python -m pytest
python -m pytest --cov --cov-branch --cov-report=term-missing
python -m ruff check .
python -m compileall -q manage.py cdse cli core db domain processing satgeo scripts
```

Те же проверки автоматически выполняются в GitHub Actions для Python 3.13.

В окружении с установленным GDAL ключевую цепочку облачной маски можно
проверить без БД и внешних сервисов:

```bash
python -m scripts.gdal_smoke
```
