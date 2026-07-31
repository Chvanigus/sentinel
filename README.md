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
python manage.py processing --year 2026 --month 7
```

Пути архива и рабочих директорий задаются через `.env`. Во всех компонентах
используется единый `ARCHIVE_ROOT`; регистр имени каталога важен на Linux.

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
