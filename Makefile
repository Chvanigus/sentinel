.DEFAULT_GOAL := help

PYTHON ?= .venv/bin/python
MANAGE := $(PYTHON) manage.py
DAYS ?= 3

DOWNLOAD_RANGE := $(if $(strip $(START)),--start $(START),--lookback-days $(DAYS))
DOWNLOAD_RANGE += $(if $(strip $(END)),--end $(END))
DOWNLOAD_WORKERS := $(if $(strip $(WORKERS)),--workers $(WORKERS))

PROCESS_RANGE := $(if $(strip $(START)),--start $(START))
PROCESS_RANGE += $(if $(strip $(END)),--end $(END))
PROCESS_RANGE += $(if $(strip $(YEAR)),--year $(YEAR))
PROCESS_RANGE += $(if $(strip $(MONTH)),--month $(MONTH))
PROCESS_DEBUG := $(if $(filter 1 true yes,$(strip $(DEBUG))),--debug)
NDVI_TARGET := $(if $(strip $(AGRO)),--agro "$(AGRO)")
NDVI_TARGET += $(if $(strip $(FIELD)),--field "$(FIELD)")

.PHONY: help check-env search download process process-debug recalculate-ndvi
.PHONY: refresh-metadata
.PHONY: test lint smoke deploy
.PHONY: install-systemd timer logs

help:
	@echo "Sentinel — доступные команды"
	@echo
	@echo "  make search                         Поиск за последние 3 дня"
	@echo "  make search DAYS=7                  Поиск за последние 7 дней"
	@echo "  make search START=2026-07-01 END=2026-07-31"
	@echo "  make download                       Поиск и скачивание за 3 дня"
	@echo "  make download START=... END=..."
	@echo "  make download WORKERS=2             Число параллельных загрузок"
	@echo "  make process                        Обработка всех незавершённых пар"
	@echo "  make process START=... END=..."
	@echo "  make process YEAR=2026 MONTH=7"
	@echo "  make process DEBUG=1                Без очистки рабочих файлов"
	@echo "  make recalculate-ndvi YEAR=2026     Полная замена NDVI за год"
	@echo "  make recalculate-ndvi START=... END=..."
	@echo "  make recalculate-ndvi YEAR=2026 AGRO=3,4"
	@echo "  make recalculate-ndvi YEAR=2026 FIELD=A3/F100б"
	@echo "  make refresh-metadata YEAR=2026     Только метаданные снимков"
	@echo "  make refresh-metadata               Метаданные всего архива"
	@echo "  make test | lint | smoke            Локальные проверки"
	@echo "  make deploy                         Ручной deploy текущего checkout"
	@echo "  make install-systemd                Установка ночного таймера"
	@echo "  make timer | logs                   Статус расписания и live-логи"

check-env:
	@test -x "$(PYTHON)" || { echo "Не найден Python: $(PYTHON)"; exit 1; }
	@test -r ".env" || { echo "Не найден читаемый .env"; exit 1; }

search: check-env
	$(MANAGE) download $(DOWNLOAD_RANGE)

download: check-env
	$(MANAGE) download $(DOWNLOAD_RANGE) --download $(DOWNLOAD_WORKERS)

process: check-env
	$(MANAGE) processing $(PROCESS_RANGE) $(PROCESS_DEBUG)

process-debug: check-env
	$(MANAGE) processing $(PROCESS_RANGE) --debug

recalculate-ndvi: check-env
	$(MANAGE) processing $(PROCESS_RANGE) --recalculate-ndvi $(NDVI_TARGET)

refresh-metadata: check-env
	$(MANAGE) metadata $(PROCESS_RANGE)

test:
	$(PYTHON) -m pytest -q

lint:
	$(PYTHON) -m ruff check .

smoke: check-env
	$(PYTHON) -m scripts.gdal_smoke

deploy:
	./deploy/deploy.sh

install-systemd:
	sudo install -o root -g root -m 644 deploy/systemd/sentinel-nightly.service /etc/systemd/system/sentinel-nightly.service
	sudo install -o root -g root -m 644 deploy/systemd/sentinel-nightly.timer /etc/systemd/system/sentinel-nightly.timer
	sudo systemd-analyze verify /etc/systemd/system/sentinel-nightly.service /etc/systemd/system/sentinel-nightly.timer
	sudo systemctl daemon-reload
	sudo systemctl enable --now sentinel-nightly.timer

timer:
	@systemctl list-timers sentinel-nightly.timer --all

logs:
	sudo journalctl -fu sentinel-nightly.service
