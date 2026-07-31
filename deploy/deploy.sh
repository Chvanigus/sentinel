#!/usr/bin/env bash
# Развёртывает уже полученный checkout проекта на целевом сервере.

set -Eeuo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PROJECT_ROOT}/.venv/bin/python"
LOCK_FILE="${XDG_RUNTIME_DIR:-/tmp}/sentinel-deploy.lock"

exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
    echo "Другой deploy уже выполняется" >&2
    exit 1
fi

if systemctl is-active --quiet sentinel-nightly.service; then
    echo "Ночной pipeline сейчас выполняется; deploy отменён" >&2
    exit 1
fi

if [[ ! -x "${PYTHON_BIN}" ]]; then
    echo "Не найден Python виртуального окружения: ${PYTHON_BIN}" >&2
    exit 1
fi

if [[ ! -r "${PROJECT_ROOT}/.env" ]]; then
    echo "Не найден читаемый ${PROJECT_ROOT}/.env" >&2
    exit 1
fi

if ! systemctl is-active --quiet xray.service; then
    echo "xray.service не запущен" >&2
    exit 1
fi

if ! systemctl is-enabled --quiet sentinel-nightly.timer; then
    echo "sentinel-nightly.timer не включён" >&2
    exit 1
fi

echo "Обновление Python-зависимостей"
"${PYTHON_BIN}" -m pip install --disable-pip-version-check \
    -r "${PROJECT_ROOT}/requirements.txt"

echo "Проверка импортов и CLI"
"${PYTHON_BIN}" -m compileall -q \
    "${PROJECT_ROOT}/manage.py" \
    "${PROJECT_ROOT}/cdse" \
    "${PROJECT_ROOT}/cli" \
    "${PROJECT_ROOT}/core" \
    "${PROJECT_ROOT}/db" \
    "${PROJECT_ROOT}/domain" \
    "${PROJECT_ROOT}/processing" \
    "${PROJECT_ROOT}/satgeo" \
    "${PROJECT_ROOT}/scripts"
"${PYTHON_BIN}" "${PROJECT_ROOT}/manage.py" help >/dev/null

echo "Deploy завершён: $(git -C "${PROJECT_ROOT}" rev-parse --short HEAD)"
