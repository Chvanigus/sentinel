# Ночной запуск через systemd

Таймер ежедневно в 03:00 запускает один `oneshot`-сервис. Сервис сначала
скачивает отсутствующие продукты CDSE за последние три календарных дня, а после
успешного скачивания запускает обработку архива. Python-оркестратор атомарно
публикует состояние этапов для административного мониторинга Geo.

Сервис рассчитан на расположение проекта `/home/sysop/sentinel`, виртуального
окружения `/home/sysop/sentinel/.venv` и локальный прокси `xray.service`.

## Установка

```bash
cd /home/sysop/sentinel

sudo install -o root -g root -m 644 \
  deploy/systemd/sentinel-nightly.service \
  /etc/systemd/system/sentinel-nightly.service

sudo install -o root -g root -m 644 \
  deploy/systemd/sentinel-nightly.timer \
  /etc/systemd/system/sentinel-nightly.timer

sudo systemctl daemon-reload
sudo systemctl enable --now sentinel-nightly.timer
```

Те же действия можно выполнить из корня проекта:

```bash
make install-systemd
```

## Проверка

Сначала рекомендуется вручную запустить тот же сервис:

```bash
sudo systemctl start sentinel-nightly.service
sudo systemctl status sentinel-nightly.service --no-pager
sudo journalctl -u sentinel-nightly.service -n 200 --no-pager
```

Состояние расписания:

```bash
systemctl list-timers sentinel-nightly.timer --all
```

Для наблюдения за текущим запуском:

```bash
sudo journalctl -fu sentinel-nightly.service
```

После успешного завершения `oneshot`-сервис переходит в состояние `inactive
(dead)` с кодом `0`; для такого типа сервиса это нормально.

## Heartbeat

Сервис сохраняет `/home/sysop/sentinel/runtime/monitoring/sentinel.json`. Файл содержит только
технические поля `status`, `stage`, времена запуска/завершения и код возврата при
ошибке. Секреты, параметры подключения и содержимое снимков туда не попадают.

Статусы:

- `running` — выполняется `download` или `processing`;
- `ok` — оба этапа успешно завершены;
- `error` — указанному этапу не удалось завершиться.

Постоянный каталог проекта сохраняет последний результат после завершения
oneshot-сервиса и перезагрузки хоста, чтобы backend мог читать файл через
read-only bind mount.
