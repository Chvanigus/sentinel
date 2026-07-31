# Ночной запуск через systemd

Таймер ежедневно в 03:00 запускает один `oneshot`-сервис. Сервис сначала
скачивает отсутствующие продукты CDSE за последние три календарных дня, а после
успешного скачивания запускает обработку архива. Отдельный shell-скрипт не
используется.

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

