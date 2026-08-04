# Автоматический deploy

После одноразовой настройки GitHub Actions автоматически развёртывает каждый
успешно проверенный push в `main`. Deploy выполняется по SSH от пользователя
`sysop` и не требует passwordless `sudo`.

## Что делает workflow

1. Выполняет линтер, тесты и компиляцию в GitHub Actions.
2. Подключается к серверу по SSH с проверкой host key.
3. Отказывается менять checkout, если ночной pipeline сейчас работает.
4. Проверяет чистоту tracked-файлов и выполняет только fast-forward.
5. Развёртывает конкретный проверенный commit, а не произвольный HEAD ветки.
6. Обновляет Python-зависимости и проверяет импорты и CLI.

`.env`, архивы и конфигурация Xray находятся вне Git и не изменяются.
Systemd-unit устанавливается один раз командой `make install-systemd`; обычный
deploy не получает root-доступ и не перезапускает выполняющуюся обработку.
Пошаговая замена VLESS Reality/gRPC-прокси с проверкой и откатом описана в
[`XRAY_PROXY_REPLACEMENT.txt`](XRAY_PROXY_REPLACEMENT.txt).

## Одноразовая настройка SSH

На сервере под `sysop` создайте отдельный ключ для GitHub Actions:

```bash
ssh-keygen -t ed25519 \
  -C github-actions-sentinel \
  -f ~/.ssh/github-actions-sentinel \
  -N ''

cat ~/.ssh/github-actions-sentinel.pub >> ~/.ssh/authorized_keys
chmod 700 ~/.ssh
chmod 600 ~/.ssh/authorized_keys
```

Закрытый ключ из `~/.ssh/github-actions-sentinel` добавьте в GitHub Actions
secret `DEPLOY_SSH_KEY`. После проверки deploy локальную копию закрытого ключа
на сервере следует удалить; публичный `.pub` можно оставить.

## Настройки GitHub

В `Settings → Secrets and variables → Actions` добавьте secrets:

- `DEPLOY_HOST` — публичный адрес сервера;
- `DEPLOY_PORT` — SSH-порт, обычно `22`;
- `DEPLOY_USER` — `sysop`;
- `DEPLOY_SSH_KEY` — закрытый ключ целиком;
- `DEPLOY_KNOWN_HOSTS` — доверенная строка `known_hosts` для сервера.

`DEPLOY_KNOWN_HOSTS` следует получить с доверенной машины и сверить с host key
сервера:

```bash
ssh-keyscan -H -p 22 SERVER_HOST
```

После добавления secrets создайте repository variable:

```text
AUTO_DEPLOY_ENABLED=true
```

Пока переменная отсутствует или не равна `true`, deploy-job безопасно
пропускается.

## Первый запуск

Автоматический deploy можно запустить новым push в `main` или вручную через
`Actions → Quality → Run workflow`. На сервере результат проверяется так:

```bash
cd /home/sysop/sentinel
git log -1 --oneline
systemctl list-timers sentinel-nightly.timer --all
```

Ручной запуск того же deploy без GitHub Actions:

```bash
cd /home/sysop/sentinel
make deploy
```
