# Установка бота на сервер (Ubuntu 22.04 / 24.04)

Инструкция рассчитана на чистый VPS (подойдёт самый дешёвый, 1 CPU / 1 GB RAM)
и бесплатный тариф Helius (`USE_ENHANCED_WS=0`).

## 1. Что нужно подготовить заранее

| Что | Где взять |
|---|---|
| Helius API key | [dashboard.helius.dev](https://dashboard.helius.dev) → Create API Key |
| Токен Telegram-бота | написать [@BotFather](https://t.me/BotFather) → `/newbot` → скопировать токен |
| Свой chat_id | написать [@userinfobot](https://t.me/userinfobot) — пришлёт твой id |

После создания бота у BotFather **напиши своему боту любое сообщение** (нажми Start) —
иначе Telegram не даст ему писать тебе первым.

## 2. Установка системных пакетов

Подключись к серверу по SSH и выполни:

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3 python3-venv python3-pip redis-server git
```

Проверь, что Redis запустился:

```bash
sudo systemctl enable --now redis-server
redis-cli ping        # должен ответить PONG
```

## 3. Скачивание бота

Кладём бота в `/opt/pumpbot`:

```bash
sudo mkdir -p /opt/pumpbot
sudo chown $USER:$USER /opt/pumpbot
git clone https://github.com/businessmalasia-blip/bot.git /opt/pumpbot
cd /opt/pumpbot
git checkout claude/pump-fun-token-screener-mbqmil
```

## 4. Python-окружение

```bash
cd /opt/pumpbot
python3 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt
```

## 5. Настройка .env

```bash
cp .env.example .env
nano .env
```

Обязательно заполни три строки:

```
HELIUS_API_KEY=твой_ключ_helius
TELEGRAM_BOT_TOKEN=токен_от_BotFather
TELEGRAM_CHAT_ID=твой_chat_id
```

Для бесплатного тарифа Helius проверь, что стоит:

```
USE_ENHANCED_WS=0
TX_FETCH_RPS=0.25
VELOCITY_MIN_BUYERS=1
```

Остальное можно не трогать. Сохранение в nano: `Ctrl+O`, `Enter`, выход `Ctrl+X`.

## 6. Пробный запуск

```bash
cd /opt/pumpbot
.venv/bin/python main.py
```

В логе должно появиться:

```
Redis connected
Stats DB ready at screener_stats.db
Subscribed to Pump.fun logs (free-tier mode)
Bot is running
```

Если всё так — останови (`Ctrl+C`) и переходи к автозапуску.

## 7. Автозапуск через systemd

Создай файл сервиса:

```bash
sudo nano /etc/systemd/system/pumpbot.service
```

Вставь:

```ini
[Unit]
Description=Pump.fun Token Screener
After=network-online.target redis-server.service
Wants=network-online.target
Requires=redis-server.service

[Service]
Type=simple
WorkingDirectory=/opt/pumpbot
ExecStart=/opt/pumpbot/.venv/bin/python main.py
Restart=always
RestartSec=10
User=root

[Install]
WantedBy=multi-user.target
```

(если ставил под обычным пользователем — замени `User=root` на своё имя)

Запусти:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now pumpbot
```

## 8. Проверка и повседневные команды

```bash
sudo systemctl status pumpbot        # статус
journalctl -u pumpbot -f             # живые логи
sudo systemctl restart pumpbot       # перезапуск
sudo systemctl stop pumpbot          # остановка
```

В Telegram боту можно отправить `/stats` — он ответит статистикой по алертам
(винрейт появится, когда накопятся данные за 1/6/24 часа).

## 9. Обновление бота

```bash
cd /opt/pumpbot
git pull
.venv/bin/pip install -r requirements.txt
sudo systemctl restart pumpbot
```

## Особенности бесплатного тарифа Helius

- Лимит — 1M кредитов/месяц. Бот сэмплирует поток покупок (~0.25 запроса/сек),
  поэтому токены обнаруживаются с задержкой в десятки секунд — для нашей
  стратегии (вход на $10k) это приемлемо, но часть быстрых монет будет пропущена.
- Если кредиты кончаются раньше конца месяца — подними `MCAP_THRESHOLD`
  (например, до 9000), чтобы реже запускать дорогой анализ, или уменьши
  `TX_FETCH_RPS` до 0.15.
- Расход кредитов виден в дашборде Helius. Если решишь перейти на план
  Developer ($49) — просто поставь `USE_ENHANCED_WS=1` и `VELOCITY_MIN_BUYERS=5`
  в `.env` и перезапусти бота: задержки исчезнут, сэмплирование отключится.
