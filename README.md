Andromeda/
├─ app/
│  ├─ main.py                 # запуск FastAPI + роутинг страниц
│  ├─ core/
│  │  ├─ config.py            # чтение config.yaml, пути, глобальные настройки
│  │  ├─ auth.py              # авторизация, куки-сессии, смена пароля
│  │  └─ state.py             # "текущие значения" из опросчика (in-memory)
│  ├─ db/
│  │  ├─ session.py           # engine, SessionLocal
│  │  └─ models.py            # TelemetryEvent, User
│  ├─ services/
│  │  ├─ mqtt_bridge.py       # MQTT мост (публикация + запись в БД)
│  │  ├─ modbus_line.py       # Опрос Modbus RTU (пакетный, ошибки, heartbeat)
│  │  └─ hot_reload.py        # старт/стоп линий, hot-reload настроек линий
│  ├─ api/
│  │  └─ routes/
│  │     ├─ current.py        # /api/current
│  │     ├─ journal.py        # /api/events (с фильтрами)
│  │     ├─ settings.py       # /api/settings/* CRUD по yaml (формовая)
│  │     ├─ account.py        # /api/account/change_password
│  │     └─ mock.py           # /api/mock/send_control (эмуляция управления)
│  └─ web/
│     ├─ templates/
│     │  ├─ base.html         # layout: верхнее меню + модал смены пароля
│     │  ├─ login.html        # страница авторизации
│     │  ├─ current.html      # "Текущие"
│     │  ├─ journal.html      # "Журнал"
│     │  ├─ settings.html     # "Настройки" (формы + таблица регистров)
│     │  └─ andromeda.html    # "Андромеда" (заглушки)
│     └─ static/
│        ├─ app.css
│        └─ app.js
├─ config.yaml                # твой YAML конфиг (без комментариев в бою)
├─ run.py                     # точка входа (uvicorn)
└─ README.md

app/core/auth.py
учётка по умолчанию: user / default
хранение пароля в БД (hash PBKDF2)
cookie-сессии
модалка «Сменить пароль» — сервер проверяет старый пароль, запрещает одинаковый новый, возвращает JSON

app/db/models.py
сокращённая таблица журнала: только нужные колонки (topic, object, line, unit, reg info, value NULL, code, message, silent_for_s, ts)
таблица пользователей

app/core/state.py
«Текущие»: in-memory словарь, обновляется из опросчика

app/services/mqtt_bridge.py
публикация + запись в БД TelemetryEvent (без payload)
поддержка status_details и value=None
принимает context (line/unit/addr/…): это даёт «Журнал» ровно то, что ты просил

app/services/modbus_line.py
формирует контекст для БД + обновляет «Текущие»
режим публикации: on_change / interval / both
heartbeat с value=null и кодами ошибок + silent_for_s

Общие разделы

debug (object) — управление логами

enabled (bool, default: false) — общий уровень логирования DEBUG.

log_reads (bool, default: false) — логировать каждое успешное чтение регистра.

summary_every_s (int, default: 0) — период печати сводок по узлам (0 = выкл).

mqtt (object) — параметры MQTT-клиента

host (string) — адрес брокера.

port (int, default: 1883) — порт брокера.

client_id (string|null, default: "") — client id (можно пустой).

base_topic (string, default: "/devices") — базовый префикс для относительных топиков.

Если param.topic не начинается с /, публикуем в base_topic/<topic>. Если начинается с /, считаем абсолютным и публикуем «как есть».

qos (int {0,1,2}, default: 0) — QoS публикаций.

retain (bool, default: false) — признак retain сообщений.

db (object) — хранилище истории публикаций

url (string, например sqlite:///./data/data.db) — строка подключения SQLAlchemy.

history (object) — ротация таблицы истории

max_rows (int, default: 50000) — сколько последних записей хранить (0 = без лимита).

ttl_days (int, default: 14) — удалить записи старше N дней (0 = без TTL).

cleanup_every (int, default: 500) — как часто (через сколько вставок) выполнять очистку.

polling (object) — глобальные настройки цикла опроса

interval_ms (int, default: 1000) — базовый шаг цикла опроса.

jitter_ms (int, default: 0…120) — добавочный случайный «джиттер» между проходами.

max_errors_before_backoff (int, default: 5) — после N подряд неответов по узлу включаем локальный бэкофф.

backoff_ms (int, default: 500) — пауза бэкоффа для проблемного узла.

port_retry_backoff_s (int, default: 5) — период попыток переоткрыть упавший/занятый порт.

batch_read (object) — пакетное чтение последовательных регистров

enabled (bool, default: false) — включить объединение запросов.

max_bits (int, default: 64…1968) — максимум coil/discrete за один запрос.

max_registers (int, default: 60…120) — максимум holding/input за один запрос.

Линии Modbus

lines (array of object) — список физических линий, каждая — отдельный поток.

name (string) — логическое имя линии (видно в UI/логах).

device (string) — порт (COM4, /dev/ttyUSB0, и т.п.).

baudrate (int, default: 9600) — скорость.

timeout (float, default: 0.1) — таймаут ответа, сек.

parity (string {"N","E","O"}, default: "N") — чётность.

stopbits (int {1,2}, default: 1) — стоп-биты.

port_retry_backoff_s (int, optional) — перезапись глобального дефолта для данной линии.

rs485_rts_toggle (bool, optional) — зарезервировано (сейчас не используется).

nodes (array of object) — узлы (Modbus slave / unit id):

unit_id (int) — адрес узла.

object (string) — логическое имя объекта (используем в темах/интерфейсе).

num_object (int, optional) — номер объекта (для внешних систем; не влияет на опрос).

params (array of object) — параметры/регистры узла:

name (string) — имя параметра (уникально в пределах узла).

register_type (string {"coil","discrete","holding","input"}) — тип регистра.

address (int) — адрес (обычно 0-based; в логах нормализуем при необходимости).

scale (number, default: 1.0) — коэффициент: итоговое значение = raw / scale.

mode (string {"r","rw"}, default: "r") — режим (только чтение или чтение/запись).

publish_mode (string {"on_change","interval","on_change_and_interval"}, default: "on_change")

on_change — публикуем только при изменении значения.

interval — публикуем каждые publish_interval_ms независимо от изменений.

on_change_and_interval — и при изменении, и по интервалу.

publish_interval_ms (int, default: 0) — период публикации для режимов с интервалом.

topic (string|null) — относительный или абсолютный MQTT-топик:

не начинается с / → считается относительным, публикуется в <base_topic>/<topic>;

начинается с / → абсолютный, публикуется «как есть».

error_state (int {0,1}, optional) — значение, при котором трактуем параметр как «аварийный» (для дискретных).

display_error_text (string, optional) — текст для UI при error_state.

mqttROM (string, optional) — имя поля при интеграции с ПО «РОМ».