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
НАДО ДОПИСАТЬ!!

/api/settings/* - НАДО ДОПИСАТЬ!!
/api/account/change_password — уже есть в auth.py

settings.html (формовый YAML без «всего файла»)
andromeda.html
поля-заглушки + «Сохранить (скоро)»