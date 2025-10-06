# app/main.py
from __future__ import annotations

import logging
from pathlib import Path

from fastapi import FastAPI, Request, Depends, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from app.core.config import settings

# Авторизация/аккаунты (файловое хранилище)
from app.api.routes.account import (
    router as account_router,
    _ensure_default_user as ensure_default_user,   # создать user/default, если нет
    _require_session as require_session,           # защита UI-страниц
)

# Остальные роутеры API
from app.api.routes.current import router as current_router
from app.api.routes.journal import router as journal_router
from app.api.routes.settings import router as settings_router
from app.api.routes.mock import router as mock_router
from app.api.routes.andromeda_cfg import andromeda_router as andromeda_router

# Сервисы
from app.services.mqtt_bridge import MqttBridge
from app.services.hot_reload import start_lines, stop_lines

# БД (создать таблицы, в т.ч. telemetry_events)
from app.db.session import init_db

from app.api.routes.service import router as service_router

from app.api.routes.alerts import router as alerts_router
from app.services.alerts_runtime import ensure_started as start_engine_if_needed, stop_if_running as stop_engine_if_running



# ─────────────────────────────────────────────────────────────────────────────
# Пути к статике/шаблонам
# ─────────────────────────────────────────────────────────────────────────────
APP_DIR = Path(__file__).resolve().parent
STATIC_DIR = APP_DIR / "web" / "static"
TEMPLATES_DIR = APP_DIR / "web" / "templates"

# ─────────────────────────────────────────────────────────────────────────────
# Приложение
# ─────────────────────────────────────────────────────────────────────────────
app = FastAPI(title="USPD")

# cookie-сессии (для авторизации)
app.add_middleware(SessionMiddleware, secret_key=settings.session_secret, same_site="lax")

# статика и шаблоны
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# ─────────────────────────────────────────────────────────────────────────────
# Подключаем роутеры
# ─────────────────────────────────────────────────────────────────────────────
# Авторизация (файловое JSON-хранилище пользователей)
app.include_router(account_router, tags=["auth"])

# Ваши API
app.include_router(current_router, tags=["current"])
app.include_router(journal_router, prefix="/api", tags=["journal"])  # даёт /api/events и /api/journal/events
app.include_router(settings_router, tags=["settings"])
app.include_router(mock_router, tags=["mock"])
app.include_router(andromeda_router, tags=["andromeda"])

app.include_router(service_router, tags=["service"])

app.include_router(alerts_router)
# ─────────────────────────────────────────────────────────────────────────────
# Совместимость со «старыми» путями
# ─────────────────────────────────────────────────────────────────────────────
# Старт/стоп
@app.on_event("startup")
def _on_startup():
    try:
        start_engine_if_needed()
    except Exception as e:
        # логируйте по желанию
        print("alerts engine start failed:", e)

@app.on_event("shutdown")
def _on_shutdown():
    try:
        stop_engine_if_running()
    except Exception:
        pass
@app.get("/login")
def compat_login_page():
    return RedirectResponse(url="/ui/login", status_code=302)

@app.api_route("/api/login", methods=["POST"])
async def compat_api_login():
    # Старый клиент стучится сюда — пробрасываем на новый эндпоинт авторизации
    return RedirectResponse(url="/api/auth/login", status_code=307)

# Корень: если не залогинен — на логин; иначе — на текущие
@app.get("/")
def root(request: Request):
    if not request.session.get("auth_user"):
        return RedirectResponse(url="/ui/login", status_code=302)
    return RedirectResponse(url="/current", status_code=302)

# ─────────────────────────────────────────────────────────────────────────────
# UI-страницы (защищённые сессией)
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/ui/login", response_class=HTMLResponse)
def page_login(request: Request):
    return templates.TemplateResponse(
        "login.html",
        {"request": request, "title": "Вход"},
    )

@app.get("/current", response_class=HTMLResponse)
def page_current(request: Request, user: str = Depends(require_session)):
    return templates.TemplateResponse(
        "current.html",
        {"request": request, "title": "Текущие", "user": user},
    )

@app.get("/journal", response_class=HTMLResponse)
def page_journal(request: Request, user: str = Depends(require_session)):
    return templates.TemplateResponse(
        "journal.html",
        {"request": request, "title": "Журнал", "user": user},
    )

@app.get("/settings", response_class=HTMLResponse)
def page_settings(request: Request, user: str = Depends(require_session)):
    return templates.TemplateResponse(
        "settings.html",
        {"request": request, "title": "Настройки", "user": user},
    )

@app.get("/andromeda", response_class=HTMLResponse)
def page_andromeda(request: Request, user: str = Depends(require_session)):
    return templates.TemplateResponse(
        "andromeda.html",
        {"request": request, "title": "Андромеда", "user": user},
    )

# Выход: чистим сессию и на логин
@app.get("/logout")
def page_logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/ui/login", status_code=302)

# ─────────────────────────────────────────────────────────────────────────────
# Старт сервисов на поднятии приложения
# ─────────────────────────────────────────────────────────────────────────────
mqtt_bridge: MqttBridge | None = None

@app.on_event("startup")
def _startup():
    # 1) грузим YAML
    settings.load_yaml_config()

    # 2) создаём таблицы БД (включая telemetry_events)
    init_db()

    # 3) гарантируем дефолтного пользователя (user/default)
    ensure_default_user()

    # 4) поднимаем MQTT и Modbus-линии
    global mqtt_bridge
    mqtt_bridge = MqttBridge(settings.mqtt)

    try:
        mqtt_bridge.connect()
    except Exception as e:
        logging.getLogger("web").error("mqtt connect error (non-fatal): %s", e)

    start_lines(settings.get_cfg(), mqtt_bridge)

    app.state.mqtt_bridge = mqtt_bridge
    logging.getLogger("web").info("web ui ready")

@app.on_event("shutdown")
def _shutdown():
    stop_lines()

@app.get("/.well-known/appspecific/com.chrome.devtools.json")
def _chrome_devtools_probe():
    # Глушим «пинг» от Chrome DevTools, чтобы не мусорил в логах
    return Response(status_code=204)