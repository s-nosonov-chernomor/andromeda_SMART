# app/main.py
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from app.core.config import settings
from app.core.auth import router as auth_router, ensure_default_user, require_auth_middleware
from app.api.routes.current import router as current_router
from app.api.routes.journal import router as journal_router
from app.api.routes.settings import router as settings_router
from app.api.routes.mock import router as mock_router
from app.services.mqtt_bridge import MqttBridge
from app.services.hot_reload import start_lines, stop_lines, hot_reload_lines
from app.api.routes.andromeda_cfg import router as andromeda_router
import logging
from starlette.middleware.sessions import SessionMiddleware
from app.api.routes.account import router as account_router


app = FastAPI(title="USPD")
app.add_middleware(require_auth_middleware())
app.mount("/static", StaticFiles(directory="app/web/static"), name="static")
templates = Jinja2Templates(directory="app/web/templates")

app.add_middleware(SessionMiddleware, secret_key=settings.session_secret, same_site="lax")

# API
app.include_router(auth_router, prefix="/api", tags=["auth"])
app.include_router(current_router, tags=["current"])
app.include_router(journal_router, tags=["journal"])
app.include_router(settings_router, tags=["settings"])
app.include_router(mock_router, tags=["mock"])
app.include_router(andromeda_router, tags=["andromeda"])

# Страницы
@app.get("/login", response_class=HTMLResponse)
def page_login(request: Request): return templates.TemplateResponse("login.html", {"request": request})
@app.get("/", response_class=HTMLResponse)
def root(): return RedirectResponse("/current")
@app.get("/current", response_class=HTMLResponse)
def page_current(request: Request): return templates.TemplateResponse("current.html", {"request": request, "title":"Текущие"})
@app.get("/journal", response_class=HTMLResponse)
def page_journal(request: Request): return templates.TemplateResponse("journal.html", {"request": request, "title":"Журнал"})
@app.get("/settings", response_class=HTMLResponse)
def page_settings(request: Request): return templates.TemplateResponse("settings.html", {"request": request, "title":"Настройки"})
@app.get("/andromeda", response_class=HTMLResponse)
def page_andromeda(request: Request): return templates.TemplateResponse("andromeda.html", {"request": request, "title":"Андромеда"})

@app.get("/ui/andromeda", response_class=HTMLResponse)
def ui_andromeda():
    with open("app/web/templates/andromeda.html", "r", encoding="utf-8") as f:
        return f.read()
@app.get("/logout")
def page_logout(): return RedirectResponse("/api/logout", status_code=307)

# Старт сервисов
mqtt_bridge: MqttBridge = None
def start_services():
    global mqtt_bridge
    ensure_default_user()
    mqtt_bridge = MqttBridge(settings.cfg["mqtt"]); mqtt_bridge.connect()
    start_lines(settings.cfg, mqtt_bridge)

@app.on_event("startup")
def _startup():
    settings.load_yaml_config()

@app.on_event("shutdown")
def on_shutdown(): stop_lines()
