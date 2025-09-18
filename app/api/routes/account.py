# app/api/routes/account.py
from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from fastapi import APIRouter, HTTPException, Request, Response, Depends
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field, validator
from passlib.hash import pbkdf2_sha256

from app.core.config import settings

router = APIRouter()
templates = Jinja2Templates(directory="app/web/templates")

# ─────────────────────────────────────────────────────────────────────────────
# Простое файловое хранилище пользователей
# ─────────────────────────────────────────────────────────────────────────────

_ACCOUNTS_LOCK = threading.Lock()

def _accounts_path() -> str:
    path = getattr(settings, "accounts_path", None) or "./data/users.json"
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    return path

def _now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def _load_users() -> Dict[str, Any]:
    path = _accounts_path()
    if not os.path.exists(path):
        # создать дефолтного пользователя: user / default
        default_hash = pbkdf2_sha256.hash("default")
        data = {
            "updated_at": _now_iso(),
            "users": {
                "user": {"hash": default_hash}
            }
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return data
    with open(path, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except Exception:
            # сломан файл — сделаем резервную копию и пересоздадим
            backup = path + ".corrupt." + datetime.utcnow().strftime("%Y%m%d%H%M%S")
            try:
                os.replace(path, backup)
            except Exception:
                pass
            return _load_users()

def _save_users(data: Dict[str, Any]) -> None:
    path = _accounts_path()
    tmp = path + ".tmp"
    data["updated_at"] = _now_iso()
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def _ensure_default_user() -> None:
    with _ACCOUNTS_LOCK:
        data = _load_users()
        if "users" not in data or not data["users"]:
            data["users"] = {}
        if "user" not in data["users"]:
            data["users"]["user"] = {"hash": pbkdf2_sha256.hash("default")}
            _save_users(data)

# ─────────────────────────────────────────────────────────────────────────────
# Модели запросов/ответов
# ─────────────────────────────────────────────────────────────────────────────

class LoginDTO(BaseModel):
    username: str = Field(..., min_length=1, max_length=64)
    password: str = Field(..., min_length=1, max_length=256)
    remember: bool = False

class ChangePasswordDTO(BaseModel):
    old_password: str = Field(..., min_length=1, max_length=256)
    new_password: str = Field(..., min_length=6, max_length=256)   # минимальная длина 6
    confirm_new: str = Field(..., min_length=6, max_length=256)

    @validator("confirm_new")
    def _match(cls, v, values):
        if "new_password" in values and v != values["new_password"]:
            raise ValueError("Пароли не совпадают")
        return v

class MeDTO(BaseModel):
    authenticated: bool
    username: Optional[str] = None

# ─────────────────────────────────────────────────────────────────────────────
# Сессионные утилиты
# ─────────────────────────────────────────────────────────────────────────────

SESSION_KEY = "auth_user"  # хранится в request.session

def _require_session(request: Request) -> str:
    """Возвращает имя пользователя из сессии или бросает 401."""
    u = request.session.get(SESSION_KEY)
    if not u:
        raise HTTPException(401, "not authenticated")
    return str(u)

def _set_session_login(response: Response, request: Request, username: str, remember: bool):
    # Starlette SessionMiddleware уже хранит всё в cookie — достаточно записать в request.session.
    request.session[SESSION_KEY] = username
    # Продлим cookie (по умолчанию session cookie), если remember=True — зададим max_age
    if remember:
        # переустанавливаем cookie с max_age (middleware сам подпишет содержимое)
        # Нет прямого API изменить max_age у SessionMiddleware здесь — оставим как session cookie.
        # Если нужен max_age, можно завести собственный cookie-флаг. Для простоты — пропустим.
        pass

def _clear_session(request: Request):
    request.session.pop(SESSION_KEY, None)

# ─────────────────────────────────────────────────────────────────────────────
# API
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/api/auth/login")
def api_login(dto: LoginDTO, request: Request):
    """
    Логин: проверка логина/пароля и создание сессии.
    При первом запуске создаётся user/default.
    """
    _ensure_default_user()
    uname = dto.username.strip()
    if not uname:
        raise HTTPException(400, "empty username")
    uname_key = uname  # чувствительность к регистру — оставим как есть

    with _ACCOUNTS_LOCK:
        data = _load_users()
        user = (data.get("users") or {}).get(uname_key)
        if not user:
            raise HTTPException(401, "Неверные учетные данные")
        h = user.get("hash") or ""
        try:
            ok = pbkdf2_sha256.verify(dto.password, h)
        except Exception:
            ok = False
        if not ok:
            raise HTTPException(401, "Неверные учетные данные")

    _set_session_login(Response(), request, uname_key, dto.remember)
    return {"ok": True, "username": uname_key}

@router.post("/api/auth/logout")
def api_logout(request: Request):
    _clear_session(request)
    return {"ok": True}

@router.get("/api/auth/me", response_model=MeDTO)
def api_me(request: Request):
    u = request.session.get(SESSION_KEY)
    return MeDTO(authenticated=bool(u), username=str(u) if u else None)

@router.post("/api/auth/change_password")
def api_change_password(dto: ChangePasswordDTO, request: Request, username: str = Depends(_require_session)):
    """
    Смена пароля текущего пользователя:
     - сверяем старый
     - пишем новый hash в JSON
    """
    with _ACCOUNTS_LOCK:
        data = _load_users()
        users = data.get("users") or {}
        user = users.get(username)
        if not user:
            raise HTTPException(404, "user not found")
        old_hash = user.get("hash") or ""
        if not pbkdf2_sha256.verify(dto.old_password, old_hash):
            raise HTTPException(400, "Старый пароль неверен")
        # минимальная политика: длина >=6 уже проверена валидатором
        new_hash = pbkdf2_sha256.hash(dto.new_password)
        user["hash"] = new_hash
        _save_users(data)

    return {"ok": True}

# ─────────────────────────────────────────────────────────────────────────────
# UI-роуты (опционально; можно не использовать, если фронт — SPA)
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/ui/login", response_class=HTMLResponse)
def ui_login(request: Request):
    """
    Простейшая страница логина.
    Если у тебя есть свой шаблон login.html — он будет отрендерен.
    Иначе вернём минимальную форму.
    """
    template_path = os.path.join("app/web/templates", "login.html")
    if os.path.exists(template_path):
        return templates.TemplateResponse("login.html", {"request": request})
    # Фолбэк: встроенная форма
    html = """<!doctype html>
<html lang="ru"><head><meta charset="utf-8">
<title>Вход</title>
<link rel="stylesheet" href="/static/andromeda.css">
</head>
<body class="auth-wrap">
  <div class="auth-card">
    <h1>Вход</h1>
    <p class="muted">Введите логин и пароль</p>
    <div class="grid">
      <label class="label">Логин</label>
      <input id="u" class="input" placeholder="user" autofocus>
      <label class="label">Пароль</label>
      <input id="p" class="input" type="password" placeholder="••••••">
      <label class="label"><input id="r" type="checkbox"> Запомнить</label>
      <button class="btn btn-primary" onclick="login()">Войти</button>
      <div id="e" class="muted"></div>
    </div>
  </div>
<script>
async function login(){
  const u = document.getElementById('u').value.trim();
  const p = document.getElementById('p').value;
  const r = document.getElementById('r').checked;
  const e = document.getElementById('e');
  e.textContent = '';
  const res = await fetch('/api/auth/login',{
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({username:u, password:p, remember:r})
  });
  if(!res.ok){ e.textContent = 'Ошибка: ' + await res.text(); return; }
  location.href = '/';
}
document.addEventListener('keydown', (ev)=>{ if(ev.key==='Enter') login(); });
</script>
</body></html>"""
    return HTMLResponse(html)

@router.get("/ui/logout")
def ui_logout(request: Request):
    _clear_session(request)
    return RedirectResponse(url="/ui/login", status_code=302)
