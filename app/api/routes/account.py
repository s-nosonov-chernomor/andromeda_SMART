# app/api/routes/account.py
from __future__ import annotations

import json
import os
from datetime import timedelta
from pathlib import Path
from typing import Dict, Any, Optional

from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from passlib.hash import pbkdf2_sha256

from app.core.config import settings

router = APIRouter()

# ── шаблоны ──────────────────────────────────────────────────────────────────
APP_DIR = Path(__file__).resolve().parents[2]       # .../app
TEMPLATES_DIR = APP_DIR / "web" / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# ── путь к файлу пользователей ───────────────────────────────────────────────
def _accounts_path() -> Path:
    p = Path(settings.accounts_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    return p

# ── низкоуровневое хранилище (без рекурсии!) ─────────────────────────────────
def _load_users() -> Dict[str, Any]:
    """
    Безопасно читает users.json.
    Если файла нет / пуст / битый — возвращает {"users": []}.
    Битый файл переименовывает в .bak, чтобы не мешал.
    """
    path = _accounts_path()
    if not path.exists() or path.stat().st_size == 0:
        return {"users": []}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or "users" not in data or not isinstance(data["users"], list):
            return {"users": []}
        return data
    except json.JSONDecodeError:
        # Бэкапнем кривой файл и начнём с чистого листа
        try:
            bkp = path.with_suffix(path.suffix + ".bak")
            path.replace(bkp)
        except Exception:
            pass
        return {"users": []}
    except Exception:
        return {"users": []}

def _save_users(data: Dict[str, Any]) -> None:
    path = _accounts_path()
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)

# ── утилиты ──────────────────────────────────────────────────────────────────
def _get_user(data: Dict[str, Any], username: str) -> Optional[Dict[str, Any]]:
    for u in data.get("users", []):
        if u.get("username") == username:
            return u
    return None

# Вызывается из main.py на старте
def _ensure_default_user() -> None:
    data = _load_users()
    if _get_user(data, "user") is None:
        data["users"].append({
            "username": "user",
            "password_hash": pbkdf2_sha256.hash("default"),
        })
        _save_users(data)

# dependency для защиты UI
def _require_session(request: Request) -> str:
    user = request.session.get("auth_user")
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user

# ── DTO ──────────────────────────────────────────────────────────────────────
class LoginDTO(BaseModel):
    username: str
    password: str

class ChangePasswordDTO(BaseModel):
    old_password: str
    new_password: str
    confirm_new_password: str

# ── UI: страница логина ──────────────────────────────────────────────────────
@router.get("/ui/login", response_class=HTMLResponse)
def ui_login(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "title": "Вход"})

# ── API: логин / логаут / смена пароля ───────────────────────────────────────
@router.post("/api/auth/login")
async def login(request: Request):
    # принимаем JSON или form-data
    data = _load_users()
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        payload = await request.json()
    else:
        form = await request.form()
        payload = dict(form)

    try:
        dto = LoginDTO(**payload)
    except Exception:
        raise HTTPException(status_code=400, detail="Bad payload")

    user = _get_user(data, dto.username)
    if not user or not pbkdf2_sha256.verify(dto.password, user.get("password_hash", "")):
        raise HTTPException(status_code=401, detail="Не верный логин или пароль")

    # сессия на 1 день
    request.session["auth_user"] = dto.username
    request.session["auth_until"] = (timedelta(days=1)).total_seconds()
    return {"ok": True, "user": dto.username}

@router.post("/api/auth/logout")
def logout(request: Request):
    request.session.clear()
    return {"ok": True}

@router.post("/api/auth/change_password")
async def change_password(request: Request):
    user = _require_session(request)
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        payload = await request.json()
    else:
        form = await request.form()
        payload = dict(form)

    try:
        dto = ChangePasswordDTO(**payload)
    except Exception:
        raise HTTPException(status_code=400, detail="Bad payload")

    if dto.new_password != dto.confirm_new_password:
        raise HTTPException(status_code=400, detail="Password confirmation mismatch")

    data = _load_users()
    u = _get_user(data, user)
    if not u:
        raise HTTPException(status_code=404, detail="User not found")

    if not pbkdf2_sha256.verify(dto.old_password, u.get("password_hash", "")):
        raise HTTPException(status_code=401, detail="Old password is incorrect")

    u["password_hash"] = pbkdf2_sha256.hash(dto.new_password)
    _save_users(data)
    return {"ok": True}

# ── экспорт зависимостей для main.py ─────────────────────────────────────────
# чтобы совпадали имена из твоего main.py
ensure_default_user = _ensure_default_user
require_session = _require_session
