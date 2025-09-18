# app/core/auth.py
import os, secrets, time
from typing import Optional, Dict
from fastapi import APIRouter, Depends, Request, Response, HTTPException, status
from fastapi.responses import RedirectResponse
from passlib.hash import pbkdf2_sha256
from pydantic import BaseModel
from app.db.session import SessionLocal
from app.db.models import User
from starlette.middleware.base import BaseHTTPMiddleware

SESSION_TTL_S = 24*3600
_sessions: Dict[str, dict] = {}  # sid -> {username, exp}

def _now(): return int(time.time())

def get_user_by_name(db, username: str) -> Optional[User]:
    return db.query(User).filter(User.username==username).first()

def ensure_default_user():
    with SessionLocal() as db:
        u = get_user_by_name(db, "user")
        if not u:
            u = User(username="user", password_hash=pbkdf2_sha256.hash("default"))
            db.add(u); db.commit()

class LoginDTO(BaseModel):
    username: str
    password: str

router = APIRouter()

@router.post("/login")
def login(dto: LoginDTO, response: Response):
    with SessionLocal() as db:
        u = get_user_by_name(db, dto.username)
        if not u or not pbkdf2_sha256.verify(dto.password, u.password_hash):
            raise HTTPException(status_code=401, detail="Неверный логин или пароль")
    sid = secrets.token_urlsafe(24)
    _sessions[sid] = {"username": dto.username, "exp": _now()+SESSION_TTL_S}
    resp = {"ok": True}
    response.set_cookie("sid", sid, httponly=True, samesite="Lax")
    return resp

@router.post("/logout")
def logout(request: Request):
    sid = request.cookies.get("sid")
    if sid: _sessions.pop(sid, None)
    return {"ok": True}

class ChangePasswordDTO(BaseModel):
    old_password: str
    new_password: str
    confirm_password: str

@router.post("/account/change_password")
def change_password(dto: ChangePasswordDTO, request: Request):
    if dto.new_password != dto.confirm_password:
        raise HTTPException(400, "Пароли не совпадают")
    sid = request.cookies.get("sid")
    if not sid or sid not in _sessions: raise HTTPException(401, "Нет сессии")
    username = _sessions[sid]["username"]
    with SessionLocal() as db:
        u = get_user_by_name(db, username)
        if not u or not pbkdf2_sha256.verify(dto.old_password, u.password_hash):
            raise HTTPException(400, "Старый пароль неверный")
        if pbkdf2_sha256.verify(dto.new_password, u.password_hash):
            raise HTTPException(400, "Новый пароль совпадает со старым")
        u.password_hash = pbkdf2_sha256.hash(dto.new_password)
        db.commit()
    return {"ok": True}

def require_auth_middleware():
    class AuthMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request, call_next):
            path = request.url.path
            public = path.startswith("/login") or path.startswith("/static") or path.startswith("/api/login") or path.startswith("/docs")
            if public: return await call_next(request)
            sid = request.cookies.get("sid")
            if not sid or sid not in _sessions or _sessions[sid]["exp"] < _now():
                return RedirectResponse("/login", status_code=302)
            _sessions[sid]["exp"] = _now()+SESSION_TTL_S
            return await call_next(request)
    return AuthMiddleware
