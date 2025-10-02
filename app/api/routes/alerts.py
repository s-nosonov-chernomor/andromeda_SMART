# app/api/routes/alerts.py
from __future__ import annotations

from typing import Any, Dict
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request, Body
from fastapi.responses import JSONResponse, PlainTextResponse, HTMLResponse
from fastapi.templating import Jinja2Templates

import yaml

from app.services.alerts_engine import alerts_engine
from app.core.config import settings

from app.core.alerts_config import read_alerts_cfg, save_alerts_cfg
from app.core.validate_alerts import validate_alerts_cfg
from app.services.alerts_runtime import engine_instance

router = APIRouter()

# Папка с шаблонами (как в других роутерах)
templates = Jinja2Templates(directory="app/web/templates")


# ─────────────────────────────────────────────────────────────────────────────
# UI
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/alerts", response_class=HTMLResponse)
def ui_alerts(request: Request):
    """
    Страница «Оповещения» (UI). Шаблон добавим следующим файлом.
    """
    return templates.TemplateResponse("alerts.html", {"request": request, "title": "Оповещения"})


# ─────────────────────────────────────────────────────────────────────────────
# API: конфиг и справочная информация
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/api/alerts/enums")
def get_enums():
    """
    Enum-ы для UI.
    """
    return {
        "flow_types": ["telegram", "ronet"],
    }


@router.get("/api/alerts/config")
def get_alerts_config():
    """
    Вернуть текущую нормализованную конфигурацию alerts.yaml (JSON).
    Читаем через storage, который сам делает нормализацию legacy-полей.
    """
    try:
        return read_alerts_cfg()
    except Exception as e:
        raise HTTPException(500, f"read config failed: {e}")


@router.put("/api/alerts/config")
async def put_alerts_config(request: Request):
    """
    Сохранить конфиг (валидируем, нормализуем legacy `flow.telegram*` в `options.telegram.*`,
    пишем YAML, перезапускаем рантайм).
    """
    ct = (request.headers.get("content-type") or "").split(";")[0].strip().lower()

    # 1) читаем тело
    if ct.startswith("application/json"):
        try:
            data = await request.json()
            if not isinstance(data, dict):
                raise HTTPException(400, "JSON config must be an object")
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(400, f"Bad JSON: {e}")
    else:
        raw = (await request.body()).decode("utf-8", errors="replace")
        try:
            data = yaml.safe_load(raw) or {}
            if not isinstance(data, dict):
                raise HTTPException(400, "YAML root must be a mapping")
        except yaml.YAMLError as e:
            raise HTTPException(400, f"YAML parse error: {e}")

    # 2) нормализуем legacy-поля токенов (если фронт прислал flow.telegram.*)
    try:
        for f in data.get("flows", []) or []:
            if f.get("type") == "telegram":
                opts = f.setdefault("options", {})
                tele = opts.get("telegram") or {}
                legacy = f.get("telegram") or {}
                # переносим непустые значения из legacy
                if legacy:
                    if not tele.get("bot_token") and legacy.get("bot_token"):
                        tele["bot_token"] = str(legacy.get("bot_token"))
                    if not tele.get("chat_id") and legacy.get("chat_id") is not None:
                        tele["chat_id"] = str(legacy.get("chat_id"))
                    opts["telegram"] = tele
                # и удаляем верхнеуровневый срез
                f.pop("telegram", None)
    except Exception as e:
        raise HTTPException(400, f"normalize failed: {e}")

    # 3) валидация
    try:
        validate_alerts_cfg(data)
    except ValueError as e:
        raise HTTPException(400, f"Некорректный конфиг: {e}")

    # 4) сохраняем YAML (+ backup/rotation) и перезапускаем движок
    try:
        backup = save_alerts_cfg(data)
    except Exception as e:
        raise HTTPException(500, f"save failed: {e}")

    # перезапуск рантайма с переданным конфигом
    eng = engine_instance()
    if eng:
        try:
            eng.reload(data)
        except Exception as e:
            # файл уже сохранён; сообщаем о состоянии честно
            raise HTTPException(500, f"config saved (backup: {backup}), but runtime reload failed: {e}")

    return {"ok": True, "backup": backup}



@router.post("/api/alerts/reload")
def reload_alerts_config():
    """
    Перечитать alerts.yaml с диска и перезапустить потоки.
    """
    try:
        alerts_engine.reload_config()
        return {"ok": True}
    except Exception as e:
        raise HTTPException(500, f"reload failed: {e}")


@router.get("/api/alerts/known_params")
def list_known_params():
    """
    Вернуть список параметров из основного YAML (для выпадающего списка «Имя»).
    Элементы: { line, unit_id, name, object }
    """
    try:
        items = alerts_engine.list_known_params_from_main_cfg()
        return items
    except Exception as e:
        raise HTTPException(500, f"list params failed: {e}")
