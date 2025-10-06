# app/api/routes/alerts.py
from __future__ import annotations

from typing import Any, Dict, Optional
from pathlib import Path
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request, Body
from fastapi.responses import JSONResponse, PlainTextResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
import yaml

from app.services.alerts_engine import alerts_engine   # CHANGED: будем обращаться прямо к синглтону
from app.services.alerts_runtime import engine_instance, ensure_started  # CHANGED
from app.core.validate_alerts import validate_alerts_cfg

router = APIRouter()

# Папка с шаблонами (как в других роутерах)
templates = Jinja2Templates(directory="app/web/templates")

# гарантируем запуск движка при подключении роутера
ensure_started()

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
    try:
        ensure_started()
        eng = engine_instance()
        return eng.dump_config() if eng else {"flows": []}
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
        backup = alerts_engine.save_config(data)
    except Exception as e:
        raise HTTPException(500, f"save failed: {e}")

    return {"ok": True, "backup": backup}

@router.post("/api/alerts/reload")
def reload_alerts_config():
    try:
        ensure_started()
        eng = engine_instance()
        if eng:
            eng.reload_config()
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

# ───────────────── Диагностика/тест ─────────────────
class TestTelegramDTO(BaseModel):
    flow_id: str
    text: str = "Проверка: 👋 это тестовое сообщение от USPD"

@router.get("/api/alerts/diag")
def alerts_diag():
    eng = engine_instance()
    if not eng:
        return {"running": False, "detail": "engine is not started"}
    try:
        return eng.diag()
    except Exception as e:
        raise HTTPException(500, f"diag failed: {e}")


@router.post("/api/alerts/test_telegram")
def alerts_test_telegram(dto: TestTelegramDTO):
    eng = engine_instance()
    if not eng:
        raise HTTPException(503, "engine is not running")
    try:
        ok, detail = eng.send_test_telegram(dto.flow_id, dto.text)
        return {"ok": ok, "detail": detail}
    except Exception as e:
        raise HTTPException(500, f"test send failed: {e}")

@router.get("/api/alerts/outbox")
def alerts_outbox(limit: int = 20):
    eng = engine_instance()
    if not eng:
        return {"running": False, "items": []}
    try:
        return {"running": True, "items": eng.get_last_flushes(limit)}
    except Exception as e:
        raise HTTPException(500, f"outbox failed: {e}")


class SimPublishDTO(BaseModel):
    line: str
    unit_id: int
    name: str
    value: Any
    pub_kind: str = "event"      # "event" | "interval"
    ts: Optional[str] = None     # ISO8601 или Unix (строкой), опц.

@router.post("/api/alerts/sim_publish")
def alerts_sim_publish(dto: SimPublishDTO):
    # используем singleton через runtime-обёртку
    eng = engine_instance()
    if not eng:
        raise HTTPException(503, "engine is not running")

    # разберём ts, если передан
    ts_dt = None
    if dto.ts:
        try:
            if dto.ts.isdigit():
                ts_dt = datetime.fromtimestamp(int(dto.ts), tz=timezone.utc)
            else:
                ts_dt = datetime.fromisoformat(dto.ts)
        except Exception:
            ts_dt = None

    eng.notify_publish(
        line=dto.line, unit_id=int(dto.unit_id), name=dto.name,
        value=dto.value, pub_kind=dto.pub_kind, ts=ts_dt
    )
    return {"ok": True}