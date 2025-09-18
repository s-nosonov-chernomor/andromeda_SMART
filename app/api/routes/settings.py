# app/api/routes/settings.py
from __future__ import annotations
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException, Request
from app.core.config import settings
from app.services.hot_reload import start_lines, stop_lines
import yaml
import copy

router = APIRouter(prefix="/api/settings")

# ───────────────────────── helpers ─────────────────────────
def _cfg() -> Dict[str, Any]:
    """Безопасно получить текущий конфиг (глубокая копия)."""
    cfg = settings.get_cfg()
    if not isinstance(cfg, dict):
        raise HTTPException(500, "Config is not loaded")
    return copy.deepcopy(cfg)

def _write_cfg(cfg: Dict[str, Any]) -> str:
    """Сохранить YAML через settings + вернуть имя backup-файла (или '')."""
    backup_name = settings.save_yaml_config(cfg)
    # при необходимости — тут же перечитать в память (но save_yaml_config уже обновил self._cfg)
    # settings.load_yaml_config()
    return backup_name


# ───────────────────────── enums ─────────────────────────
@router.get("/enums")
def enums():
    return {
        "register_types": ["coil", "discrete", "holding", "input"],
        "param_modes": ["r", "rw"],
        "publish_modes": ["on_change", "interval", "on_change_and_interval"],
    }


# ───────────────────────── general (mqtt/polling/history/debug) ─────────────────────────
@router.get("/general")
def get_general():
    cfg = _cfg()
    return {
        "mqtt": cfg.get("mqtt", {}),
        "polling": cfg.get("polling", {}),
        "history": cfg.get("history", {}),
        "debug": cfg.get("debug", {}),
    }

@router.put("/general")
def put_general(body: Dict[str, Any]):
    cfg = _cfg()

    # минимальная валидация
    for k in ("mqtt", "polling", "history", "debug"):
        if k not in body:
            raise HTTPException(400, f"Missing section in body: {k}")

    cfg["mqtt"]    = body["mqtt"]
    cfg["polling"] = body["polling"]
    cfg["history"] = body["history"]
    cfg["debug"]   = body["debug"]

    backup = _write_cfg(cfg)
    return {"ok": True, "backup": backup}


# ───────────────────────── lines/params ─────────────────────────
@router.get("/lines")
def get_lines():
    cfg = _cfg()
    return cfg.get("lines", [])

@router.post("/param/add")
def add_param(payload: Dict[str, Any]):
    """
    payload = {
      "line": "line1",
      "unit_id": 1,
      "object": "object1_io",
      "param": {... ParamDTO ...}
    }
    """
    cfg = _cfg()
    line_name = payload.get("line")
    unit_id   = payload.get("unit_id")
    object_   = payload.get("object")
    param     = payload.get("param")

    if not line_name or unit_id is None or not object_ or not isinstance(param, dict):
        raise HTTPException(400, "Bad payload")

    lines: List[Dict[str, Any]] = cfg.setdefault("lines", [])

    # ищем/создаём линию
    line = next((ln for ln in lines if ln.get("name") == line_name), None)
    if not line:
        line = {"name": line_name, "device": "", "baudrate": 9600, "timeout": 0.1, "parity": "N", "stopbits": 1, "nodes": []}
        lines.append(line)

    nodes: List[Dict[str, Any]] = line.setdefault("nodes", [])
    node  = next((nd for nd in nodes if int(nd.get("unit_id", -1)) == int(unit_id)), None)
    if not node:
        node = {"unit_id": int(unit_id), "object": object_, "params": []}
        nodes.append(node)

    # добавляем/обновляем параметр (если имя совпадает — перезапишем)
    params: List[Dict[str, Any]] = node.setdefault("params", [])
    existing = next((p for p in params if p.get("name") == param.get("name")), None)
    if existing:
        params.remove(existing)
    params.append(param)

    backup = _write_cfg(cfg)
    return {"ok": True, "backup": backup}

@router.put("/param/update")
def update_param(body: Dict[str, Any]):
    """
    body = {
      "line": "line1",
      "unit_id": 1,
      "name": "q1",
      "updates": {... ParamDTO ...}
    }
    """
    cfg = _cfg()
    line_name = body.get("line")
    unit_id   = body.get("unit_id")
    name      = body.get("name")
    updates   = body.get("updates")

    if not line_name or unit_id is None or not name or not isinstance(updates, dict):
        raise HTTPException(400, "Bad payload")

    lines: List[Dict[str, Any]] = cfg.get("lines", [])
    line = next((ln for ln in lines if ln.get("name") == line_name), None)
    if not line:
        raise HTTPException(404, "Line not found")

    nodes: List[Dict[str, Any]] = line.get("nodes", [])
    node  = next((nd for nd in nodes if int(nd.get("unit_id", -1)) == int(unit_id)), None)
    if not node:
        raise HTTPException(404, "Node not found")

    params: List[Dict[str, Any]] = node.get("params", [])
    param  = next((p for p in params if p.get("name") == name), None)
    if not param:
        raise HTTPException(404, "Param not found")

    param.update(updates)

    backup = _write_cfg(cfg)
    return {"ok": True, "backup": backup}

@router.delete("/param/delete")
def delete_param(body: Dict[str, Any]):
    """
    body = { "line": "line1", "unit_id": 1, "name": "q1" }
    """
    cfg = _cfg()
    line_name = body.get("line")
    unit_id   = body.get("unit_id")
    name      = body.get("name")

    if not line_name or unit_id is None or not name:
        raise HTTPException(400, "Bad payload")

    lines: List[Dict[str, Any]] = cfg.get("lines", [])
    line = next((ln for ln in lines if ln.get("name") == line_name), None)
    if not line:
        raise HTTPException(404, "Line not found")

    nodes: List[Dict[str, Any]] = line.get("nodes", [])
    node  = next((nd for nd in nodes if int(nd.get("unit_id", -1)) == int(unit_id)), None)
    if not node:
        raise HTTPException(404, "Node not found")

    params: List[Dict[str, Any]] = node.get("params", [])
    idx    = next((i for i, p in enumerate(params) if p.get("name") == name), -1)
    if idx < 0:
        raise HTTPException(404, "Param not found")

    params.pop(idx)

    backup = _write_cfg(cfg)
    return {"ok": True, "backup": backup}


# ───────────────────────── disk ops ─────────────────────────
@router.post("/read_disk")
def read_disk():
    settings.load_yaml_config()
    return {"ok": True}

@router.post("/save_disk")
def save_disk():
    cfg = _cfg()
    backup = _write_cfg(cfg)
    return {"ok": True, "backup": backup}


# ───────────────────────── hot reload ─────────────────────────
@router.post("/reload")
def reload_lines(request: Request):
    """
    Перезапустить линии Modbus с текущим конфигом.
    Важно: в main.py на старте мы сохраняем mqtt_bridge в app.state.mqtt_bridge
    """
    # останавливаем текущие
    stop_lines()
    # запускаем новые
    mqtt_bridge = getattr(request.app.state, "mqtt_bridge", None)
    start_lines(settings.get_cfg(), mqtt_bridge)
    return {"ok": True}
