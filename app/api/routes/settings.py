# app/api/routes/settings.py
from __future__ import annotations

from fastapi import APIRouter, UploadFile, File, HTTPException, Request
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional
from io import BytesIO
import yaml, copy, io
from copy import deepcopy
from openpyxl import Workbook, load_workbook

from app.core.config import settings
from app.services.hot_reload import start_lines, stop_lines, hot_reload_lines
from app.core.validate_cfg import validate_cfg


# ─────────────────────────────────────────────────────────────────────────────
# helpers
# ─────────────────────────────────────────────────────────────────────────────

def _cfg() -> Dict[str, Any]:
    cfg = settings.get_cfg()
    if not isinstance(cfg, dict):
        raise HTTPException(500, "Config is not loaded")
    return copy.deepcopy(cfg)

def _write_cfg(cfg: Dict[str, Any]) -> str:
    """Сохранить YAML через settings + вернуть имя backup-файла (или '')."""
    try:
        validate_cfg(cfg)  # ← проверяем ПЕРЕД записью
    except ValueError as e:
        # отдаём 400 в читаемом виде
        from fastapi import HTTPException
        raise HTTPException(400, f"Некорректный конфиг: {e}")

    backup_name = settings.save_yaml_config(cfg)
    return backup_name


def _find_line(cfg: Dict[str, Any], line_name: str) -> Optional[Dict[str, Any]]:
    for ln in cfg.get("lines", []) or []:
        if ln.get("name") == line_name:
            return ln
    return None

def _find_node(line: Dict[str, Any], unit_id: int) -> Optional[Dict[str, Any]]:
    for nd in line.get("nodes", []) or []:
        if int(nd.get("unit_id")) == int(unit_id):
            return nd
    return None

# ─────────────────────────────────────────────────────────────────────────────
# схемы
# ─────────────────────────────────────────────────────────────────────────────

class BatchRead(BaseModel):
    enabled: Optional[bool] = None
    max_bits: Optional[int] = None
    max_registers: Optional[int] = None

class Polling(BaseModel):
    interval_ms: Optional[int] = None
    jitter_ms: Optional[int] = None
    backoff_ms: Optional[int] = None
    max_errors_before_backoff: Optional[int] = None
    port_retry_backoff_s: Optional[int] = None
    batch_read: Optional[BatchRead] = None

class Mqtt(BaseModel):
    host: Optional[str] = None
    port: Optional[int] = None
    base_topic: Optional[str] = None
    qos: Optional[int] = None
    retain: Optional[bool] = None
    client_id: Optional[str] = None

class Debug(BaseModel):
    enabled: Optional[bool] = None
    log_reads: Optional[bool] = None
    summary_every_s: Optional[int] = None

class History(BaseModel):
    max_rows: Optional[int] = None
    ttl_days: Optional[int] = None
    cleanup_every: Optional[int] = None

class GeneralDTO(BaseModel):
    mqtt: Mqtt
    polling: Polling
    history: History
    debug: Debug

class ParamDTO(BaseModel):
    name: str
    register_type: str
    address: int
    scale: Optional[float] = 1.0
    mode: str = "r"
    publish_mode: Optional[str] = "on_change"
    publish_interval_ms: Optional[int] = 0
    topic: Optional[str] = None
    # новые поля
    error_state: Optional[Any] = None
    display_error_text: Optional[str] = None
    mqttROM: Optional[str] = None

class AddParamDTO(BaseModel):
    line: str
    unit_id: int
    object: str
    # если узла нет — создадим; можно передать его номер
    num_object: Optional[int] = 1
    param: ParamDTO

class UpdateParamDTO(BaseModel):
    line: str
    unit_id: int
    name: str
    updates: ParamDTO

class LineDTO(BaseModel):
    name: str
    device: Optional[str] = ""
    baudrate: Optional[int] = 9600
    timeout: Optional[float] = 0.1
    parity: Optional[str] = "N"           # N/E/O
    stopbits: Optional[int] = 1           # 1/2
    port_retry_backoff_s: Optional[int] = 5
    rs485_rts_toggle: Optional[bool] = False
    nodes: Optional[List[Dict[str, Any]]] = None  # узлы с unit_id/object/num_object/params...

class LinesFullDTO(BaseModel):
    lines: List[LineDTO]

router = APIRouter(prefix="/api/settings", tags=["settings"])

# ─────────────────────────────────────────────────────────────────────────────
# enums (для UI)
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/enums")
def get_enums():
    return {
        "register_types": ["coil", "discrete", "holding", "input"],
        "param_modes": ["r", "rw"],
        "publish_modes": ["on_change", "interval", "on_change_and_interval"],
        "parity": ["N", "E", "O"],
        "stopbits": [1, 2]
    }

# ─────────────────────────────────────────────────────────────────────────────
# Общие
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/general")
def get_general():
    cfg = _cfg()
    return {
        "mqtt": cfg.get("mqtt", {}),
        "db": cfg.get("db", {}),               # ← добавили БД
        "polling": cfg.get("polling", {}),
        "history": cfg.get("history", {}),
        "debug": cfg.get("debug", {}),
        "serial": cfg.get("serial", {}),
        "addressing": cfg.get("addressing", {"normalize": True}),
    }

@router.put("/general")
def put_general(body: Dict[str, Any]):
    for k in ("mqtt", "db", "polling", "history", "debug", "serial"):
        if k not in body:
            raise HTTPException(400, f"Missing section in body: {k}")

    cfg = _cfg()
    cfg["mqtt"] = body["mqtt"]
    cfg["db"] = body["db"] or {"url": "sqlite:///./data/data.db"}
    cfg["polling"] = body["polling"]
    cfg["history"] = body["history"]
    cfg["debug"] = body["debug"]
    cfg["serial"] = body["serial"] or {"echo": False}
    cfg["addressing"] = {
        "normalize": bool((body.get("addressing") or {}).get("normalize", True))  # ← ДОБАВЬ ЭТО
    }

    backup = _write_cfg(cfg)
    return {"ok": True, "backup": backup}

# ─────────────────────────────────────────────────────────────────────────────
# ЛИНИИ целиком (редактирование портов/скоростей и т.п.)
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/lines")
def get_lines():
    return _cfg().get("lines", [])

@router.post("/line/add")
def add_line(payload: Dict[str, Any]):
    name = (payload.get("name") or "").strip()
    if not name:
        raise HTTPException(400, "Missing line name")
    cfg = _cfg()
    lines: List[Dict[str, Any]] = cfg.setdefault("lines", [])
    if any((ln.get("name") == name) for ln in lines):
        raise HTTPException(409, "Line already exists")

    # дефолты не затирают YAML, просто добавляем новую
    lines.append({
        "name": name,
        "device": "",
        "baudrate": 9600,
        "timeout": 0.1,
        "parity": "N",
        "stopbits": 1,
        "port_retry_backoff_s": 5,
        "nodes": []
    })
    backup = _write_cfg(cfg)
    return {"ok": True, "backup": backup}

@router.put("/line/update")
def update_line(body: Dict[str, Any]):
    body = body or {}
    name = body.get("name")
    if not name:
        raise HTTPException(400, "Missing line name")

    cfg = _cfg()
    lines: List[Dict[str, Any]] = cfg.get("lines", [])
    line = next((ln for ln in lines if ln.get("name") == name), None)
    if not line:
        raise HTTPException(404, "Line not found")

    # поддерживаем оба формата: {name, updates:{...}} ИЛИ {name, device,...}
    updates = body.get("updates") or {
        k: body[k] for k in (
            "device", "baudrate", "parity", "stopbits",
            "timeout", "port_retry_backoff_s", "rs485_rts_toggle"
        ) if k in body
    }

    for k, v in updates.items():
        if k in ("device","baudrate","parity","stopbits","timeout","port_retry_backoff_s","rs485_rts_toggle"):
            line[k] = v

    backup = _write_cfg(cfg)
    return {"ok": True, "backup": backup}

@router.delete("/line/delete")
def delete_line(body: Dict[str, Any]):
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(400, "Missing line name")

    cfg = _cfg()
    lines: List[Dict[str, Any]] = cfg.get("lines", [])
    idx = next((i for i, ln in enumerate(lines) if ln.get("name") == name), -1)
    if idx < 0:
        raise HTTPException(404, "Line not found")

    # удаляем линию целиком (вместе с nodes/params)
    lines.pop(idx)

    backup = _write_cfg(cfg)
    return {"ok": True, "backup": backup}

@router.post("/node/add")
def add_node(body: Dict[str, Any]):
    body = body or {}
    node_block = body.get("node")
    if isinstance(node_block, dict):
        line_name = body.get("line")
        unit_id   = node_block.get("unit_id")
        object_   = (node_block.get("object") or "").strip()
        num_obj   = node_block.get("num_object", None)
    else:
        line_name = body.get("line")
        unit_id   = body.get("unit_id")
        object_   = (body.get("object") or "").strip()
        num_obj   = body.get("num_object", None)


    if not line_name or unit_id is None or not object_:
        raise HTTPException(400, "Bad payload")

    cfg = _cfg()
    lines: List[Dict[str, Any]] = cfg.get("lines", [])
    line = next((ln for ln in lines if ln.get("name") == line_name), None)
    if not line:
        raise HTTPException(404, "Line not found")

    nodes: List[Dict[str, Any]] = line.setdefault("nodes", [])
    if any(int(nd.get("unit_id", -1)) == int(unit_id) for nd in nodes):
        raise HTTPException(409, "Node with this unit_id already exists")

    node = {"unit_id": int(unit_id), "object": object_, "params": []}
    if num_obj is not None:
        node["num_object"] = int(num_obj)
    nodes.append(node)

    backup = _write_cfg(cfg)
    return {"ok": True, "backup": backup}

@router.put("/node/update")
def update_node(body: Dict[str, Any]):
    """
    body = {
      "line": "line1",
      "old_unit_id": 1,
      "updates": {"unit_id": 2, "object": "new_obj", "num_object": 5}
    }
    """
    body = body or {}
    line_name = body.get("line")
    old_unit_id = body.get("old_unit_id")
    updates = body.get("updates") or {}
    if not line_name or old_unit_id is None:
        raise HTTPException(400, "Bad payload (need line, old_unit_id)")

    cfg = _cfg()
    lines: List[Dict[str, Any]] = cfg.get("lines", [])
    line = next((ln for ln in lines if ln.get("name") == line_name), None)
    if not line:
        raise HTTPException(404, "Line not found")

    nodes: List[Dict[str, Any]] = line.setdefault("nodes", [])
    node = next((nd for nd in nodes if int(nd.get("unit_id", -1)) == int(old_unit_id)), None)
    if not node:
        raise HTTPException(404, "Node not found")

    if "unit_id" in updates:
        new_uid = int(updates["unit_id"])
        if new_uid != int(old_unit_id) and any(int(nd.get("unit_id", -1)) == new_uid for nd in nodes):
            raise HTTPException(409, "Another node with this unit_id already exists")
        node["unit_id"] = new_uid

    if "object" in updates:
        node["object"] = updates["object"]

    if "num_object" in updates:
        node["num_object"] = updates["num_object"]

    backup = _write_cfg(cfg)
    return {"ok": True, "backup": backup}

@router.delete("/node/delete")
def delete_node(body: Dict[str, Any]):
    line_name = body.get("line")
    unit_id   = body.get("unit_id")
    if not line_name or unit_id is None:
        raise HTTPException(400, "Bad payload")

    cfg = _cfg()
    lines: List[Dict[str, Any]] = cfg.get("lines", [])
    line = next((ln for ln in lines if ln.get("name") == line_name), None)
    if not line:
        raise HTTPException(404, "Line not found")

    nodes: List[Dict[str, Any]] = line.get("nodes", [])
    idx = next((i for i, nd in enumerate(nodes) if int(nd.get("unit_id", -1)) == int(unit_id)), -1)
    if idx < 0:
        raise HTTPException(404, "Node not found")

    nodes.pop(idx)
    backup = _write_cfg(cfg)
    return {"ok": True, "backup": backup}


# ─────────────────────────────────────────────────────────────────────────────
# Параметры (добавить/изменить/удалить) — бережное обновление
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/param/add")
def add_param(payload: Dict[str, Any]):
    line_name = payload.get("line")
    unit_id   = payload.get("unit_id")
    object_   = payload.get("object")
    param     = payload.get("param")
    if not line_name or unit_id is None or not object_ or not isinstance(param, dict):
        raise HTTPException(400, "Bad payload")

    cfg = _cfg()
    lines: List[Dict[str, Any]] = cfg.setdefault("lines", [])
    line = next((ln for ln in lines if ln.get("name") == line_name), None)
    if not line:
        raise HTTPException(404, "Line not found")

    nodes: List[Dict[str, Any]] = line.setdefault("nodes", [])
    node  = next((nd for nd in nodes if int(nd.get("unit_id", -1)) == int(unit_id)), None)
    if not node:
        # создадим узел на лету, чтобы не падать
        node = {"unit_id": int(unit_id), "object": object_, "params": []}
        nodes.append(node)

    params: List[Dict[str, Any]] = node.setdefault("params", [])
    existing = next((p for p in params if p.get("name") == param.get("name")), None)
    if existing:
        params.remove(existing)
    params.append(param)

    backup = _write_cfg(cfg)
    return {"ok": True, "backup": backup}

@router.put("/param/update")
def update_param(body: Dict[str, Any]):
    line_name = body.get("line")
    unit_id   = body.get("unit_id")
    name      = body.get("name")
    updates   = body.get("updates")
    if not line_name or unit_id is None or not name or not isinstance(updates, dict):
        raise HTTPException(400, "Bad payload")

    cfg = _cfg()
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

    # аккуратно поддержим rename
    current = params[idx].copy()
    newp = current.copy()
    newp.update(updates or {})

    new_name = newp.get("name", name)
    if new_name != name and any(p.get("name") == new_name for p in params):
        raise HTTPException(409, "Param with new name already exists")

    params[idx] = newp
    backup = _write_cfg(cfg)
    return {"ok": True, "backup": backup}

@router.delete("/param/delete")
def delete_param(body: Dict[str, Any]):
    line_name = body.get("line")
    unit_id   = body.get("unit_id")
    name      = body.get("name")
    if not line_name or unit_id is None or not name:
        raise HTTPException(400, "Bad payload")

    cfg = _cfg()
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

@router.get("/params/export")
def export_params_xlsx():
    cfg = settings.get_cfg()
    wb = Workbook()
    ws = wb.active
    ws.title = "params"

    headers = [
        "Линия","Unit","Объект","Номер объекта",
        "Параметр","Тип","Адрес","Scale","Mode",
        "Publish","Interval, ms","Topic",
        "error_state","display_error_text","mqttROM"
    ]
    ws.append(headers)

    for ln in cfg.get("lines", []):
        line_name = ln.get("name","")
        for nd in ln.get("nodes", []):
            unit = nd.get("unit_id", "")
            obj  = nd.get("object", "")
            numo = nd.get("num_object", "")
            for p in nd.get("params", []):
                ws.append([
                    line_name,
                    unit,
                    obj,
                    numo,
                    p.get("name",""),
                    p.get("register_type",""),
                    p.get("address",""),
                    p.get("scale", 1.0),
                    p.get("mode","r"),
                    p.get("publish_mode","on_change"),
                    p.get("publish_interval_ms", 0),
                    p.get("topic") or "",
                    p.get("error_state", ""),
                    p.get("display_error_text", "") or "",
                    p.get("mqttROM", "") or "",
                ])

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="params.xlsx"'},
    )

@router.post("/params/import")
async def import_params_xlsx(file: UploadFile = File(...)):
    content = await file.read()
    try:
        wb = load_workbook(io.BytesIO(content), data_only=True)
    except Exception as e:
        raise HTTPException(400, f"XLSX parse error: {e}")

    ws = wb["params"] if "params" in wb.sheetnames else wb.active
    headers_row = [str(c.value or "").strip() for c in ws[1]]
    idx = {name: i for i, name in enumerate(headers_row)}

    required = ["Линия","Unit","Объект","Параметр","Тип","Адрес","Scale","Mode","Publish","Interval, ms","Topic"]
    missing = [h for h in required if h not in idx]
    if missing:
        raise HTTPException(400, f"В файле отсутствуют колонки: {', '.join(missing)}")

    cfg = settings.get_cfg()
    new_lines: Dict[str, Dict[str, Any]] = {}
    nodes_by_key: Dict[tuple, Dict[str, Any]] = {}
    total_params = 0

    for row in ws.iter_rows(min_row=2, values_only=True):
        if not any(row):
            continue
        line_name = str(row[idx["Линия"]] or "").strip()
        if not line_name:
            continue
        unit_id = int(row[idx["Unit"]] or 0)
        object_ = str(row[idx["Объект"]] or "").strip()
        name    = str(row[idx["Параметр"]] or "").strip()
        if not (object_ and name and unit_id):
            continue

        reg_type = str(row[idx["Тип"]] or "").strip()
        address  = int(row[idx["Адрес"]] or 0)
        scale    = float(row[idx["Scale"]] or 1.0)
        mode     = str(row[idx["Mode"]] or "r").strip()
        pmode    = str(row[idx["Publish"]] or "on_change").strip()
        pint     = int(row[idx["Interval, ms"]] or 0)
        topic    = str(row[idx["Topic"]] or "").strip() or None

        num_obj  = None
        if "Номер объекта" in idx:
            v = row[idx["Номер объекта"]]
            if v not in (None, ""):
                num_obj = int(v)

        error_state = None
        if "error_state" in idx:
            v = row[idx["error_state"]]
            if v not in (None, ""):
                error_state = int(v)

        display_error_text = None
        if "display_error_text" in idx:
            v = row[idx["display_error_text"]]
            if v not in (None, ""):
                display_error_text = str(v)

        mqttROM = None
        if "mqttROM" in idx:
            v = row[idx["mqttROM"]]
            if v not in (None, ""):
                mqttROM = str(v)

        line = new_lines.get(line_name)
        if not line:
            # сохраняем существующие портовые поля, если линия уже есть в cfg
            existing = next((ln for ln in cfg.get("lines", []) if ln.get("name")==line_name), None)
            if existing:
                line = {k: existing.get(k) for k in ("name","device","baudrate","timeout","parity","stopbits","port_retry_backoff_s","nodes")}
                line["name"] = line_name
                line["nodes"] = []
            else:
                line = {"name": line_name, "device": "", "baudrate": 9600, "timeout": 0.1, "parity": "N", "stopbits": 1, "nodes": []}
            new_lines[line_name] = line

        key = (line_name, unit_id, object_)
        node = nodes_by_key.get(key)
        if not node:
            node = {"unit_id": unit_id, "object": object_, "params": []}
            if num_obj is not None:
                node["num_object"] = num_obj
            nodes_by_key[key] = node
            line["nodes"].append(node)

        param = {
            "name": name,
            "register_type": reg_type,
            "address": address,
            "scale": scale,
            "mode": mode,
            "publish_mode": pmode,
            "publish_interval_ms": pint,
            "topic": topic
        }
        if error_state is not None:
            param["error_state"] = error_state
        if display_error_text is not None:
            param["display_error_text"] = display_error_text
        if mqttROM is not None:
            param["mqttROM"] = mqttROM

        node["params"].append(param)
        total_params += 1

    cfg["lines"] = list(new_lines.values())
    backup = _write_cfg(cfg)

    try:
        hot_reload_lines(cfg)
    except Exception as e:
        raise HTTPException(500, f"YAML сохранён (backup: {backup}), но перезапуск линий не удался: {e}")

    return {"ok": True, "backup": backup, "imported_params": total_params}


# ─────────────────────────────────────────────────────────────────────────────
# Файл на диск / перечитать / перезапустить линии
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/read_disk")
def read_disk():
    settings.load_yaml_config()
    return {"ok": True}

@router.post("/save_disk")
def save_disk():
    backup = _write_cfg(_cfg())
    return {"ok": True, "backup": backup}

@router.post("/reload")
def reload_lines():
    hot_reload_lines(settings.get_cfg())
    return {"ok": True}
