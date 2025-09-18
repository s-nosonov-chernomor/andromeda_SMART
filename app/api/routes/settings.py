# app/api/routes/settings.py
from __future__ import annotations

import os
import threading
from copy import deepcopy
from datetime import datetime
from typing import Optional, List, Dict, Any, Literal

import yaml
from fastapi import APIRouter, HTTPException, Body
from pydantic import BaseModel, Field, validator

from app.core.config import settings
from app.services.hot_reload import hot_reload_lines

router = APIRouter()
_CFG_LOCK = threading.Lock()

# ─────────────────────────────────────────────────────────────────────────────
# Допустимые значения перечислений
# ─────────────────────────────────────────────────────────────────────────────
REGISTER_TYPES = {"coil", "discrete", "holding", "input"}
PARAM_MODES = {"r", "rw"}
PUBLISH_MODES = {"on_change", "interval", "on_change_and_interval", "both"}

# ─────────────────────────────────────────────────────────────────────────────
# Pydantic-модели для запросов/ответов
# ─────────────────────────────────────────────────────────────────────────────
class MqttGeneral(BaseModel):
    host: str
    port: int
    base_topic: str = "/devices"
    qos: int = 0
    retain: bool = False
    client_id: Optional[str] = None

class PollingGeneral(BaseModel):
    interval_ms: int = 1000
    jitter_ms: int = 0
    max_errors_before_backoff: int = 5
    backoff_ms: int = 500
    port_retry_backoff_s: int = 5
    batch_read: Optional[Dict[str, Any]] = Field(default_factory=lambda: {
        "enabled": True,
        "max_bits": 1968,         # modbus max bits per request (some masters use 2000-8*something)
        "max_registers": 120      # safe chunk size
    })

class HistoryPolicy(BaseModel):
    max_rows: int = 50000
    ttl_days: int = 14
    cleanup_every: int = 500

class DebugGeneral(BaseModel):
    enabled: bool = False
    log_reads: bool = False
    summary_every_s: int = 0

class GeneralSettingsDTO(BaseModel):
    mqtt: MqttGeneral
    polling: PollingGeneral
    history: HistoryPolicy
    debug: DebugGeneral

class ParamDTO(BaseModel):
    # описание одной строки таблицы регистров
    name: str
    register_type: Literal["coil", "discrete", "holding", "input"]
    address: int
    scale: float = 1.0
    mode: Literal["r", "rw"] = "r"
    publish_mode: Literal["on_change", "interval", "on_change_and_interval", "both"] = "on_change"
    publish_interval_ms: int = 0
    topic: Optional[str] = None

    @validator("publish_interval_ms")
    def _nonnegative(cls, v):
        if v < 0:
            raise ValueError("publish_interval_ms must be >= 0")
        return v

class ParamAddDTO(BaseModel):
    line: str
    unit_id: int
    object: str
    param: ParamDTO

class ParamUpdateDTO(BaseModel):
    line: str
    unit_id: int
    name: str                                # текущее имя
    updates: ParamDTO                        # новые значения (можно менять name)

class ParamDeleteDTO(BaseModel):
    line: str
    unit_id: int
    name: str

# ─────────────────────────────────────────────────────────────────────────────
# Вспомогалки по работе с in-memory YAML
# ─────────────────────────────────────────────────────────────────────────────
def _ensure_sections(cfg: Dict[str, Any]) -> None:
    cfg.setdefault("mqtt", {})
    cfg.setdefault("db", {})
    cfg.setdefault("polling", {})
    cfg.setdefault("history", {})
    cfg.setdefault("debug", {})
    cfg.setdefault("lines", [])

def _load_from_disk() -> Dict[str, Any]:
    if not os.path.exists(settings.cfg_path):
        raise HTTPException(404, f"config not found: {settings.cfg_path}")
    with open(settings.cfg_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    _ensure_sections(data)
    return data

def _save_to_disk(cfg: Dict[str, Any]) -> str:
    # backup с таймштампом
    backup = f"{settings.cfg_path}.{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.bak"
    try:
        if os.path.exists(settings.cfg_path):
            os.replace(settings.cfg_path, backup)
        with open(settings.cfg_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(cfg, f, sort_keys=False, allow_unicode=True)
    except Exception as e:
        raise HTTPException(500, f"cannot write config: {e}")
    return os.path.basename(backup)

def _find_line(cfg: Dict[str, Any], line_name: str) -> Dict[str, Any]:
    for ln in cfg.get("lines", []):
        if ln.get("name") == line_name:
            return ln
    raise HTTPException(404, f"line not found: {line_name}")

def _find_or_create_node(line: Dict[str, Any], unit_id: int, object_name: Optional[str]) -> Dict[str, Any]:
    nodes = line.setdefault("nodes", [])
    for nd in nodes:
        if int(nd.get("unit_id", -1)) == int(unit_id):
            # обновим object, если дали новое валидное имя
            if object_name:
                nd["object"] = object_name
            nd.setdefault("params", [])
            return nd
    # создать
    if not object_name:
        raise HTTPException(400, "object is required to create new node")
    nd = {"unit_id": int(unit_id), "object": object_name, "params": []}
    nodes.append(nd)
    return nd

def _find_param(node: Dict[str, Any], name: str) -> Dict[str, Any]:
    for p in node.get("params", []):
        if p.get("name") == name:
            return p
    raise HTTPException(404, f"param not found: {name}")

def _validate_param_dict(d: Dict[str, Any]) -> None:
    if d.get("register_type") not in REGISTER_TYPES:
        raise HTTPException(400, f"register_type must be one of {sorted(REGISTER_TYPES)}")
    if d.get("mode") not in PARAM_MODES:
        raise HTTPException(400, f"mode must be one of {sorted(PARAM_MODES)}")
    pm = (d.get("publish_mode") or "on_change").lower()
    if pm not in PUBLISH_MODES:
        raise HTTPException(400, f"publish_mode must be one of {sorted(PUBLISH_MODES)}")
    try:
        int(d.get("address"))
    except Exception:
        raise HTTPException(400, "address must be integer")

# ─────────────────────────────────────────────────────────────────────────────
# enums — чтобы фронту заполнить выпадающие списки
# ─────────────────────────────────────────────────────────────────────────────
@router.get("/api/settings/enums")
def get_enums():
    return {
        "register_types": sorted(REGISTER_TYPES),
        "param_modes": sorted(PARAM_MODES),
        "publish_modes": sorted(PUBLISH_MODES),
        "parity": ["N", "E", "O"],          # для линий
        "stopbits": [1, 2]
    }

# ─────────────────────────────────────────────────────────────────────────────
# Общие поля (верх формы «Настройки»)
# ─────────────────────────────────────────────────────────────────────────────
@router.get("/api/settings/general", response_model=GeneralSettingsDTO)
def get_general():
    with _CFG_LOCK:
        cfg = settings.cfg
        _ensure_sections(cfg)
        gen = GeneralSettingsDTO(
            mqtt=MqttGeneral(
                host=cfg["mqtt"].get("host","127.0.0.1"),
                port=int(cfg["mqtt"].get("port",1883)),
                base_topic=cfg["mqtt"].get("base_topic","/devices"),
                qos=int(cfg["mqtt"].get("qos",0)),
                retain=bool(cfg["mqtt"].get("retain",False)),
                client_id=cfg["mqtt"].get("client_id")
            ),
            polling=PollingGeneral(
                interval_ms=int(cfg["polling"].get("interval_ms",1000)),
                jitter_ms=int(cfg["polling"].get("jitter_ms",0)),
                max_errors_before_backoff=int(cfg["polling"].get("max_errors_before_backoff",5)),
                backoff_ms=int(cfg["polling"].get("backoff_ms",500)),
                port_retry_backoff_s=int(cfg["polling"].get("port_retry_backoff_s",5)),
                batch_read=cfg["polling"].get("batch_read") or {"enabled": True, "max_bits": 1968, "max_registers": 120}
            ),
            history=HistoryPolicy(
                max_rows=int(cfg.get("history",{}).get("max_rows",50000)),
                ttl_days=int(cfg.get("history",{}).get("ttl_days",14)),
                cleanup_every=int(cfg.get("history",{}).get("cleanup_every",500)),
            ),
            debug=DebugGeneral(
                enabled=bool(cfg.get("debug",{}).get("enabled",False)),
                log_reads=bool(cfg.get("debug",{}).get("log_reads",False)),
                summary_every_s=int(cfg.get("debug",{}).get("summary_every_s",0)),
            )
        )
        return gen

@router.put("/api/settings/general")
def put_general(dto: GeneralSettingsDTO):
    with _CFG_LOCK:
        cfg = deepcopy(settings.cfg)
        _ensure_sections(cfg)

        # mqtt/db изменения сохраняем в файл, но применяются только после рестарта процесса
        cfg["mqtt"].update({
            "host": dto.mqtt.host,
            "port": int(dto.mqtt.port),
            "base_topic": dto.mqtt.base_topic,
            "qos": int(dto.mqtt.qos),
            "retain": bool(dto.mqtt.retain),
            "client_id": dto.mqtt.client_id
        })

        # polling применяем «горячо» (через hot_reload_lines)
        cfg["polling"].update({
            "interval_ms": int(dto.polling.interval_ms),
            "jitter_ms": int(dto.polling.jitter_ms),
            "max_errors_before_backoff": int(dto.polling.max_errors_before_backoff),
            "backoff_ms": int(dto.polling.backoff_ms),
            "port_retry_backoff_s": int(dto.polling.port_retry_backoff_s),
            "batch_read": dto.polling.batch_read or {"enabled": True, "max_bits": 1968, "max_registers": 120}
        })

        # history/debug — сразу в конфиг
        cfg["history"].update({
            "max_rows": int(dto.history.max_rows),
            "ttl_days": int(dto.history.ttl_days),
            "cleanup_every": int(dto.history.cleanup_every),
        })
        cfg["debug"].update({
            "enabled": bool(dto.debug.enabled),
            "log_reads": bool(dto.debug.log_reads),
            "summary_every_s": int(dto.debug.summary_every_s),
        })

        # сохранить в память
        settings.cfg = cfg
        # «горячо» перезапускаем линии с новыми polling
        try:
            hot_reload_lines(cfg)
        except Exception as e:
            raise HTTPException(500, f"reload failed: {e}")

        return {"ok": True, "note": "MQTT/DB изменения вступят в силу после рестарта сервиса."}

# ─────────────────────────────────────────────────────────────────────────────
# Линии/узлы/параметры (нижняя таблица)
# ─────────────────────────────────────────────────────────────────────────────
@router.get("/api/settings/lines")
def get_lines():
    with _CFG_LOCK:
        lines = settings.cfg.get("lines", [])
        # вернём то, что нужно для таблицы
        out = []
        for ln in lines:
            item = {
                "name": ln.get("name"),
                "device": ln.get("device"),
                "baudrate": ln.get("baudrate"),
                "timeout": ln.get("timeout"),
                "parity": ln.get("parity"),
                "stopbits": ln.get("stopbits"),
                "port_retry_backoff_s": ln.get("port_retry_backoff_s", settings.cfg.get("polling",{}).get("port_retry_backoff_s",5)),
                "nodes": []
            }
            for nd in ln.get("nodes", []):
                item["nodes"].append({
                    "unit_id": nd.get("unit_id"),
                    "object": nd.get("object"),
                    "params": nd.get("params", [])
                })
            out.append(item)
        return out

@router.post("/api/settings/param/add")
def add_param(dto: ParamAddDTO):
    with _CFG_LOCK:
        cfg = deepcopy(settings.cfg)
        ln = _find_line(cfg, dto.line)
        nd = _find_or_create_node(ln, dto.unit_id, dto.object)
        # проверка на дубликат имени в рамках node
        for p in nd.get("params", []):
            if p.get("name") == dto.param.name:
                raise HTTPException(400, f"param '{dto.param.name}' already exists in unit {dto.unit_id}")
        pdict = dto.param.dict()
        _validate_param_dict(pdict)
        nd.setdefault("params", []).append(pdict)
        settings.cfg = cfg
        # hot reload линий
        try:
            hot_reload_lines(cfg)
        except Exception as e:
            raise HTTPException(500, f"reload failed: {e}")
        return {"ok": True}

@router.put("/api/settings/param/update")
def update_param(dto: ParamUpdateDTO):
    with _CFG_LOCK:
        cfg = deepcopy(settings.cfg)
        ln = _find_line(cfg, dto.line)
        # node должен существовать
        nd = None
        for n in ln.get("nodes", []):
            if int(n.get("unit_id", -1)) == int(dto.unit_id):
                nd = n; break
        if nd is None:
            raise HTTPException(404, f"node {dto.unit_id} not found on line {dto.line}")
        p = _find_param(nd, dto.name)
        newp = dto.updates.dict()
        _validate_param_dict(newp)

        # если меняется имя — проверим, нет ли конфликта
        if newp["name"] != dto.name:
            for other in nd.get("params", []):
                if other is not p and other.get("name") == newp["name"]:
                    raise HTTPException(400, f"name '{newp['name']}' already exists")

        # применяем
        p.clear(); p.update(newp)
        settings.cfg = cfg
        try:
            hot_reload_lines(cfg)
        except Exception as e:
            raise HTTPException(500, f"reload failed: {e}")
        return {"ok": True}

@router.delete("/api/settings/param/delete")
def delete_param(dto: ParamDeleteDTO):
    with _CFG_LOCK:
        cfg = deepcopy(settings.cfg)
        ln = _find_line(cfg, dto.line)
        nd = None
        for n in ln.get("nodes", []):
            if int(n.get("unit_id", -1)) == int(dto.unit_id):
                nd = n; break
        if nd is None:
            raise HTTPException(404, f"node {dto.unit_id} not found on line {dto.line}")

        params = nd.get("params", [])
        idx = next((i for i,p in enumerate(params) if p.get("name")==dto.name), -1)
        if idx < 0:
            raise HTTPException(404, f"param not found: {dto.name}")
        params.pop(idx)

        settings.cfg = cfg
        try:
            hot_reload_lines(cfg)
        except Exception as e:
            raise HTTPException(500, f"reload failed: {e}")
        return {"ok": True}

# ─────────────────────────────────────────────────────────────────────────────
# Кнопки «Считать / Записать / Перезапустить линии»
# ─────────────────────────────────────────────────────────────────────────────
@router.post("/api/settings/read_disk")
def read_disk():
    with _CFG_LOCK:
        fresh = _load_from_disk()
        settings.cfg = fresh
        try:
            hot_reload_lines(fresh)
        except Exception as e:
            raise HTTPException(500, f"reload failed: {e}")
        return {"ok": True, "note": "Config reloaded from disk and lines hot-restarted."}

@router.post("/api/settings/save_disk")
def save_disk():
    with _CFG_LOCK:
        backup = _save_to_disk(settings.cfg)
        try:
            hot_reload_lines(settings.cfg)
        except Exception as e:
            raise HTTPException(500, f"saved, but reload failed: {e}")
        return {"ok": True, "backup": backup, "note": "Saved to disk. MQTT/DB changes require service restart."}

@router.post("/api/settings/reload")
def reload_lines():
    with _CFG_LOCK:
        try:
            hot_reload_lines(settings.cfg)
        except Exception as e:
            raise HTTPException(500, f"reload failed: {e}")
        return {"ok": True}
