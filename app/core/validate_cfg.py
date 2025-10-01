# app/core/validate_cfg.py
from __future__ import annotations
from typing import Dict, Any, List, Optional

ALLOWED_REGISTER_TYPES = {"coil", "discrete", "holding", "input"}
ALLOWED_PARAM_MODES    = {"r", "rw"}
ALLOWED_PUBLISH_MODES  = {"on_change", "interval", "on_change_and_interval"}

# multi-register / analog helpers
ALLOWED_DATA_TYPES   = {"u16", "s16", "u32", "s32", "f32"}
ALLOWED_WORD_ORDERS  = {"AB", "BA"}  # порядок 16-битных слов для 32-битных типов

def _as_int(v, name, min_: Optional[int] = None, max_: Optional[int] = None) -> int:
    try:
        iv = int(v)
    except Exception:
        raise ValueError(f"{name}: ожидается целое, получено {v!r}")
    if min_ is not None and iv < min_:
        raise ValueError(f"{name}: должно быть ≥ {min_} (получено {iv})")
    if max_ is not None and iv > max_:
        raise ValueError(f"{name}: должно быть ≤ {max_} (получено {iv})")
    return iv

def _as_float(v, name, min_: Optional[float] = None) -> float:
    try:
        fv = float(v)
    except Exception:
        raise ValueError(f"{name}: ожидается число, получено {v!r}")
    if min_ is not None and fv < min_:
        raise ValueError(f"{name}: должно быть ≥ {min_} (получено {fv})")
    return fv

def _as_bool(v, name) -> bool:
    if isinstance(v, bool):
        return v
    # допускаем 'true'/'false'/1/0 из yaml
    if isinstance(v, (int, float)) and v in (0, 1):
        return bool(v)
    if isinstance(v, str) and v.lower() in ("true", "false"):
        return v.lower() == "true"
    raise ValueError(f"{name}: должен быть true/false")

def validate_cfg(cfg: Dict[str, Any]) -> None:
    """Бросает ValueError с понятным текстом, если конфиг некорректен."""
    if not isinstance(cfg, dict):
        raise ValueError("корневой YAML должен быть объектом")

    # ─── mqtt ───
    mqtt = cfg.get("mqtt", {})
    if not isinstance(mqtt, dict):
        raise ValueError("mqtt: должен быть объектом")
    host = str(mqtt.get("host", "")).strip()
    if not host:
        raise ValueError("mqtt.host: не должен быть пустым")
    _as_int(mqtt.get("port", 1883), "mqtt.port", 1, 65535)
    _as_int(mqtt.get("qos", 0), "mqtt.qos", 0, 2)
    # retain/client_id допускаем как есть

    # ─── db ───
    db = cfg.get("db", {})
    if not isinstance(db, dict):
        raise ValueError("db: должен быть объектом")
    url = str(db.get("url", "")).strip()
    if not url:
        raise ValueError("db.url: обязателен (например sqlite:///./data/data.db)")

    # ─── serial ───
    serial = cfg.get("serial", {})
    if serial and not isinstance(serial, dict):
        raise ValueError("serial: должен быть объектом")
    if "echo" in serial:
        _as_bool(serial["echo"], "serial.echo")

    # ─── addressing ───
    adr = cfg.get("addressing", {})
    if adr and not isinstance(adr, dict):
        raise ValueError("addressing: должен быть объектом")
    if "normalize" in adr:
        _as_bool(adr["normalize"], "addressing.normalize")

    # ─── backups ───
    bkp = cfg.get("backups", {})
    if bkp:
        if not isinstance(bkp, dict):
            raise ValueError("backups: должен быть объектом")
        if "dir" in bkp and not isinstance(bkp["dir"], str):
            raise ValueError("backups.dir: должен быть строкой")
        if "keep" in bkp:
            _as_int(bkp["keep"], "backups.keep", 0)

    # ─── service ───
    svc = cfg.get("service", {})
    if svc:
        if not isinstance(svc, dict):
            raise ValueError("service: должен быть объектом")
        if "unit" in svc and not isinstance(svc["unit"], str):
            raise ValueError("service.unit: должен быть строкой")
        if "restart_cmd" in svc and not (svc["restart_cmd"] is None or isinstance(svc["restart_cmd"], str)):
            raise ValueError("service.restart_cmd: должен быть строкой или null")

    # ─── andromeda ───
    andr = cfg.get("andromeda", {})
    if andr:
        if not isinstance(andr, dict):
            raise ValueError("andromeda: должен быть объектом")
        if "restart_cmd" in andr and not (andr["restart_cmd"] is None or isinstance(andr["restart_cmd"], str)):
            raise ValueError("andromeda.restart_cmd: должен быть строкой или null")

    # ─── history ───
    hist = cfg.get("history", {})
    if not isinstance(hist, dict):
        raise ValueError("history: должен быть объектом")
    _as_int(hist.get("max_rows", 0), "history.max_rows", 0)
    _as_int(hist.get("ttl_days", 0), "history.ttl_days", 0)
    _as_int(hist.get("cleanup_every", 500), "history.cleanup_every", 1)

    # ─── current ───
    cur = cfg.get("current", {})
    if cur:
        if not isinstance(cur, dict):
            raise ValueError("current: должен быть объектом")
        if "touch_read_every_s" in cur:
            _as_int(cur["touch_read_every_s"], "current.touch_read_every_s", 0)

    # ─── polling ───
    pol = cfg.get("polling", {})
    if not isinstance(pol, dict):
        raise ValueError("polling: должен быть объектом")
    _as_int(pol.get("interval_ms", 1000), "polling.interval_ms", 1)
    _as_int(pol.get("jitter_ms", 0), "polling.jitter_ms", 0)
    _as_int(pol.get("backoff_ms", 0), "polling.backoff_ms", 0)
    _as_int(pol.get("max_errors_before_backoff", 0), "polling.max_errors_before_backoff", 0)
    if "port_retry_backoff_s" in pol:
        _as_int(pol.get("port_retry_backoff_s", 0), "polling.port_retry_backoff_s", 0)

    br = pol.get("batch_read", {})
    if br:
        if not isinstance(br, dict):
            raise ValueError("polling.batch_read: должен быть объектом")
        if "enabled" in br:
            _as_bool(br["enabled"], "polling.batch_read.enabled")
        _as_int(br.get("max_bits", 1), "polling.batch_read.max_bits", 1)
        _as_int(br.get("max_registers", 1), "polling.batch_read.max_registers", 1)

    # ─── debug ───
    dbg = cfg.get("debug", {})
    if not isinstance(dbg, dict):
        raise ValueError("debug: должен быть объектом")
    _as_int(dbg.get("summary_every_s", 0), "debug.summary_every_s", 0)
    if "enabled" in dbg:
        _as_bool(dbg["enabled"], "debug.enabled")
    if "log_reads" in dbg:
        _as_bool(dbg["log_reads"], "debug.log_reads")

    # ─── lines/nodes/params ───
    lines = cfg.get("lines", [])
    if not isinstance(lines, list):
        raise ValueError("lines: должен быть массивом")

    seen_line_names: set[str] = set()

    for i, ln in enumerate(lines, start=1):
        if not isinstance(ln, dict):
            raise ValueError(f"lines[{i}]: должен быть объектом")
        name = str(ln.get("name", "")).strip()
        if not name:
            raise ValueError(f"lines[{i}].name: обязателен")
        if name in seen_line_names:
            raise ValueError(f"lines: имя линии '{name}' дублируется")
        seen_line_names.add(name)

        # базовые поля линии
        str(ln.get("device", ""))  # может быть пусто, заполняется позже через UI
        _as_int(ln.get("baudrate", 9600), f"lines[{name}].baudrate", 1)
        _as_float(ln.get("timeout", 0.1), f"lines[{name}].timeout", 0.0)
        if "parity" in ln and ln["parity"] not in (None, "N", "E", "O"):
            raise ValueError(f"lines[{name}].parity: допустимо N/E/O")
        if "stopbits" in ln:
            _as_int(ln.get("stopbits", 1), f"lines[{name}].stopbits", 1, 2)
        if "port_retry_backoff_s" in ln:
            _as_int(ln.get("port_retry_backoff_s", 0), f"lines[{name}].port_retry_backoff_s", 0)
        if "rs485_rts_toggle" in ln:
            _as_bool(ln["rs485_rts_toggle"], f"lines[{name}].rs485_rts_toggle")

        nodes = ln.get("nodes", [])
        if not isinstance(nodes, list):
            raise ValueError(f"lines[{name}].nodes: должен быть массивом")

        seen_units: set[int] = set()
        for nd in nodes:
            if not isinstance(nd, dict):
                raise ValueError(f"lines[{name}].nodes[]: каждый узел — объект")
            unit_id = _as_int(nd.get("unit_id", -1), f"lines[{name}].nodes[].unit_id", 0, 247)
            obj = str(nd.get("object", "")).strip()
            if not obj:
                raise ValueError(f"lines[{name}].nodes[unit {unit_id}].object: обязателен")
            # допускаем повтор unit_id на одной линии при осознанной конфигурации
            seen_units.add(unit_id)

            if "num_object" in nd and nd["num_object"] is not None:
                _as_int(nd["num_object"], f"lines[{name}].nodes[unit {unit_id}].num_object", 0)

            params = nd.get("params", [])
            if not isinstance(params, list):
                raise ValueError(f"lines[{name}].nodes[unit {unit_id}].params: должен быть массивом")

            seen_param_names: set[str] = set()
            for p in params:
                if not isinstance(p, dict):
                    raise ValueError(f"lines[{name}].nodes[unit {unit_id}].params[]: каждый параметр — объект")

                pname = str(p.get("name", "")).strip()
                if not pname:
                    raise ValueError(f"param.name (line '{name}', unit {unit_id}): обязателен")
                if pname in seen_param_names:
                    raise ValueError(f"параметр '{pname}' (line '{name}', unit {unit_id}) дублируется")
                seen_param_names.add(pname)

                rt = str(p.get("register_type", "")).strip()
                if rt not in ALLOWED_REGISTER_TYPES:
                    raise ValueError(f"{name}/{unit_id}/{pname}: register_type должен быть {ALLOWED_REGISTER_TYPES}")
                _as_int(p.get("address", 0), f"{name}/{unit_id}/{pname}: address", 0)
                _as_float(p.get("scale", 1.0), f"{name}/{unit_id}/{pname}: scale", 0.000001)

                md = str(p.get("mode", "r")).strip()
                if md not in ALLOWED_PARAM_MODES:
                    raise ValueError(f"{name}/{unit_id}/{pname}: mode должен быть {ALLOWED_PARAM_MODES}")

                pm = str(p.get("publish_mode", "on_change")).strip()
                if pm not in ALLOWED_PUBLISH_MODES:
                    raise ValueError(f"{name}/{unit_id}/{pname}: publish_mode должен быть {ALLOWED_PUBLISH_MODES}")
                _as_int(p.get("publish_interval_ms", 0), f"{name}/{unit_id}/{pname}: publish_interval_ms", 0)

                # error/mqttROM/text — как были
                if "error_state" in p and p["error_state"] is not None:
                    _as_int(p["error_state"], f"{name}/{unit_id}/{pname}: error_state", 0, 1)
                if "display_error_text" in p and p["display_error_text"] is not None:
                    if not isinstance(p["display_error_text"], str):
                        raise ValueError(f"{name}/{unit_id}/{pname}: display_error_text должен быть строкой")
                if "mqttROM" in p and p["mqttROM"] is not None:
                    if not isinstance(p["mqttROM"], str):
                        raise ValueError(f"{name}/{unit_id}/{pname}: mqttROM должен быть строкой")

                # ─── NEW: multi-register поля ───
                words = int(p.get("words", 1) or 1)
                if words < 1:
                    raise ValueError(f"{name}/{unit_id}/{pname}: words должно быть ≥ 1")
                dtype = str(p.get("data_type", "u16") or "u16").strip()
                if dtype not in ALLOWED_DATA_TYPES:
                    raise ValueError(f"{name}/{unit_id}/{pname}: data_type должен быть {ALLOWED_DATA_TYPES}")
                worder = str(p.get("word_order", "AB") or "AB").strip()
                if words == 1:
                    # для 16-битных значений порядок слов не влияет; но если задан — ограничим
                    if worder not in ALLOWED_WORD_ORDERS:
                        raise ValueError(f"{name}/{unit_id}/{pname}: word_order должен быть {ALLOWED_WORD_ORDERS}")
                else:
                    # сейчас поддерживаем только 2-словные типы (32 бита)
                    if words != 2:
                        raise ValueError(f"{name}/{unit_id}/{pname}: words={words} не поддерживается (ожидалось 1 или 2)")
                    if worder not in ALLOWED_WORD_ORDERS:
                        raise ValueError(f"{name}/{unit_id}/{pname}: word_order должен быть {ALLOWED_WORD_ORDERS}")
                    if dtype in {"u16", "s16"}:
                        raise ValueError(f"{name}/{unit_id}/{pname}: data_type={dtype} конфликтует с words=2")
                    # для u32/s32/f32 words=2 — ок

                # для coil/discrete разрешим только words=1
                if rt in {"coil", "discrete"} and words != 1:
                    raise ValueError(f"{name}/{unit_id}/{pname}: для {rt} допустимы только words=1")

                # ─── NEW: аналоговые пороги ───
                if "step" in p and p["step"] is not None:
                    _as_float(p["step"], f"{name}/{unit_id}/{pname}: step", 0.0)
                if "hysteresis" in p and p["hysteresis"] is not None:
                    _as_float(p["hysteresis"], f"{name}/{unit_id}/{pname}: hysteresis", 0.0)
