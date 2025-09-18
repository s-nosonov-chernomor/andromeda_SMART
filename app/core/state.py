# app/core/state.py
import time
from typing import Dict, Any

_current: Dict[str, dict] = {}  # key -> {value, code, message, last_ok_ts, last_attempt_ts, topic, line, unit, reg}

def make_key(line: str, unit_id: int, param: str) -> str:
    return f"{line}:{unit_id}:{param}"

def update(line, unit_id, node_object, param, topic, register_type, address, value, code, message, last_ok_ts, last_attempt_ts):
    _current[make_key(line, unit_id, param)] = {
        "line": line, "unit_id": unit_id, "object": node_object, "param": param,
        "topic": topic, "register_type": register_type, "address": address,
        "value": value, "code": code, "message": message,
        "last_ok_ts": last_ok_ts, "last_attempt_ts": last_attempt_ts,
    }

def all_current():
    return list(_current.values())
