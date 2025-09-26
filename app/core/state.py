# app/core/state.py
import time
from typing import Dict, Any, Optional

# key -> запись по параметру
# поля: value, code, message, last_ok_ts, last_attempt_ts, last_pub_ts, no_reply, topic, line, unit, register_type, address, trigger
_current: Dict[str, dict] = {}

def make_key(line: str, unit_id: int, param: str) -> str:
    return f"{line}:{unit_id}:{param}"

def update(
    line: str,
    unit_id: int,
    node_object: str,
    param: str,
    topic: str,
    register_type: str,
    address: int,
    value: Any,
    code: int,
    message: str,
    last_ok_ts: float,
    last_attempt_ts: float,
    *,
    last_pub_ts: Optional[float] = None,
    no_reply: Optional[int] = None,
    trigger: Optional[str] = None,  # "change" | "interval" | None
) -> None:
    key = make_key(line, unit_id, param)
    rec = _current.get(key, {})

    # базовые поля (перезатираем каждый раз)
    rec.update({
        "line": line,
        "unit": unit_id,          # оставим оба ключа на всякий случай
        "unit_id": unit_id,
        "object": node_object,
        "param": param,
        "topic": topic,
        "register_type": register_type,
        "address": address,
        "value": value,
        "code": int(code),
        "message": str(message),
        "last_ok_ts": float(last_ok_ts or 0.0),
        "last_attempt_ts": float(last_attempt_ts or 0.0),
    })

    # дополнительные поля — пишем только если пришли (чтобы не терять прежние значения)
    if last_pub_ts is not None:
        rec["last_pub_ts"] = float(last_pub_ts)
    else:
        rec.setdefault("last_pub_ts", 0.0)

    if no_reply is not None:
        rec["no_reply"] = int(no_reply)
    else:
        rec.setdefault("no_reply", 0)

    if trigger is not None:
        rec["trigger"] = trigger

    _current[key] = rec

def all_current():
    return list(_current.values())
