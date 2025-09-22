# app/services/current_store.py
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Tuple, List, Any, Optional
import threading

@dataclass
class ParamState:
    object: str
    param: str
    line: str
    unit_id: int
    register_type: str
    address: int
    value: Optional[str] = None
    code: int = 0
    message: str = ""
    ts: Optional[datetime] = None  # время ПОСЛЕДНЕГО успешного ответа

Key = Tuple[str, int, str, str]  # (line, unit_id, object, param)

class CurrentStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._items: Dict[Key, ParamState] = {}

    # вызывать при запуске и после изменений YAML
    def reset_from_cfg(self, cfg: Dict[str, Any]) -> None:
        with self._lock:
            new_items: Dict[Key, ParamState] = {}
            for ln in cfg.get("lines", []):
                line = str(ln.get("name", ""))
                for nd in ln.get("nodes", []):
                    unit = int(nd.get("unit_id", 0))
                    obj  = str(nd.get("object", ""))
                    for p in nd.get("params", []):
                        name = str(p.get("name", ""))
                        if not (line and name and obj):
                            continue
                        key: Key = (line, unit, obj, name)
                        prev = self._items.get(key)
                        if prev:
                            # сохраняем предыдущее значение/время, но обновим тип/адрес если изменились
                            prev.register_type = str(p.get("register_type", prev.register_type))
                            prev.address = int(p.get("address", prev.address))
                            new_items[key] = prev
                        else:
                            new_items[key] = ParamState(
                                object=obj,
                                param=name,
                                line=line,
                                unit_id=unit,
                                register_type=str(p.get("register_type", "")),
                                address=int(p.get("address", 0)),
                            )
            # заменяем полностью: удалённые в YAML исчезают из «текущих»
            self._items = new_items

    # вызывать из MQTT-паблишера после успешной публикации значения/статуса
    def apply_publish(self, ctx: Dict[str, Any], payload: Dict[str, Any], ts: datetime) -> None:
        line = str(ctx.get("line", ""))
        unit = int(ctx.get("unit_id", 0))
        obj  = str(ctx.get("object", ""))
        name = str(ctx.get("param", ""))
        rtype = str(ctx.get("register_type", ""))
        addr  = int(ctx.get("address", 0))

        md = payload.get("metadata", {}) or {}
        sc = (md.get("status_code", {}) or {})
        code = int(sc.get("code", 0))
        message = str(sc.get("message", "OK"))

        value = payload.get("value", None)
        # В API возвращаем None (а не строку "null")
        if isinstance(value, str) and value.lower() == "null":
            value = None

        key: Key = (line, unit, obj, name)
        with self._lock:
            st = self._items.get(key)
            if not st:
                st = ParamState(
                    object=obj, param=name, line=line, unit_id=unit,
                    register_type=rtype, address=addr
                )
                self._items[key] = st
            st.value = value
            st.code = code
            st.message = message
            st.register_type = rtype or st.register_type
            st.address = addr or st.address
            # Обновляем «последний успех». Если код ≠ 0 и это неуспех — ts не трогаем.
            if code == 0:
                st.ts = ts

    def list(self) -> List[Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        with self._lock:
            out: List[Dict[str, Any]] = []
            for st in self._items.values():
                silent_for_s = None
                if st.ts is not None:
                    silent_for_s = int((now - st.ts).total_seconds())
                out.append({
                    "object": st.object,
                    "param": st.param,
                    "value": st.value,
                    "code": st.code,
                    "message": st.message,
                    "line": st.line,
                    "unit_id": st.unit_id,
                    "register_type": st.register_type,
                    "address": st.address,
                    "ts": st.ts.isoformat() if st.ts else None,
                    "silent_for_s": silent_for_s,
                })
            return out

current_store = CurrentStore()
