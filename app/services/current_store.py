# app/services/current_store.py
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
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

    value: Optional[Any] = None
    code: int = 0
    message: str = ""

    # Время последнего УДАЧНОГО опроса (то, что ты хочешь как «Время опроса»)
    last_ok_ts: Optional[datetime] = None
    # Время ПОСЛЕДНЕЙ публикации (любая публикация: по изменению/интервалу/ошибке)
    last_pub_ts: Optional[datetime] = None

    # Доп. атрибуты для UI
    trigger: Optional[str] = None          # "change" | "interval" | None
    no_reply: int = 0                      # счётчик подряд неответов

Key = Tuple[str, int, str, str]  # (line, unit_id, object, param)

class CurrentStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._items: Dict[Key, ParamState] = {}

    def reset_from_cfg(self, cfg: Dict[str, Any]) -> None:
        with self._lock:
            new_items: Dict[Key, ParamState] = {}
            for ln in cfg.get("lines", []) or []:
                line = str(ln.get("name", ""))
                for nd in ln.get("nodes", []) or []:
                    unit = int(nd.get("unit_id", 0))
                    obj  = str(nd.get("object", ""))
                    for p in nd.get("params", []) or []:
                        name = str(p.get("name", ""))
                        if not (line and name and obj):
                            continue
                        key: Key = (line, unit, obj, name)
                        prev = self._items.get(key)
                        if prev:
                            # бережно переносим старые времена/значения
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
            self._items = new_items

    def apply_publish(self, ctx: Dict[str, Any], payload: Dict[str, Any], ts: datetime) -> None:
        """
        Вызывай из MQTT-бриджа сразу после успешной публикации.
        Мы читаем:
          - metadata.status_code.{code,message}
          - metadata.silent_for_s  (сколько секунд прошло с последнего УСПЕШНОГО ответа)
          - metadata.trigger       ("change"/"interval")
          - metadata.no_reply      (счётчик подряд неответов)
        """
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

        trigger = md.get("trigger") or None
        silent_for_s = md.get("silent_for_s", None)
        try:
            silent_for_s = int(silent_for_s) if silent_for_s is not None else None
        except Exception:
            silent_for_s = None
        no_reply = int(md.get("no_reply", 0) or 0)

        value = payload.get("value", None)

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

            # Всегда фиксируем время публикации
            st.last_pub_ts = ts
            st.trigger = trigger
            st.no_reply = no_reply

            # Время ПОСЛЕДНЕГО успешного опроса:
            #  - при code == 0 — это текущая публикация;
            #  - при ошибке — если есть silent_for_s, восстановим last_ok_ts = ts - silent_for_s;
            if code == 0:
                st.last_ok_ts = ts
            elif silent_for_s is not None:
                st.last_ok_ts = ts - timedelta(seconds=int(silent_for_s))
            # если silent_for_s нет — оставляем как было

    # вызывать из линии при УСПЕШНОМ чтении, если публикации нет/ещё рано
    def touch_read(self, ctx: Dict[str, Any], ts: datetime) -> None:
        line = str(ctx.get("line", ""))
        unit = int(ctx.get("unit_id", 0))
        obj  = str(ctx.get("object", ""))
        name = str(ctx.get("param", ""))
        rtype = str(ctx.get("register_type", ""))
        addr  = int(ctx.get("address", 0))
        key: Key = (line, unit, obj, name)
        with self._lock:
            st = self._items.get(key)
            if not st:
                st = ParamState(object=obj, param=name, line=line, unit_id=unit,
                                register_type=rtype, address=addr)
                self._items[key] = st
            st.last_ok_ts = ts
            if rtype: st.register_type = rtype
            if addr:  st.address = addr

    def apply_read(self, ctx: Dict[str, Any], value: Any, ts: datetime) -> None:
        """
        Обновить «текущие» по факту УСПЕШНОГО чтения с линии,
        даже если публикации в брокер не было.
        Не трогаем last_pub_ts, trigger и т.п.
        """
        line = str(ctx.get("line", ""))
        unit = int(ctx.get("unit_id", 0))
        obj  = str(ctx.get("object", ""))
        name = str(ctx.get("param", ""))
        rtype = str(ctx.get("register_type", ""))
        addr  = int(ctx.get("address", 0))

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
            st.code = 0
            st.message = "OK"
            if rtype: st.register_type = rtype
            if addr:  st.address = addr
            st.last_ok_ts = ts
            # last_pub_ts НЕ трогаем

    def list(self) -> List[Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        with self._lock:
            out: List[Dict[str, Any]] = []
            for st in self._items.values():
                since_ok = None
                since_pub = None
                if st.last_ok_ts is not None:
                    since_ok = max(0, int((now - st.last_ok_ts).total_seconds()))
                if st.last_pub_ts is not None:
                    since_pub = max(0, int((now - st.last_pub_ts).total_seconds()))

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

                    # новое
                    "last_ok_ts": st.last_ok_ts.isoformat() if st.last_ok_ts else None,
                    "last_pub_ts": st.last_pub_ts.isoformat() if st.last_pub_ts else None,
                    "since_last_ok_s": since_ok,
                    "since_last_pub_s": since_pub,
                    "trigger": st.trigger,
                    "no_reply": st.no_reply,

                    # для обратной совместимости со старым фронтом:
                    "ts": st.last_ok_ts.isoformat() if st.last_ok_ts else None,
                    "silent_for_s": since_ok,
                })
            return out

current_store = CurrentStore()
