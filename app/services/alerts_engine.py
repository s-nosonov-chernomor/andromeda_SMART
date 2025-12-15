# app/services/alerts_engine.py
from __future__ import annotations

import os
import io
import json
import time
import threading
import logging
import requests
import paho.mqtt.client as mqtt


try:
    import certifi
    _CERT_BUNDLE = certifi.where()
except Exception:
    _CERT_BUNDLE = None

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Callable
from pathlib import Path
from datetime import datetime, timezone

import urllib.request
import urllib.parse

from app.core.config import settings


# ─────────────────────────────────────────────────────────────────────────────
# Конфиг (alerts.yaml) — лёгкая схема
# ─────────────────────────────────────────────────────────────────────────────

"""
Структура файла alerts.yaml:

flows:
  - id: "telegram-1"            # внутр. идентификатор (генерируется автоматически, строка)
    name: "Крылья — аварии"     # отображаемое имя потока
    type: "telegram"            # "telegram" | "ronet"
    enabled: true

    # telegram-канал
    telegram:
      bot_token: "123:AA..."
      chat_id: "-9999999"

    # ro.net-канал (заглушка, на будущее)
    ronet:
      endpoint: "https://api.ro.net/send"
      api_key: "secret"

    # параметры в потоке (можно переиспользовать один и тот же физ. параметр в разных потоках)
    params:
      - line: "line1"
        unit_id: 1
        name: "q1"                       # имя параметра в нашем YAML
        alias: "Протечка санузел"        # как будет называться в сообщении
        location: "ЖК Крылья/Корпус 1/Кв 3"
        nominal: 1                       # для bool — 0/1 (норма), для аналогов — число
        tolerance: 0                     # для bool игнор, для аналогов — дельта, в пределах которой считаем нормой
        ok_text: "норма"                 # текст состояния, когда «в норме»
        alarm_text: "АВАРИЯ"             # текст состояния, когда «авария»

    # Настройки по триггерам «События»
    events:
      group_window_s: 20                 # окно группировки (сек)
      include:                           # какие из params участвуют
        - { line: "line1", unit_id: 1, name: "q1" }
      exceptions:                        # исключения: не отправлять, если у параметра значение ровно == value
        - { line: "line1", unit_id: 1, name: "q1", value: 0 }

    # Настройки по триггерам «Интервальные оповещения»
    intervals:
      group_window_s: 60
      include:
        - { line: "line1", unit_id: 1, name: "q1" }
      exceptions: []
"""

def _alerts_path() -> Path:
    # можно прописать в settings.alerts_path; иначе — ./data/alerts.yaml
    p = Path(getattr(settings, "alerts_path", "./data/alerts.yaml")).resolve()
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


# ─────────────────────────────────────────────────────────────────────────────
# Модель в памяти
# ─────────────────────────────────────────────────────────────────────────────

ParamKey = Tuple[str, int, str]  # (line, unit_id, name)

@dataclass
class FlowParam:
    line: str
    unit_id: int
    name: str
    alias: str
    location: str
    nominal: Optional[float] = None   # bool: 0/1; analog: число
    tolerance: Optional[float] = None
    ok_text: str = "OK"
    alarm_text: str = "ALARM"

    tag: str = ""  # "A+0", "R-1" и т.п.

    def key(self) -> ParamKey:
        return (self.line, int(self.unit_id), self.name)


@dataclass
class FlowFilter:
    group_window_s: int = 30
    include: List[Dict[str, Any]] = field(default_factory=list)    # [{line,unit_id,name}]
    exceptions: List[Dict[str, Any]] = field(default_factory=list) # [{line,unit_id,name,value}]


@dataclass
class FlowCfg:
    id: str
    name: str
    type: str                      # "telegram" | "ronet"
    enabled: bool = True
    telegram: Dict[str, Any] = field(default_factory=dict)
    ronet: Dict[str, Any] = field(default_factory=dict)
    params: List[FlowParam] = field(default_factory=list)
    events: FlowFilter = field(default_factory=FlowFilter)
    intervals: FlowFilter = field(default_factory=FlowFilter)


# ─────────────────────────────────────────────────────────────────────────────
# Отправители
# ─────────────────────────────────────────────────────────────────────────────

class Sender:
    def send(self, flow: FlowCfg, text: str) -> None:
        raise NotImplementedError

class TelegramSender(Sender):
    def __init__(self, insecure_tls: bool = False, timeout: int = 10, trust_env: bool = False):
        self.insecure_tls = insecure_tls
        self.timeout = timeout
        self.trust_env = trust_env
        self.log = logging.getLogger("alerts.telemetry")

    def send(self, flow: FlowCfg, text: str) -> bool:
        tok = (flow.telegram or {}).get("bot_token") or ""
        chat = (flow.telegram or {}).get("chat_id") or ""
        tok = tok.strip()
        chat = str(chat).strip()
        if not tok or not chat:
            self.log.warning("telegram: empty token/chat (flow=%s)", flow.id)
            return False

        url = f"https://api.telegram.org/bot{tok}/sendMessage"
        payload = {
            "chat_id": chat,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }

        # Выбираем «verify»: сначала certifi (если есть), иначе системный.
        verify_arg = False if self.insecure_tls else (_CERT_BUNDLE or True)

        try:
            r = requests.post(
                url,
                json=payload,
                timeout=self.timeout,
                verify=verify_arg,
            )
            if r.status_code != 200:
                self.log.error("telegram HTTP %s: %s", r.status_code, r.text[:500])
                return False
            data = r.json()
            if not data.get("ok", False):
                self.log.error("telegram API error: %s", data)
                return False
            return True

        except requests.exceptions.SSLError as e:
            self.log.error("telegram SSL error: %s (insecure_tls=%s)", e, self.insecure_tls)
            return False
        except Exception as e:
            self.log.exception("telegram send failed: %s", e)
            return False

class RonetSender(Sender):
    """
    Отправляет уже сформированный JSON (строкой) во внешний MQTT брокер.
    """
    def __init__(self, timeout: int = 10):
        self.timeout = timeout
        self.log = logging.getLogger("alerts.ronet")
        self._lock = threading.RLock()
        self._clients: dict[str, mqtt.Client] = {}  # cache by (host|port|client_id|user)

    def _client_key(self, host: str, port: int, client_id: str, username: str) -> str:
        return f"{host}|{port}|{client_id}|{username}"

    def _get_or_create_client(self, host: str, port: int, client_id: str, username: str, password: str) -> mqtt.Client:
        key = self._client_key(host, port, client_id, username)
        with self._lock:
            c = self._clients.get(key)
            if c:
                return c

            c = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id=client_id,
                protocol=mqtt.MQTTv311,
            )
            if username:
                c.username_pw_set(username, password or "")

            # подключаемся синхронно (быстро) + loop_start
            try:
                c.connect(host, int(port), keepalive=30)
                c.loop_start()
            except Exception as e:
                self.log.error("ronet mqtt connect failed: %s", e)

            self._clients[key] = c
            return c

    def send_json(self, flow: FlowCfg, payload_json: str) -> bool:
        rn = flow.ronet or {}

        host = str(rn.get("broker_host", "") or "").strip()
        port = int(rn.get("broker_port", 1883) or 1883)
        topic = str(rn.get("topic", "") or "").strip()
        qos = int(rn.get("qos", 0) or 0)
        retain = bool(rn.get("retain", False))

        username = str(rn.get("username", "") or "")
        password = str(rn.get("password", "") or "")
        client_id = str(rn.get("client_id", "") or f"alerts-ronet-{flow.id}")

        if not host or not topic:
            self.log.warning("ronet: empty broker_host or topic (flow=%s)", flow.id)
            return False

        c = self._get_or_create_client(host, port, client_id, username, password)

        try:
            info = c.publish(topic, payload_json, qos=qos, retain=retain)
            # дождёмся результата публикации (коротко)
            info.wait_for_publish(timeout=self.timeout)
            return True
        except Exception as e:
            self.log.error("ronet mqtt publish failed: %s", e)
            return False



# ─────────────────────────────────────────────────────────────────────────────
# Утилиты форматирования сообщений
# ─────────────────────────────────────────────────────────────────────────────

# --- helpers: key "line|unit_id|name" <-> dict ---
def _parse_key_str(key: str) -> tuple[str, int, str]:
    parts = (key or "").split("|", 2)
    if len(parts) != 3:
        raise ValueError(f"bad key format: {key!r}")
    line, uid, name = parts
    return str(line), int(uid), str(name)

def _key_to_str(line: str, unit_id: int, name: str) -> str:
    return f"{line}|{int(unit_id)}|{name}"

def _html_escape(s: str) -> str:
    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def _nice_pct(value: Optional[float], nominal: Optional[float]) -> Optional[int]:
    try:
        if value is None or nominal in (None, 0):
            return None
        pct = round((float(value) / float(nominal)) * 100)
        if pct < 0: pct = 0
        if pct > 999: pct = 999
        return int(pct)
    except Exception:
        return None

def _build_tree(items: List[Tuple[List[str], str]]) -> Dict[str, Any]:
    """
    items: [ (["ЖК Крылья", "Корпус 1", "Кв 3"], "Квартира 3 - протечка") , ...]
    Возвращает древо { "ЖК Крылья": { "Корпус 1": { "Кв 3": ["...","..."] } } }
    """
    root: Dict[str, Any] = {}
    for path, leaf in items:
        cur = root
        for part in path:
            cur = cur.setdefault(part, {})
        cur.setdefault("_items", []).append(leaf)
    return root

def _render_tree_html(tree: Dict[str, Any], level: int = 0) -> str:
    parts: List[str] = []
    indent = " " * (level * 2)
    for key in sorted([k for k in tree.keys() if k != "_items"]):
        title = _html_escape(key)
        parts.append(f"{indent}💡️ <b>{title}</b>")
        parts.append(_render_tree_html(tree[key], level + 1))
    for leaf in tree.get("_items", []):
        parts.append(f"{indent}• {leaf}")
    return "\n".join(parts)

def _format_message(flow_name: str, block_title: str, lines: List[Tuple[List[str], str]]) -> str:
    """
    block_title: "События" | "Интервальные оповещения"
    lines: [ (["ЖК","Корпус","Кв"], "Квартира 3 — текст") , ... ]
    """
    head = f"🍅️ <b>{_html_escape(flow_name)}</b>\n<b>{_html_escape(block_title)}</b>\n"
    tree = _build_tree(lines)
    body = _render_tree_html(tree)
    return head + "\n" + body

def _build_ronet_payload(flow: FlowCfg, items: list[_CollectedItem]) -> dict:
    """
    Преобразовать список собранных значений в JSON, который ждёт RoNet/UM SMART.
    """
    from datetime import timezone, timedelta

    if not items:
        return {}

    # Берём последнее время из bucket
    last_ts = max((it.ts for it in items if isinstance(it.ts, datetime)), default=datetime.now(timezone.utc))

    # tz_offset_minutes из options.ronet (если есть)
    tz_offset_min = 0
    try:
        tz_offset_min = int((flow.ronet or {}).get("tz_offset_minutes", 0))
    except Exception:
        tz_offset_min = 0
    tz = timezone(timedelta(minutes=tz_offset_min))
    ts_local = last_ts.astimezone(tz)
    ts_str = ts_local.isoformat()

    # Готовим карту tag -> val по последнему значению
    last_by_param: dict[ParamKey, Any] = {}
    for it in items:
        last_by_param[it.key] = it.value

    tags_arr: list[dict[str, Any]] = []
    for p in flow.params:
        # Нужен осмысленный tag
        if not p.tag:
            continue
        key = p.key()
        if key not in last_by_param:
            continue
        val = last_by_param[key]
        try:
            # В RoNet часто числа — float
            val = float(val)
        except Exception:
            pass
        tags_arr.append({"tag": p.tag, "val": val})

    if not tags_arr:
        return {}

    r_opts = flow.ronet or {}
    um_name   = r_opts.get("um_name", "UM SMART")
    um_serial = r_opts.get("um_serial", "")
    um_fw     = r_opts.get("um_fw", "")
    measure   = r_opts.get("measure", "aMonth")

    device_id      = int(r_opts.get("device_id", 1) or 1)
    meter          = int(r_opts.get("meter", device_id) or device_id)
    device_serial  = r_opts.get("device_serial", "")
    device_model   = r_opts.get("device_model", "")
    device_type    = int(r_opts.get("device_type", 0) or 0)

    payload: dict[str, Any] = {
        "name": um_name,
        "serial": um_serial,
        "fw": um_fw,
        "measures": [
            {
                "measure": measure,
                "devices": [
                    {
                        "id": device_id,
                        "meter": meter,
                        "serial": device_serial,
                        "model": device_model,
                        "type": device_type,
                        "vals": [
                            {
                                "tags": tags_arr,
                                "ts": ts_str,
                                "diff": 0,
                            }
                        ],
                    }
                ],
            }
        ],
    }
    return payload

# ─────────────────────────────────────────────────────────────────────────────
# Алгоритм: окна группировки и сборка сообщений
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class _CollectedItem:
    key: ParamKey
    value: Any
    ts: datetime

@dataclass
class _Bucket:
    window_s: int
    items: List[_CollectedItem] = field(default_factory=list)
    timer: Optional[threading.Timer] = None

class _FlowRuntime:
    def __init__(self, flow: FlowCfg, sender: Sender, on_flush: Optional[Callable[[dict], None]] = None):
        self.flow = flow
        self.sender = sender
        self._on_flush = on_flush
        self.lock = threading.RLock()
        self.events = _Bucket(window_s=max(1, int(flow.events.group_window_s or 30)))
        self.intervals = _Bucket(window_s=max(1, int(flow.intervals.group_window_s or 60)))
        self.last_seen_ts: Optional[float] = None

    def _ensure_timer(self, bucket: _Bucket, flush_fn: Callable[[], None]) -> None:
        if bucket.timer and bucket.timer.is_alive():
            return
        bucket.timer = threading.Timer(bucket.window_s, flush_fn)
        bucket.timer.daemon = True
        bucket.timer.start()

    def _in_include(self, key: ParamKey, mode: str) -> bool:
        flt = self.flow.events if mode == "event" else self.flow.intervals
        for it in flt.include or []:
            if it.get("line") == key[0] and int(it.get("unit_id", -9999)) == key[1] and it.get("name") == key[2]:
                return True
        return False

    def _in_exceptions(self, key: ParamKey, value: Any, mode: str) -> bool:
        flt = self.flow.events if mode == "event" else self.flow.intervals
        for ex in flt.exceptions or []:
            if ex.get("line") == key[0] and int(ex.get("unit_id", -9999)) == key[1] and ex.get("name") == key[2]:
                if "value" in ex and ex["value"] == value:
                    return True
        return False

    def _param_cfg(self, key: ParamKey) -> Optional[FlowParam]:
        for p in self.flow.params:
            if p.key() == key:
                return p
        return None

    def _append(self, mode: str, item: _CollectedItem) -> None:
        bucket = self.events if mode == "event" else self.intervals
        with self.lock:
            bucket.items.append(item)
            self._ensure_timer(bucket, lambda: self._flush(mode))

    def on_publish(self, key: ParamKey, value: Any, pub_kind: str, ts: datetime) -> None:
        self.last_seen_ts = time.time()
        if not self.flow.enabled:
            return
        mode = "interval" if pub_kind == "interval" else "event"
        if not self._in_include(key, mode):
            return
        if self._in_exceptions(key, value, mode):
            return
        self._append(mode, _CollectedItem(key=key, value=value, ts=ts))

    # ── сборка и отправка ────────────────────────────────────────────────────
    def _flush(self, mode: str) -> None:
        bucket = self.events if mode == "event" else self.intervals
        with self.lock:
            items = bucket.items
            bucket.items = []
            bucket.timer = None

        if not items:
            return

        # --- ВЕТКА ДЛЯ RONET ------------------------------------------------
        if self.flow.type == "ronet":
            payload = _build_ronet_payload(self.flow, items)
            if not payload:
                return

            rn = (self.flow.ronet or {})
            dest = {
                "broker_host": rn.get("broker_host", ""),
                "broker_port": rn.get("broker_port", 1883),
                "topic": rn.get("topic", ""),
            }

            payload_json = json.dumps(payload, ensure_ascii=False)
            preview = payload_json[:4000]

            if self._on_flush:
                try:
                    self._on_flush({
                        "ts": time.time(),
                        "flow_id": self.flow.id,
                        "flow_name": self.flow.name,
                        "mode": mode,
                        "count": len(items),
                        "dest": dest,
                        "preview": preview,
                    })
                except Exception:
                    pass

            ok = False
            try:
                # ВАЖНО: ronet отправляет JSON-строку через send_json
                if isinstance(self.sender, RonetSender):
                    ok = self.sender.send_json(self.flow, payload_json)
                else:
                    ok = False
            except Exception:
                ok = False

            try:
                from app.services.alerts_engine import alerts_engine as _eng_singleton
                _eng_singleton._sent({
                    "ts": time.time(),
                    "flow_id": self.flow.id,
                    "flow_name": self.flow.name,
                    "mode": "События" if mode == "event" else "Интервалы",
                    "count": len(items),
                    "dest": dest,
                    "preview": preview[:1000],
                    "ok": bool(ok),
                })
                if not ok:
                    _eng_singleton._err(f"ronet send failed for flow={self.flow.id}")
            except Exception:
                pass

            return  # на этом всё, дальше телеграм-ветку НЕ выполняем

        # сгруппировать по location и подготовить строки
        lines: List[Tuple[List[str], str]] = []
        for it in items:
            p = self._param_cfg(it.key)
            if not p:
                # параметр удалили — пропустим
                continue

            # вычисляем ok/alarm
            is_bool = (p.nominal in (0, 1)) and (p.tolerance in (None, 0))
            ok = False
            if is_bool:
                try:
                    ok = int(it.value) == int(p.nominal or 0)
                except Exception:
                    ok = False
            else:
                try:
                    val = float(it.value)
                    nom = float(p.nominal or 0)
                    tol = float(p.tolerance or 0)
                    ok = abs(val - nom) <= tol
                except Exception:
                    ok = False

            pct = None if is_bool else _nice_pct(float(it.value) if it.value is not None else None, float(p.nominal or 0))
            state_text = (p.ok_text if ok else p.alarm_text)
            if not is_bool and pct is not None and not ok:
                state_text = f"{state_text} ({pct}%)"

            alias = p.alias or p.name
            leaf = f"<b>{_html_escape(alias)}</b> — {_html_escape(state_text)}"
            loc_path = [seg.strip() for seg in (p.location or "").split("/") if seg.strip()]
            if not loc_path:
                loc_path = ["Объект"]

            lines.append((loc_path, leaf))

        if not lines:
            return

        block_title = "События" if mode == "event" else "Интервальные оповещения"
        text = _format_message(self.flow.name, block_title, lines)

        dest = {}
        if self.flow.type == "telegram":
            dest = {"telegram_chat_id": (self.flow.telegram or {}).get("chat_id")}
        elif self.flow.type == "ronet":
            dest = {"ronet_endpoint": (self.flow.ronet or {}).get("endpoint")}

        if self._on_flush:
            try:
                self._on_flush({
                    "ts": time.time(),
                    "flow_id": self.flow.id,
                    "flow_name": self.flow.name,
                    "mode": mode,  # "event" | "interval"
                    "count": len(lines),
                    "dest": dest,
                    "preview": text[:4000],  # целиком безопасно, но ограничим
                })
            except Exception:
                pass

        ok = False
        try:
            ok = self.sender.send(self.flow, text)
        except Exception as e:
            ok = False

        # лог в движок (через синглтон — он уже импортируется так же, как alerts_engine)
        try:
            from app.services.alerts_engine import alerts_engine as _eng_singleton
            _eng_singleton._sent({
                "ts": time.time(),
                "flow_id": self.flow.id,
                "flow_name": self.flow.name,
                "mode": "События" if mode == "event" else "Интервалы",
                "count": len(items),
                "dest": {"telegram_chat_id": (self.flow.telegram or {}).get("chat_id", "")},
                "preview": text[:1000],
                "ok": bool(ok),
            })
            if not ok:
                _eng_singleton._err(f"send failed for flow={self.flow.id}")
        except Exception:
            pass
class _LogsFlowRuntime:
    """
    Runtime для потоков типа automation_logs.
    Здесь нет окон группировки: каждый лог → отдельное сообщение.
    """

    def __init__(
        self,
        flow: FlowCfg,
        sender: Sender,
        on_flush: Optional[Callable[[dict], None]] = None,
    ):
        self.flow = flow
        self.sender = sender
        self._on_flush = on_flush
        self.lock = threading.RLock()
        self.last_seen_ts: Optional[float] = None

    def _param_cfg(self, source_param: str) -> List[FlowParam]:
        """
        Могут быть несколько параметров с одним и тем же source_param (key),
        поэтому возвращаем список.
        Для automation_logs мы кладём source_param в p.name.
        """
        res: List[FlowParam] = []
        for p in self.flow.params:
            if p.name == source_param:
                res.append(p)
        return res

    def on_log(
        self,
        *,
        source_param: str,
        level: str,
        msg: str,
        ts: datetime,
        ctx: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.last_seen_ts = time.time()
        if not self.flow.enabled:
            return
        if not source_param:
            return

        params = self._param_cfg(source_param)
        if not params:
            # лог пришёл, но для такого source_param поток не настроен
            return

        lines: List[Tuple[List[str], str]] = []

        # перевод уровней в человекочитаемый русский
        lvl = (level or "").upper()
        lvl_ru = {
            "DEBUG": "отладка",
            "INFO": "информация",
            "WARNING": "предупреждение",
            "WARN": "предупреждение",
            "ERROR": "ошибка",
            "CRITICAL": "авария",
        }.get(lvl, lvl.lower() or "сообщение")

        # ts приводим к локальному и красиво форматируем
        try:
            ts_local = ts.astimezone()
        except Exception:
            ts_local = datetime.now().astimezone()
        ts_txt = ts_local.strftime("%d.%m.%Y %H:%M:%S")

        for p in params:
            alias = p.alias or source_param
            loc_path = [
                seg.strip()
                for seg in (p.location or "").split("/")
                if seg.strip()
            ]
            if not loc_path:
                loc_path = ["Объект"]

            leaf = (
                f"<b>{_html_escape(alias)}</b> — "
                f"[{ts_txt}] {lvl_ru}: {_html_escape(msg or '')}"
            )

            lines.append((loc_path, leaf))

        if not lines:
            return

        text = _format_message(self.flow.name, "Логи автоматики", lines)

        dest: Dict[str, Any] = {}
        if self.flow.type == "automation_logs":
            # по сути это телеграм-канал, оставляем тот же формат, что и у telegram
            dest = {"telegram_chat_id": (self.flow.telegram or {}).get("chat_id")}

        # для /api/alerts/outbox
        if self._on_flush:
            try:
                self._on_flush(
                    {
                        "ts": time.time(),
                        "flow_id": self.flow.id,
                        "flow_name": self.flow.name,
                        "mode": "log",
                        "count": len(lines),
                        "dest": dest,
                        "preview": text[:4000],
                    }
                )
            except Exception:
                pass

        ok = False
        try:
            ok = self.sender.send(self.flow, text)
        except Exception:
            ok = False

        # логируем факт отправки во внутренний sent_log, как _FlowRuntime._flush
        try:
            from app.services.alerts_engine import alerts_engine as _eng_singleton

            _eng_singleton._sent(
                {
                    "ts": time.time(),
                    "flow_id": self.flow.id,
                    "flow_name": self.flow.name,
                    "mode": "Логи автоматики",
                    "count": len(lines),
                    "dest": dest,
                    "preview": text[:1000],
                    "ok": bool(ok),
                }
            )
            if not ok:
                _eng_singleton._err(
                    f"send failed for automation_logs flow={self.flow.id}"
                )
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Менеджер оповещений (singleton)
# ─────────────────────────────────────────────────────────────────────────────

class AlertsEngine:
    def __init__(self):
        self._lock = threading.RLock()
        self._flows: List[FlowCfg] = []

        from typing import Union
        self._rt: Dict[str, Union[_FlowRuntime, _LogsFlowRuntime]] = {}

        self._last_errors: list[dict[str, Any]] = []
        self._last_flushes: list[dict[str, Any]] = []
        self._sent_log: list[dict[str, Any]] = []  # последние отправки/попытки

    # ── публичные методы для интеграции ──────────────────────────────────────
    def notify_automation_log(
        self,
        *,
        source_param: str,
        level: str,
        msg: str,
        ts: Optional[datetime] = None,
        ctx: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Вызвать при получении LOG-действия автоматики.

        source_param — то, что ты задаёшь в extra["source_param"] (и выбираешь в UI).
        """
        if not source_param:
            return
        t = ts or datetime.now(timezone.utc)

        with self._lock:
            for fr in self._rt.values():
                # интересуют только потоки типа automation_logs
                if not isinstance(fr, _LogsFlowRuntime):
                    continue
                fr.on_log(
                    source_param=source_param,
                    level=level,
                    msg=msg,
                    ts=t,
                    ctx=ctx or {},
                )


    def _on_flush(self, info: dict) -> None:
        self._last_flushes.append(info)
        if len(self._last_flushes) > 100:
            self._last_flushes = self._last_flushes[-100:]

    def get_last_flushes(self, limit: int = 20) -> list[dict]:
        return list(self._last_flushes[-max(0, int(limit)):])

    def _sent(self, item: dict[str, Any]):
        # item: {ts, flow_id, flow_name, mode, count, dest, preview, ok, err?}
        self._sent_log.append(item)
        if len(self._sent_log) > 200:
            self._sent_log = self._sent_log[-200:]

    def _err(self, msg: str):
        self._last_errors.append({"ts": time.time(), "msg": msg})
        if len(self._last_errors) > 100:
            self._last_errors = self._last_errors[-100:]
        try:
            print("[alerts] ERROR:", msg)
        except Exception:
            pass

    def _info(self, msg: str):
        try:
            print("[alerts]", msg)
        except Exception:
            pass

    def start(self) -> None:
        self.reload_config()

    def stop(self) -> None:
        with self._lock:
            for rt in self._rt.values():
                if isinstance(rt, _FlowRuntime):
                    try:
                        if rt.events.timer: rt.events.timer.cancel()
                        if rt.intervals.timer: rt.intervals.timer.cancel()
                    except Exception:
                        pass
            self._rt.clear()

    def reload_config(self) -> None:
        cfg = self._load_yaml()
        flows = self._parse_cfg(cfg)
        with self._lock:
            self._flows = flows
            # пересоздать рантаймы
            self._rt.clear()
            for f in flows:
                alerts_sec = getattr(settings, "alerts", {}) or {}
                insecure = bool(alerts_sec.get("insecure_tls", False))
                timeout_s = int(alerts_sec.get("http_timeout_s", 10) or 10)

                if f.type in ("telegram", "automation_logs"):
                    sender = TelegramSender(
                        insecure_tls=insecure,
                        timeout=timeout_s,
                    )
                else:
                    sender = RonetSender()

                if f.type == "automation_logs":
                    self._rt[f.id] = _LogsFlowRuntime(f, sender, self._on_flush)
                else:
                    self._rt[f.id] = _FlowRuntime(f, sender, self._on_flush)


    def notify_publish(self, *, line: str, unit_id: int, name: str,
                       value: Any, pub_kind: str, ts: Optional[datetime] = None) -> None:
        key = (line, int(unit_id), name)
        t = ts or datetime.now(timezone.utc)

        with self._lock:
            for fr in self._rt.values():
                try:
                    fn = getattr(fr, "on_publish", None)
                    if callable(fn):
                        fn(key, value, pub_kind, t)
                except Exception as e:
                    self._err(f"notify_publish runtime failed: {e}")

    # ── хелперы для API/UI ───────────────────────────────────────────────────

    def dump_config(self) -> Dict[str, Any]:
        """Вернуть текущий alerts.yaml в нормализованном виде (для API/UI)."""
        with self._lock:
            return self._serialize_cfg(self._flows)

    def save_config(self, data: Dict[str, Any]) -> str:
        """Сохранить (с бэкапом/ротацией). Вернёт имя backup-файла (или '')."""
        # простая проверка корректности
        flows = self._parse_cfg(data)
        # записываем
        backup = self._backup_and_write_yaml(data)
        # активируем
        with self._lock:
            self._flows = flows
            self._rt.clear()
            for f in flows:
                alerts_sec = getattr(settings, "alerts", {}) or {}
                insecure = bool(alerts_sec.get("insecure_tls", False))
                timeout_s = int(alerts_sec.get("http_timeout_s", 10) or 10)

                if f.type in ("telegram", "automation_logs"):
                    sender = TelegramSender(
                        insecure_tls=insecure,
                        timeout=timeout_s,
                    )
                else:
                    sender = RonetSender()

                if f.type == "automation_logs":
                    self._rt[f.id] = _LogsFlowRuntime(f, sender, self._on_flush)
                else:
                    self._rt[f.id] = _FlowRuntime(f, sender, self._on_flush)

        return backup

    def list_known_params_from_main_cfg(self) -> List[Dict[str, Any]]:
        """
        Перечислить параметры из основного YAML (для выпадающего списка «Имя»).
        Ключи: line, unit_id, name, object
        """
        cfg = settings.get_cfg() or {}
        out: List[Dict[str, Any]] = []
        for ln in cfg.get("lines", []) or []:
            line = ln.get("name", "")
            for nd in ln.get("nodes", []) or []:
                uid = int(nd.get("unit_id", 0) or 0)
                obj = nd.get("object", "")
                for p in nd.get("params", []) or []:
                    out.append({
                        "line": line, "unit_id": uid,
                        "name": p.get("name", ""),
                        "object": obj
                    })
        # Можно отсортировать по объекту/имени
        out.sort(key=lambda x: (x["line"], x["unit_id"], x["object"], x["name"]))
        return out

    # ── низкоуровневые: YAML I/O ────────────────────────────────────────────

    def _load_yaml(self) -> Dict[str, Any]:
        import yaml
        path = _alerts_path()
        if not path.exists() or path.stat().st_size == 0:
            return {"flows": []}
        try:
            with path.open("r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            if not isinstance(data, dict):
                return {"flows": []}
            return data
        except Exception:
            return {"flows": []}

    def _backup_and_write_yaml(self, data: Dict[str, Any]) -> str:
        import yaml, shutil
        # каталоги и политика берём из секции backups основного YAML
        main_cfg = settings.get_cfg() or {}
        bsec = (main_cfg.get("backups") or {})
        backups_dir = Path(bsec.get("dir", settings.backups_dir)).resolve()
        backups_keep = int(bsec.get("keep", settings.backups_keep) or 0)
        backups_dir.mkdir(parents=True, exist_ok=True)

        target = _alerts_path()
        target.parent.mkdir(parents=True, exist_ok=True)

        backup_name = ""
        if target.exists():
            ts = time.strftime("%Y%m%d-%H%M%S")
            backup_name = f"{target.stem}-{ts}{target.suffix}.bak"
            try:
                shutil.copy2(target, backups_dir / backup_name)
            except Exception:
                backup_name = ""

            if backups_keep > 0:
                patt = f"{target.stem}-*{target.suffix}.bak"
                files = sorted(backups_dir.glob(patt))
                extra = len(files) - backups_keep
                if extra > 0:
                    for old in files[:extra]:
                        try:
                            old.unlink()
                        except Exception:
                            pass

        tmp = target.with_suffix(target.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)
        tmp.replace(target)
        return backup_name

    # app/services/alerts_engine.py

    def diag(self) -> Dict[str, Any]:
        with self._lock:
            flows = []
            for f in self._flows:
                rt = self._rt.get(f.id)

                if isinstance(rt, _FlowRuntime):
                    runtime_info = {
                        "events_buffer": len(rt.events.items),
                        "intervals_buffer": len(rt.intervals.items),
                        "last_seen": getattr(rt, "last_seen_ts", None),
                    }
                elif isinstance(rt, _LogsFlowRuntime):
                    runtime_info = {
                        "events_buffer": None,
                        "intervals_buffer": None,
                        "last_seen": getattr(rt, "last_seen_ts", None),
                    }
                else:
                    runtime_info = {
                        "events_buffer": None,
                        "intervals_buffer": None,
                        "last_seen": None,
                    }

                flows.append({
                    "id": f.id,
                    "name": f.name,
                    "type": f.type,
                    "enabled": bool(f.enabled),
                    "params": len(f.params or []),
                    "events_selected": len((f.events.include or [])),
                    "intervals_selected": len((f.intervals.include or [])),
                    "runtime": runtime_info,
                    "telegram": {
                        "has_token": bool((f.telegram or {}).get("bot_token")),
                        "has_chat_id": bool((f.telegram or {}).get("chat_id")),
                        "chat_id": str((f.telegram or {}).get("chat_id", ""))[:6] + "…"
                        if (f.telegram or {}).get("chat_id")
                        else "",
                    } if f.type in ("telegram", "automation_logs") else None,
                })

            return {
                "running": True,
                "flows": flows,
                "last_errors": self._last_errors[-10:],
            }

    def send_test_telegram(self, flow_id: str, text: str) -> tuple[bool, str]:
        import urllib.request, urllib.parse
        with self._lock:
            f = next((x for x in self._flows if x.id == flow_id), None)
        if not f:
            return False, f"flow {flow_id} not found"
        if f.type != "telegram":
            return False, f"flow {flow_id} is not telegram"

        tok = (f.telegram or {}).get("bot_token") or ""
        chat = (f.telegram or {}).get("chat_id") or ""
        if not tok:
            return False, "bot_token is empty"
        if not chat:
            return False, "chat_id is empty"

        url = f"https://api.telegram.org/bot{tok}/sendMessage"
        payload = {
            "chat_id": chat,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        data = urllib.parse.urlencode(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Content-Type", "application/x-www-form-urlencoded")

        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                resp = r.read()
            return True, "sent"
        except Exception as e:
            self._err(f"send_test_telegram failed: {e}")
            return False, f"{e.__class__.__name__}: {e}"

    # ── сериализация/валидация ──────────────────────────────────────────────

    def _parse_cfg(self, raw: Dict[str, Any]) -> List[FlowCfg]:
        flows: List[FlowCfg] = []
        for i, it in enumerate((raw or {}).get("flows", []) or []):
            fid = str(it.get("id") or f"flow-{i + 1}")
            name = str(it.get("name") or fid)

            typ = str(it.get("type") or "telegram").lower()
            if typ not in ("telegram", "ronet", "automation_logs"):
                typ = "telegram"

            enabled = bool(it.get("enabled", True))

            # options.telegram|ronet (новый фронт) + fallback на топ-левел telegram/ronet
            opts = it.get("options", {}) or {}
            tele1 = (opts.get("telegram") or {}) if isinstance(opts, dict) else {}
            tele2 = it.get("telegram") or {}
            telegram_opts: Dict[str, Any] = {}
            for k in ("bot_token", "chat_id"):
                v = None
                v1 = tele1.get(k) if isinstance(tele1, dict) else None
                v2 = tele2.get(k) if isinstance(tele2, dict) else None
                # выбираем первый непустой (не None и не пустая строка)
                v = (v1 if (v1 is not None and str(v1).strip() != "") else
                     (v2 if (v2 is not None and str(v2).strip() != "") else None))
                if v is not None:
                    telegram_opts[k] = str(v)

            rn1 = (opts.get("ronet") or {}) if isinstance(opts, dict) else {}
            rn2 = it.get("ronet") or {}
            ronet_opts: Dict[str, Any] = {}

            RONET_KEYS = (
                # MQTT destination
                "broker_host", "broker_port",
                "username", "password",
                "client_id",
                "topic", "qos", "retain",

                # UM / device meta
                "um_name", "um_serial", "um_fw",
                "measure",
                "device_id", "meter",
                "device_serial", "device_model", "device_type",
                "tz_offset_minutes",
            )

            for k in RONET_KEYS:
                v1 = rn1.get(k) if isinstance(rn1, dict) else None
                v2 = rn2.get(k) if isinstance(rn2, dict) else None
                v = (v1 if (v1 is not None and str(v1).strip() != "") else
                     (v2 if (v2 is not None and str(v2).strip() != "") else None))
                if v is not None:
                    ronet_opts[k] = v


            # params: для обычных потоков ожидаем key в формате line|unit_id|name,
            # для automation_logs — key == source_param (просто строка).
            params: List[FlowParam] = []
            for p in it.get("params", []) or []:
                try:
                    if typ == "automation_logs":
                        key_str = str(p.get("key") or "").strip()
                        if not key_str:
                            continue
                        params.append(
                            FlowParam(
                                # "фиктивные" line/unit_id, т.к. здесь они не используются
                                line="logs",
                                unit_id=0,
                                name=key_str,  # здесь лежит именно source_param
                                alias=str(p.get("alias") or key_str),
                                location=str(p.get("path") or p.get("location", "")),
                                nominal=None,
                                tolerance=None,
                                ok_text="OK",
                                alarm_text="ALARM",
                            )
                        )
                    else:
                        if "key" in p:
                            line, uid, pname = _parse_key_str(
                                str(p.get("key") or "||")
                            )
                            params.append(
                                FlowParam(
                                    line=line,
                                    unit_id=int(uid),
                                    name=pname,
                                    alias=str(p.get("alias") or pname),
                                    location=str(p.get("path") or ""),
                                    nominal=(
                                        None
                                        if p.get("nominal", None) is None
                                        else float(p.get("nominal"))
                                    ),
                                    tolerance=(
                                        None
                                        if p.get("tolerance", None) is None
                                        else float(p.get("tolerance"))
                                    ),
                                    ok_text=str(p.get("ok_text", "OK")),
                                    alarm_text=str(p.get("alarm_text", "ALARM")),
                                    tag=str(p.get("tag") or ""),
                                )
                            )
                        else:
                            params.append(
                                FlowParam(
                                    line=str(p.get("line", "")),
                                    unit_id=int(p.get("unit_id", 0)),
                                    name=str(p.get("name", "")),
                                    alias=str(p.get("alias") or p.get("name", "")),
                                    location=str(
                                        p.get("location", "") or p.get("path", "")
                                    ),
                                    nominal=(
                                        None
                                        if p.get("nominal", None) is None
                                        else float(p.get("nominal"))
                                    ),
                                    tolerance=(
                                        None
                                        if p.get("tolerance", None) is None
                                        else float(p.get("tolerance"))
                                    ),
                                    ok_text=str(p.get("ok_text", "OK")),
                                    alarm_text=str(p.get("alarm_text", "ALARM")),
                                    tag=str(p.get("tag") or ""),
                                )
                            )
                except Exception:
                    # пропускаем битый элемент
                    continue


            # sections: events / intervals
            def _mk_filter(d: Dict[str, Any]) -> FlowFilter:
                group_s = int((d or {}).get("group_window_s", 30))

                include: List[Dict[str, Any]] = []
                # новый фронт: selected = ["line|uid|name"]
                sel = (d or {}).get("selected", None)
                if isinstance(sel, list):
                    for s in sel:
                        try:
                            line, uid, pname = _parse_key_str(str(s))
                            include.append({"line": line, "unit_id": int(uid), "name": pname})
                        except Exception:
                            pass
                else:
                    # старый формат: include = [{line,unit_id,name}]
                    include = list((d or {}).get("include", []) or [])

                exceptions: List[Dict[str, Any]] = []
                exc = (d or {}).get("exceptions", []) or []
                for ex in exc:
                    if isinstance(ex, dict) and "key" in ex:
                        # новый фронт: { key, value }
                        try:
                            line, uid, pname = _parse_key_str(str(ex.get("key")))
                            exceptions.append(
                                {"line": line, "unit_id": int(uid), "name": pname, "value": ex.get("value")})
                        except Exception:
                            pass
                    else:
                        # старый формат уже dict с line/unit_id/name/value
                        exceptions.append(ex)

                return FlowFilter(group_window_s=group_s, include=include, exceptions=exceptions)

            flows.append(FlowCfg(
                id=fid, name=name, type=typ, enabled=enabled,
                telegram=telegram_opts,
                ronet=ronet_opts,
                params=params,
                events=_mk_filter(it.get("events", {})),
                intervals=_mk_filter(it.get("intervals", {})),
            ))

        return flows

    def _serialize_cfg(self, flows: List[FlowCfg]) -> Dict[str, Any]:
        out: Dict[str, Any] = {"flows": []}
        for f in flows:
            # selected: из include -> список строк-ключей
            def _mk_selected(flt: FlowFilter) -> List[str]:
                keys = []
                for it in (flt.include or []):
                    try:
                        keys.append(
                            _key_to_str(str(it.get("line", "")), int(it.get("unit_id", 0)), str(it.get("name", ""))))
                    except Exception:
                        pass
                return keys

            # exceptions: из {line,unit_id,name,value} -> {key,value}
            def _mk_exceptions(flt: FlowFilter) -> List[Dict[str, Any]]:
                res = []
                for ex in (flt.exceptions or []):
                    try:
                        k = _key_to_str(str(ex.get("line", "")), int(ex.get("unit_id", 0)), str(ex.get("name", "")))
                        res.append({"key": k, "value": ex.get("value")})
                    except Exception:
                        pass
                return res

            out["flows"].append({
                "id": f.id,
                "name": f.name,
                "type": f.type,
                "enabled": bool(f.enabled),
                "options": {
                    "telegram": f.telegram or {},
                    "ronet": f.ronet or {},
                },
                "params": [
                    (
                        {
                            # для automation_logs key == source_param
                            "key": p.name,
                            "alias": p.alias,
                            "path": p.location,
                        }
                        if f.type == "automation_logs"
                        else {
                            "key": _key_to_str(p.line, p.unit_id, p.name),
                            "alias": p.alias,
                            "path": p.location,
                            "nominal": p.nominal,
                            "tolerance": p.tolerance,
                            "ok_text": p.ok_text,
                            "alarm_text": p.alarm_text,
                            "tag": p.tag,
                        }
                    )
                    for p in f.params
                ],
                "events": {
                    "group_window_s": f.events.group_window_s,
                    "selected": _mk_selected(f.events),
                    "exceptions": _mk_exceptions(f.events),
                },
                "intervals": {
                    "group_window_s": f.intervals.group_window_s,
                    "selected": _mk_selected(f.intervals),
                    "exceptions": _mk_exceptions(f.intervals),
                }
            })
        return out


# ─────────────────────────────────────────────────────────────────────────────
# Singleton
# ─────────────────────────────────────────────────────────────────────────────

alerts_engine = AlertsEngine()
