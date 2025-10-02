# app/services/alerts_engine.py
from __future__ import annotations

import os
import io
import json
import time
import threading
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
    def send(self, flow: FlowCfg, text: str) -> None:
        tok = (flow.telegram or {}).get("bot_token") or ""
        chat = (flow.telegram or {}).get("chat_id") or ""
        if not tok or not chat:
            return
        # Используем простой urllib, без внешних зависимостей.
        url = f"https://api.telegram.org/bot{tok}/sendMessage"
        payload = {
            "chat_id": chat,
            "text": text,
            "parse_mode": "HTML",  # проще экранировать, чем MarkdownV2
            "disable_web_page_preview": True,
        }
        data = urllib.parse.urlencode(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                r.read()  # игнорируем ответ
        except Exception:
            # Не роняем процесс — логгер UI/сервисов поймает при необходимости
            pass

class RonetSender(Sender):
    def send(self, flow: FlowCfg, text: str) -> None:
        # Заглушка: предполагаем HTTP endpoint + api_key.
        rn = flow.ronet or {}
        endpoint = rn.get("endpoint")
        api_key  = rn.get("api_key")
        if not endpoint:
            return
        payload = json.dumps({"text": text}).encode("utf-8")
        req = urllib.request.Request(endpoint, data=payload, method="POST")
        req.add_header("Content-Type", "application/json")
        if api_key:
            req.add_header("Authorization", f"Bearer {api_key}")
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                r.read()
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Утилиты форматирования сообщений
# ─────────────────────────────────────────────────────────────────────────────

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
        parts.append(f"{indent}🏷️ <b>{title}</b>")
        parts.append(_render_tree_html(tree[key], level + 1))
    for leaf in tree.get("_items", []):
        parts.append(f"{indent}• {leaf}")
    return "\n".join(parts)

def _format_message(flow_name: str, block_title: str, lines: List[Tuple[List[str], str]]) -> str:
    """
    block_title: "События" | "Интервальные оповещения"
    lines: [ (["ЖК","Корпус","Кв"], "Квартира 3 — текст") , ... ]
    """
    head = f"🛰️ <b>{_html_escape(flow_name)}</b>\n<b>{_html_escape(block_title)}</b>\n"
    tree = _build_tree(lines)
    body = _render_tree_html(tree)
    return head + "\n" + body


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
    def __init__(self, flow: FlowCfg, sender: Sender):
        self.flow = flow
        self.sender = sender
        self.lock = threading.RLock()
        self.events = _Bucket(window_s=max(1, int(flow.events.group_window_s or 30)))
        self.intervals = _Bucket(window_s=max(1, int(flow.intervals.group_window_s or 60)))

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
        try:
            self.sender.send(self.flow, text)
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Менеджер оповещений (singleton)
# ─────────────────────────────────────────────────────────────────────────────

class AlertsEngine:
    def __init__(self):
        self._lock = threading.RLock()
        self._flows: List[FlowCfg] = []
        self._rt: Dict[str, _FlowRuntime] = {}  # id -> runtime

    # ── публичные методы для интеграции ──────────────────────────────────────

    def start(self) -> None:
        self.reload_config()

    def stop(self) -> None:
        with self._lock:
            for f in self._rt.values():
                try:
                    if f.events.timer: f.events.timer.cancel()
                    if f.intervals.timer: f.intervals.timer.cancel()
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
                sender = TelegramSender() if f.type == "telegram" else RonetSender()
                self._rt[f.id] = _FlowRuntime(f, sender)

    def notify_publish(self, *, line: str, unit_id: int, name: str,
                       value: Any, pub_kind: str, ts: Optional[datetime] = None) -> None:
        """
        Вызвать это при каждой публикации параметра.
        pub_kind: "event" | "interval" | "on_change" (будет трактоваться как "event")
        ts: время публикации (UTC). Если None — возьмём now(UTC).

        РЕКОМЕНДАЦИЯ: звать из mqtt_bridge._publisher_loop сразу после обновления current_store.
        """
        key = (line, int(unit_id), name)
        t = ts or datetime.now(timezone.utc)
        with self._lock:
            for fr in self._rt.values():
                fr.on_publish(key, value, pub_kind, t)

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
                sender = TelegramSender() if f.type == "telegram" else RonetSender()
                self._rt[f.id] = _FlowRuntime(f, sender)
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

    # ── сериализация/валидация ──────────────────────────────────────────────

    def _parse_cfg(self, raw: Dict[str, Any]) -> List[FlowCfg]:
        flows: List[FlowCfg] = []
        for i, it in enumerate((raw or {}).get("flows", []) or []):
            fid = str(it.get("id") or f"flow-{i+1}")
            name = str(it.get("name") or fid)
            typ  = str(it.get("type") or "telegram").lower()
            if typ not in ("telegram", "ronet"):
                typ = "telegram"
            enabled = bool(it.get("enabled", True))

            params: List[FlowParam] = []
            for p in it.get("params", []) or []:
                try:
                    params.append(FlowParam(
                        line=str(p.get("line","")),
                        unit_id=int(p.get("unit_id", 0)),
                        name=str(p.get("name","")),
                        alias=str(p.get("alias") or p.get("name","")),
                        location=str(p.get("location","")),
                        nominal=(None if p.get("nominal", None) is None else float(p.get("nominal"))),
                        tolerance=(None if p.get("tolerance", None) is None else float(p.get("tolerance"))),
                        ok_text=str(p.get("ok_text","OK")),
                        alarm_text=str(p.get("alarm_text","ALARM")),
                    ))
                except Exception:
                    continue

            def _flt(d: Dict[str, Any]) -> FlowFilter:
                return FlowFilter(
                    group_window_s = int((d or {}).get("group_window_s", 30)),
                    include = list((d or {}).get("include", []) or []),
                    exceptions = list((d or {}).get("exceptions", []) or []),
                )

            flows.append(FlowCfg(
                id=fid, name=name, type=typ, enabled=enabled,
                telegram=it.get("telegram", {}) or {},
                ronet=it.get("ronet", {}) or {},
                params=params,
                events=_flt(it.get("events", {})),
                intervals=_flt(it.get("intervals", {})),
            ))
        return flows

    def _serialize_cfg(self, flows: List[FlowCfg]) -> Dict[str, Any]:
        out: Dict[str, Any] = {"flows": []}
        for f in flows:
            out["flows"].append({
                "id": f.id,
                "name": f.name,
                "type": f.type,
                "enabled": bool(f.enabled),
                "telegram": f.telegram or {},
                "ronet": f.ronet or {},
                "params": [
                    {
                        "line": p.line,
                        "unit_id": p.unit_id,
                        "name": p.name,
                        "alias": p.alias,
                        "location": p.location,
                        "nominal": p.nominal,
                        "tolerance": p.tolerance,
                        "ok_text": p.ok_text,
                        "alarm_text": p.alarm_text,
                    } for p in f.params
                ],
                "events": {
                    "group_window_s": f.events.group_window_s,
                    "include": f.events.include or [],
                    "exceptions": f.events.exceptions or [],
                },
                "intervals": {
                    "group_window_s": f.intervals.group_window_s,
                    "include": f.intervals.include or [],
                    "exceptions": f.intervals.exceptions or [],
                }
            })
        return out


# ─────────────────────────────────────────────────────────────────────────────
# Singleton
# ─────────────────────────────────────────────────────────────────────────────

alerts_engine = AlertsEngine()
