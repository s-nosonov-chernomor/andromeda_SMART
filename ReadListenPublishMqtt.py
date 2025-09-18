#!/usr/bin/env python3
# ReadListenPublishMqtt.py
import os, json, time, threading, queue, random, signal, sys
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Dict, Any, Optional, List
from collections import defaultdict
import logging

import yaml
import minimalmodbus
import serial
import paho.mqtt.client as mqtt

from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text
from sqlalchemy.orm import sessionmaker, declarative_base

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
log = logging.getLogger("uspd")

def setup_logging(debug_conf: dict):
    level = logging.DEBUG if debug_conf.get("enabled") else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def utc_now_iso_ms() -> str:
    dt = datetime.now(timezone.utc)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")

# ─────────────────────────────────────────────────────────────────────────────
# DB
# ─────────────────────────────────────────────────────────────────────────────
Base = declarative_base()

class MqttEvent(Base):
    __tablename__ = "mqtt_events"
    id = Column(Integer, primary_key=True)
    object = Column(String(120), index=True)
    param = Column(String(120), index=True)
    value = Column(String(64))
    ts = Column(DateTime(timezone=True), index=True, default=datetime.utcnow)
    raw = Column(Text)  # RAW JSON, что ушло в MQTT

def make_session(db_url: str):
    engine = create_engine(db_url, future=True)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)

def ensure_sqlite_dir(url: str):
    """Создать директорию под SQLite, если её нет (Windows-friendly)."""
    if not url.startswith("sqlite") or ":memory:" in url:
        return
    prefix = "sqlite:///"
    if url.startswith(prefix):
        fs_path = url[len(prefix):]
        dir_path = os.path.dirname(fs_path)
        if dir_path and not os.path.isabs(dir_path):
            dir_path = os.path.abspath(dir_path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# MQTT bridge
# ─────────────────────────────────────────────────────────────────────────────
class MqttBridge:
    def __init__(self, conf: dict, db_session_factory):
        self.conf = conf
        self.log = logging.getLogger("mqtt")
        # Callback API v2 (без DeprecationWarning)
        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=conf.get("client_id", ""),
            protocol=mqtt.MQTTv311,
        )
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.base = conf.get("base_topic", "/devices").rstrip("/")
        if not self.base.startswith("/"):
            self.base = "/" + self.base
        self.qos = int(conf.get("qos", 0))
        self.retain = bool(conf.get("retain", False))
        self.db_session_factory = db_session_factory
        self.write_handlers: Dict[str, Any] = {}  # topic->callable(value_str)->bool
        self.out_queue = queue.Queue()

    def connect(self):
        self.client.connect(self.conf["host"], int(self.conf["port"]))
        threading.Thread(target=self.client.loop_forever, daemon=True).start()
        threading.Thread(target=self._publisher_loop, daemon=True).start()

    def register_on_topic(self, topic: str, handler):
        # ожидаем ПОЛНЫЙ топик (с лидирующим '/'), иначе префиксуем base
        t = topic if topic.startswith("/") else f"{self.base}/{topic}"
        self.write_handlers[t] = handler
        self.client.subscribe(t, qos=self.qos)
        self.log.debug(f"subscribe {t}")

    def publish_value(self, object_: str, param: str, value: str, status_code: int = 0):
        topic = f"{self.base}/{object_}/controls/{param}"
        payload = {
            "value": str(value),
            "metadata": {
                "timestamp": utc_now_iso_ms(),
                "status_code": {"code": int(status_code)}
            }
        }
        self.log.debug(f"publish → {topic} value={payload['value']}")
        self.out_queue.put((topic, json.dumps(payload), self.qos, self.retain, object_, param, payload))

    def publish_raw(self, topic_like: str, value: str, status_code: int = 0):
        # принимает абсолютный или относительный топик
        if topic_like.startswith("/"):
            topic = topic_like
        else:
            topic = f"{self.base}/{topic_like}"
        payload = {
            "value": str(value),
            "metadata": {
                "timestamp": utc_now_iso_ms(),
                "status_code": {"code": int(status_code)}
            }
        }
        self.log.debug(f"publish → {topic} value={payload['value']}")
        self.out_queue.put((topic, json.dumps(payload), self.qos, self.retain, "<raw>", "<raw>", payload))

    def _publisher_loop(self):
        Session = self.db_session_factory
        while True:
            topic, payload, qos, retain, obj, prm, payload_dict = self.out_queue.get()
            try:
                self.client.publish(topic, payload, qos=qos, retain=retain)
                with Session() as s:
                    evt = MqttEvent(object=obj, param=prm, value=json.loads(payload)["value"],
                                    ts=datetime.utcnow(), raw=payload)
                    s.add(evt)
                    s.commit()
            except Exception as e:
                self.log.error(f"publish error: {e}")

    # paho-mqtt v2 signature
    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        self.log.info(f"connected rc={reason_code}")

    def _on_message(self, client, userdata, msg):
        # ждём {"value": "..."}
        try:
            data = json.loads(msg.payload.decode("utf-8"))
            val = str(data.get("value", ""))
        except Exception:
            self.log.error(f"bad json on {msg.topic}: {msg.payload!r}")
            return
        handler = self.write_handlers.get(msg.topic)
        if handler:
            ok = False
            try:
                ok = handler(val)
            finally:
                self.log.info(f"write {msg.topic} -> {val} {'OK' if ok else 'FAIL'}")

# ─────────────────────────────────────────────────────────────────────────────
# Modbus
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class ParamCfg:
    name: str
    register_type: str  # coil|discrete|holding|input
    address: int
    scale: float
    mode: str          # r|rw
    publish_mode: str  # on_change|interval|on_change_and_interval|both
    publish_interval_ms: int
    topic: Optional[str] = None   # кастомный топик (относительный/абсолютный)

@dataclass
class NodeCfg:
    unit_id: int
    object: str
    params: List[ParamCfg]

_DEBUG_CONF = {"enabled": False, "log_reads": False, "summary_every_s": 0}

class ModbusLine(threading.Thread):
    def __init__(self, line_conf: dict, mqtt_bridge: MqttBridge, poll_conf: dict):
        super().__init__(daemon=True)
        self.name_ = line_conf["name"]
        self.mqtt = mqtt_bridge
        self.poll = poll_conf
        self.port = line_conf["device"]  # Windows: "COM4"
        self.baudrate = int(line_conf["baudrate"])
        self.timeout = float(line_conf.get("timeout", 0.08))
        self.parity = line_conf.get("parity", "N")
        self.stopbits = int(line_conf.get("stopbits", 1))
        self._port_fault = False
        self._port_retry_at = 0.0
        self._port_retry_backoff = float(
            line_conf.get("port_retry_backoff_s", poll_conf.get("port_retry_backoff_s", 5.0))
        )

        self.log = logging.getLogger(f"line.{self.name_}")
        self.debug = {
            "enabled": _DEBUG_CONF.get("enabled", False),
            "log_reads": _DEBUG_CONF.get("log_reads", False),
            "summary_every_s": _DEBUG_CONF.get("summary_every_s", 0),
        }

        self.nodes: List[NodeCfg] = []
        for n in line_conf.get("nodes", []):
            params = [ParamCfg(**p) for p in n.get("params", [])]
            self.nodes.append(NodeCfg(unit_id=int(n["unit_id"]), object=n["object"], params=params))

        total_params = sum(len(n.params) for n in self.nodes)
        if not self.nodes:
            self.log.warning("no nodes configured on this line")
        else:
            self.log.info(f"configured: nodes={len(self.nodes)}, params={total_params}")

        self._instruments: Dict[int, minimalmodbus.Instrument] = {}
        self._last_values: Dict[str, float] = {}      # key = f"{unit}:{name}"
        self._last_pub_ts: Dict[str, float] = {}      # для publish_interval
        self._no_reply: Dict[int, int] = {}           # счётчик неответов по unit_id
        self._stats = defaultdict(int)                # ("unit","read_ok"/"read_err")->int

        # подписки на /on для всех rw-параметров
        for nd in self.nodes:
            for p in nd.params:
                if p.mode == "rw":
                    topic_on = self._cmd_topic_for(nd, p)
                    self.mqtt.register_on_topic(topic_on, self._make_write_handler(nd, p))

        self._stop = threading.Event()

    def stop(self):
        self._stop.set()
        # закрыть порты
        for inst in self._instruments.values():
            try:
                if getattr(inst, "serial", None):
                    inst.serial.close()
            except:
                pass

    def _inst(self, unit_id: int) -> Optional[minimalmodbus.Instrument]:
        # если недавно падали — ждём до времени следующей попытки
        now = time.time()
        if self._port_fault and now < self._port_retry_at:
            return None

        # если уже есть открытый инстанс для этого unit — используем
        inst = self._instruments.get(unit_id)
        if inst:
            return inst

        # пытаемся открыть порт
        try:
            port_name = self.port
            # На Windows иногда нужен спец. синтаксис для COM10+
            if os.name == "nt" and port_name.upper().startswith("COM"):
                try:
                    n = int(port_name[3:])
                    if n >= 10:
                        port_name = r"\\.\%s" % port_name
                except Exception:
                    pass

            inst = minimalmodbus.Instrument(port_name, unit_id, mode=minimalmodbus.MODE_RTU)
            inst.serial.baudrate = self.baudrate
            inst.serial.timeout = self.timeout
            inst.serial.bytesize = 8
            inst.serial.parity = {
                'N': serial.PARITY_NONE,
                'E': serial.PARITY_EVEN,
                'O': serial.PARITY_ODD
            }.get(self.parity, serial.PARITY_NONE)
            inst.serial.stopbits = self.stopbits
            inst.clear_buffers_before_each_transaction = True

            self._instruments[unit_id] = inst

            if self._port_fault:
                self.log.info(f"port {self.port} reopened")
            self._port_fault = False
            return inst

        except serial.serialutil.SerialException as e:
            # порт занят/нет доступа — уходим в бэкофф и попробуем позже
            self._port_fault = True
            self._port_retry_at = now + self._port_retry_backoff
            self.log.warning(f"port busy/unavailable {self.port}: {e}. retry in {self._port_retry_backoff:.1f}s")
            return None
        except Exception as e:
            # любая иная ошибка открытия
            self._port_fault = True
            self._port_retry_at = now + self._port_retry_backoff
            self.log.error(f"port open error {self.port}: {e}. retry in {self._port_retry_backoff:.1f}s")
            return None

    def _make_key(self, unit_id: int, param_name: str) -> str:
        return f"{unit_id}:{param_name}"

    def _make_write_handler(self, node: NodeCfg, p: ParamCfg):
        # Запись в регистр при получении команды в /on
        def handler(value_str: str) -> bool:
            try:
                inst = self._inst(node.unit_id)
                if inst is None:
                    self.log.warning("write skipped: serial not ready yet")
                    return False
                # обратный scale
                try:
                    num = float(value_str)
                except:
                    num = 0.0
                raw = int(round(num * (p.scale if p.scale else 1.0)))
                addr = self._normalize_addr_for_write(p)
                if p.register_type == "coil":
                    inst.write_bit(addr, 1 if raw != 0 else 0, functioncode=5)
                elif p.register_type == "holding":
                    inst.write_register(addr, raw, functioncode=6, signed=False)
                else:
                    self.log.error(f"{p.name}: write unsupported for type={p.register_type}")
                    return False
                # локально обновим последнюю величину, чтобы on_change не флудил
                self._last_values[self._make_key(node.unit_id, p.name)] = num
                return True
            except Exception as e:
                self.log.error(f"write error {node.unit_id}/{p.name}: {e}")
                self._no_reply[node.unit_id] = self._no_reply.get(node.unit_id, 0) + 1
                return False
        return handler

    def _normalize_addr(self, p: ParamCfg) -> int:
        """Автонормализация адресов в DEBUG:
        - holding: 40001→0
        - input:   30001→0
        - coil/discrete: 1-based→0-based
        В обычном режиме предполагаем, что адреса уже 0-based.
        """
        a = int(p.address)
        if not self.debug.get("enabled", False):
            return a
        na = a
        if p.register_type == "holding" and a >= 40001:
            na = a - 40001
            self.log.debug(f"{p.name}: holding address {a}→{na} (auto-normalized)")
        elif p.register_type == "input" and a >= 30001:
            na = a - 30001
            self.log.debug(f"{p.name}: input address {a}→{na} (auto-normalized)")
        elif p.register_type in ("coil", "discrete") and a >= 1 and a < 100000:
            na = a - 1
            self.log.debug(f"{p.name}: {p.register_type} address {a}→{na} (auto-normalized)")
        return na

    def _normalize_addr_for_write(self, p: ParamCfg) -> int:
        return self._normalize_addr(p)

    def read_param(self, inst: minimalmodbus.Instrument, p: ParamCfg) -> Optional[float]:
        addr = self._normalize_addr(p)
        try:
            if p.register_type == "coil":
                val = inst.read_bit(addr, functioncode=1)
            elif p.register_type == "discrete":
                val = inst.read_bit(addr, functioncode=2)
            elif p.register_type == "holding":
                val = inst.read_register(addr, functioncode=3, signed=False)
            elif p.register_type == "input":
                val = inst.read_register(addr, functioncode=4, signed=False)
            else:
                self.log.error(f"{p.name}: unknown register_type={p.register_type}")
                return None

            scaled = float(val) / (p.scale if p.scale else 1.0)
            if self.debug.get("log_reads"):
                self.log.debug(f"read ok {p.register_type}[{addr}] raw={val} → {scaled}")
            return scaled
        except Exception as e:
            if self.debug.get("enabled"):
                self.log.error(f"{p.name}: read error at {p.register_type}[{addr}] → {e}")
            return None

    def maybe_publish(self, node: NodeCfg, p: ParamCfg, value: float):
        """
        Публикация в один и тот же топик:
        - on_change: сразу при изменении
        - interval: каждые publish_interval_ms
        - on_change_and_interval|both: и так, и так
        """
        key = self._make_key(node.unit_id, p.name)
        now = time.time()
        last_val = self._last_values.get(key)
        last_pub = self._last_pub_ts.get(key, 0.0)

        mode = (p.publish_mode or "on_change").lower()
        interval_ms = int(p.publish_interval_ms or 0)
        due_interval = interval_ms > 0 and ((now - last_pub) * 1000.0 >= interval_ms)
        changed = (last_val is None) or (value != last_val)

        if mode in ("on_change_and_interval", "both"):
            should_publish = changed or due_interval
        elif mode == "on_change":
            should_publish = changed
        elif mode == "interval":
            should_publish = due_interval
        else:
            should_publish = changed  # дефолт

        if should_publish:
            topic = self._pub_topic_for(node, p)  # относительный или абсолютный
            self.mqtt.publish_raw(topic, str(value), status_code=0)
            self._last_pub_ts[key] = now

        self._last_values[key] = value

    def run(self):
        base_sleep = self.poll["interval_ms"] / 1000.0
        jitter = self.poll.get("jitter_ms", 0) / 1000.0
        backoff_ms = self.poll.get("backoff_ms", 500) / 1000.0
        max_err = int(self.poll.get("max_errors_before_backoff", 5))

        self.log.info(f"started on {self.port} @ {self.baudrate}")
        t0 = time.time()
        summary_every = int(self.debug.get("summary_every_s", 0))

        while not self._stop.is_set():
            start = time.time()
            for nd in self.nodes:
                inst = self._inst(nd.unit_id)
                if inst is None:
                    self._stats[(nd.unit_id, "read_err")] += 1
                    continue
                for p in nd.params:
                    val = self.read_param(inst, p)
                    if val is None:
                        self._no_reply[nd.unit_id] = self._no_reply.get(nd.unit_id, 0) + 1
                        self._stats[(nd.unit_id, "read_err")] += 1
                        if self._no_reply[nd.unit_id] >= max_err:
                            time.sleep(backoff_ms)
                        continue
                    else:
                        self._no_reply[nd.unit_id] = 0
                        self._stats[(nd.unit_id, "read_ok")] += 1
                        self.maybe_publish(nd, p, val)

            if summary_every and (time.time() - t0) >= summary_every:
                parts = []
                units = sorted({u for (u, k) in self._stats.keys()})
                for u in units:
                    ok = self._stats[(u, "read_ok")]
                    er = self._stats[(u, "read_err")]
                    parts.append(f"unit {u}: ok={ok} err={er} no_reply={self._no_reply.get(u,0)}")
                if parts:
                    self.log.info("summary: " + " | ".join(parts))
                t0 = time.time()

            elapsed = time.time() - start
            time.sleep(max(0.0, base_sleep - elapsed) + random.uniform(0, jitter))

    def _pub_topic_for(self, node: NodeCfg, p: ParamCfg) -> str:
        # публикационный топик (без /on)
        if p.topic:
            return p.topic  # может быть относительным или абсолютным
        # fallback на старый шаблон
        return f"{node.object}/controls/{p.name}"

    def _cmd_topic_for(self, node: NodeCfg, p: ParamCfg) -> str:
        base = self._pub_topic_for(node, p)
        return (base if base.startswith("/") else f"{self.mqtt.base}/{base}") + "/on"

# ─────────────────────────────────────────────────────────────────────────────
# Управление линиями (ГЛОБАЛЬНО, не в классе!)
# ─────────────────────────────────────────────────────────────────────────────
LINES: List[ModbusLine] = []
LINES_LOCK = threading.Lock()
CURRENT_CFG: Optional[dict] = None
MQTT_BRIDGE: Optional[MqttBridge] = None
CFG_PATH: Optional[str] = None

def start_lines(cfg: dict, mqtt_bridge: MqttBridge):
    global LINES
    with LINES_LOCK:
        # на всякий случай стопнём старые (если были)
        for ln in LINES:
            try: ln.stop()
            except: pass
        for ln in LINES:
            try: ln.join(timeout=1.0)
            except: pass
        LINES = []
        for lc in cfg.get("lines", []):
            line = ModbusLine(lc, mqtt_bridge, cfg["polling"])
            line.start()
            LINES.append(line)

def stop_lines():
    with LINES_LOCK:
        for ln in LINES:
            try: ln.stop()
            except: pass
        for ln in LINES:
            try: ln.join(timeout=1.0)
            except: pass

def hot_reload_lines(new_cfg: dict):
    """Меняем ТОЛЬКО lines/polling на лету."""
    global CURRENT_CFG, MQTT_BRIDGE
    stop_lines()
    CURRENT_CFG["lines"] = new_cfg.get("lines", CURRENT_CFG.get("lines", []))
    CURRENT_CFG["polling"] = new_cfg.get("polling", CURRENT_CFG.get("polling", {}))
    start_lines(CURRENT_CFG, MQTT_BRIDGE)

# ─────────────────────────────────────────────────────────────────────────────
# WEB (FastAPI)
# ─────────────────────────────────────────────────────────────────────────────
from fastapi import FastAPI, Body, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import uvicorn

def start_web(db_session_factory, port: int = 8080, reload_callback=None):
    app = FastAPI(title="USPD Modbus Gateway")
    app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

    class EventDTO(BaseModel):
        id: int
        object: str
        param: str
        value: str
        ts: datetime
        raw: str

    @app.get("/api/config", response_class=HTMLResponse)
    def get_config():
        if not CFG_PATH or not os.path.exists(CFG_PATH):
            raise HTTPException(404, "config not found")
        with open(CFG_PATH, "r", encoding="utf-8") as f:
            content = f.read()
        return content  # text/plain по факту, HTMLResponse чтобы не коверкал

    @app.put("/api/config")
    def put_config(raw: str = Body(..., media_type="text/plain")):
        try:
            new_cfg = yaml.safe_load(raw) or {}
        except Exception as e:
            raise HTTPException(400, f"YAML parse error: {e}")
        for k in ("mqtt", "db", "polling", "lines"):
            if k not in new_cfg:
                raise HTTPException(400, f"missing section: {k}")

        bkp = f"{CFG_PATH}.{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.bak"
        try:
            if os.path.exists(CFG_PATH):
                os.replace(CFG_PATH, bkp)
            with open(CFG_PATH, "w", encoding="utf-8") as f:
                f.write(raw)
        except Exception as e:
            raise HTTPException(500, f"cannot write config: {e}")

        if reload_callback:
            try:
                reload_callback(new_cfg)
            except Exception as e:
                raise HTTPException(500, f"reload failed: {e}")

        return {"ok": True, "backup": os.path.basename(bkp)}

    @app.get("/ui/config", response_class=HTMLResponse)
    def ui_config():
        return """<!doctype html>
<html><head><meta charset="utf-8"><title>Config editor</title></head>
<body style="font-family:system-ui; max-width:900px; margin:24px auto;">
<h2>YAML config editor</h2>
<textarea id="t" style="width:100%;height:60vh;font-family:ui-monospace,Consolas,monospace;"></textarea>
<div style="margin-top:8px;">
  <button onclick="save()">Save</button>
  <span id="s"></span>
</div>
<script>
async function load(){
  const r = await fetch('/api/config');
  document.getElementById('t').value = await r.text();
}
async function save(){
  const raw = document.getElementById('t').value;
  const r = await fetch('/api/config',{method:'PUT',headers:{'Content-Type':'text/plain'},body:raw});
  const s = document.getElementById('s');
  if(r.ok){ s.textContent = 'Saved ✔'; } else { s.textContent = 'Error: '+await r.text(); }
}
load();
</script>
</body></html>"""

    @app.get("/", response_class=HTMLResponse)
    def index():
        return """<html><head><title>USPD Modbus Gateway</title></head>
        <body style="font-family:system-ui">
          <h1>USPD Modbus Gateway</h1>
          <ul>
            <li><a href="/api/ping">/api/ping</a></li>
            <li><a href="/api/events">/api/events</a></li>
            <li><a href="/docs">/docs</a></li>
            <li><a href="/ui/config">/ui/config</a> (edit YAML)</li>
          </ul>
        </body></html>"""

    @app.get("/api/ping")
    def ping():
        return {"ok": True, "ts": utc_now_iso_ms()}

    @app.get("/api/events", response_model=List[EventDTO])
    def events(limit: int = 200):
        Session = db_session_factory
        with Session() as s:
            rows = s.query(MqttEvent).order_by(MqttEvent.id.desc()).limit(limit).all()
            return rows[::-1]

    threading.Thread(
        target=lambda: uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning"),
        daemon=True
    ).start()
    logging.getLogger("web").info(f"web ui on http://127.0.0.1:{port}/")

# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    cfg_path = os.environ.get("USPD_CFG", os.path.join(os.getcwd(), "config.yaml"))
    if not os.path.exists(cfg_path):
        print(f"Config not found: {cfg_path}")
        sys.exit(1)
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    debug_conf = cfg.get("debug", {}) or {}
    global _DEBUG_CONF
    _DEBUG_CONF = {
        "enabled": bool(debug_conf.get("enabled", False)),
        "log_reads": bool(debug_conf.get("log_reads", False)),
        "summary_every_s": int(debug_conf.get("summary_every_s", 0)),
    }
    setup_logging(_DEBUG_CONF)
    log.info("service starting…")

    global CFG_PATH, CURRENT_CFG, MQTT_BRIDGE
    CFG_PATH = cfg_path
    CURRENT_CFG = cfg

    # БД
    db_url = cfg["db"]["url"]
    ensure_sqlite_dir(db_url)
    SessionFactory = make_session(db_url)

    # MQTT
    mqtt_bridge = MqttBridge(cfg["mqtt"], SessionFactory)
    mqtt_bridge.connect()
    MQTT_BRIDGE = mqtt_bridge  # ← важно: присвоим глобальной, чтобы hot-reload знал мост

    # Линии Modbus (первичный старт)
    start_lines(cfg, MQTT_BRIDGE)

    # WEB (c reload_callback)
    start_web(SessionFactory, port=8080, reload_callback=hot_reload_lines)

    # Живём до сигнала
    stop = threading.Event()
    def handle_sig(sig, frm): stop.set()
    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)
    while not stop.is_set():
        time.sleep(0.5)

if __name__ == "__main__":
    main()
