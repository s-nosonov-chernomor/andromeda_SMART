# app/services/modbus_line.py
from __future__ import annotations

import os
import json
import time
import random
import threading
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict
from datetime import datetime, timezone

import minimalmodbus
import serial
try:
    # RS485 режим доступен не везде — используем опционально
    from serial.rs485 import RS485Settings
except Exception:  # pragma: no cover
    RS485Settings = None  # type: ignore

from app.core.config import settings
from app.core import state  # state.update(...) для «текущих»
# примечание: MqttBridge импортировать только для type hints, чтобы не ловить циклы
# from app.services.mqtt_bridge import MqttBridge

log_mod = logging.getLogger("modbus")


# ─────────────────────────────────────────────────────────────────────────────
# Модели конфигурации
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ParamCfg:
    name: str
    register_type: str              # "coil"|"discrete"|"holding"|"input"
    address: int
    scale: float = 1.0
    mode: str = "r"                 # "r"|"rw"
    publish_mode: str = "on_change" # "on_change"|"interval"|"on_change_and_interval"|"both"
    publish_interval_ms: int = 0
    topic: Optional[str] = None     # относительный или абсолютный топик
    error_state: Optional[int] = None  # 0/1 или None
    display_error_text: Optional[str] = None
    mqttROM: Optional[str] = None

@dataclass
class NodeCfg:
    unit_id: int
    object: str
    params: List[ParamCfg]
    num_object: Optional[int] = None
    params: List[ParamCfg] = field(default_factory=list)

# ─────────────────────────────────────────────────────────────────────────────
# Вспомогательные утилиты
# ─────────────────────────────────────────────────────────────────────────────

def _safe_scale(scale: Optional[float]) -> float:
    try:
        s = float(scale or 1.0)
        return s if s != 0.0 else 1.0
    except Exception:
        return 1.0

def _now_ms() -> float:
    return time.time()

def _iso_utc_ms() -> str:
    dt = datetime.now(timezone.utc)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


# ─────────────────────────────────────────────────────────────────────────────
# Класс линии Modbus RTU
# ─────────────────────────────────────────────────────────────────────────────

class ModbusLine(threading.Thread):
    """
    Одна физическая линия (COM-порт). Опрос идёт в отдельном потоке.

    Возможности:
      • Пакетное чтение последовательных регистров (1/2/3/4) с лимитами;
      • Публикация по режимам: on_change / interval / both (on_change_and_interval);
      • Heartbeat при ошибках: value=null + status_code {code, message}, metadata.silent_for_s;
      • Свой кастомный topic для каждого параметра (абс. или относительный от base_topic);
      • Команды записи по топику <pub-topic>/on для mode=rw (coil/holding);
      • Повторное открытие порта с backoff при занятости/ошибках;
      • Сводки по линии (debug.summary_every_s), нормализация адресов в debug.mode.
    """

    def __init__(self, line_conf: dict, mqtt_bridge, poll_conf: dict):
        """
        :param line_conf: секция одной линии из YAML
        :param mqtt_bridge: инстанс MqttBridge
        :param poll_conf: секция polling из YAML
        """
        super().__init__(daemon=True)
        self.name_ = line_conf.get("name") or f"line_{id(self)}"
        self.log = logging.getLogger(f"line.{self.name_}")

        # Аппаратные параметры порта
        self.port = line_conf["device"]
        self.baudrate = int(line_conf.get("baudrate", 9600))
        self.timeout = float(line_conf.get("timeout", 0.1))
        self.parity = str(line_conf.get("parity", "N")).upper()
        self.stopbits = int(line_conf.get("stopbits", 1))
        self.rs485_toggle = bool(line_conf.get("rs485_rts_toggle", False))

        # Переоткрытие порта / бэкофф
        self._port_fault = False
        self._port_retry_at = 0.0
        self._port_retry_backoff = float(
            line_conf.get("port_retry_backoff_s", poll_conf.get("port_retry_backoff_s", 5.0))
        )

        # Глобальные настройки/режимы
        self.mqtt = mqtt_bridge
        self.poll = poll_conf
        self.debug = {
            "enabled": bool(settings.debug.get("enabled", False)),
            "log_reads": bool(settings.debug.get("log_reads", False)),
            "summary_every_s": int(settings.debug.get("summary_every_s", 0)),
        }

        # Batch-read лимиты
        self.batch = (self.poll.get("batch_read") or {})
        self.batch_enabled = bool(self.batch.get("enabled", True))
        self.batch_max_bits = int(self.batch.get("max_bits", 1968))
        self.batch_max_regs = int(self.batch.get("max_registers", 120))

        # Узлы
        self.nodes: List[NodeCfg] = []
        for n in line_conf.get("nodes", []):
            params = [ParamCfg(**p) for p in n.get("params", [])]
            self.nodes.append(NodeCfg(unit_id=int(n["unit_id"]), object=n["object"], params=params))

        total_params = sum(len(n.params) for n in self.nodes)
        if not self.nodes:
            self.log.warning("no nodes configured on this line")
        else:
            self.log.info(f"configured: nodes={len(self.nodes)}, params={total_params}")

        # Runtime состояние
        self._instruments: Dict[int, minimalmodbus.Instrument] = {}
        self._last_values: Dict[str, float] = {}      # key = f"{unit}:{param}"
        self._last_pub_ts: Dict[str, float] = {}      # последний publish (для interval)
        self._last_ok_ts: Dict[str, float] = {}       # последний успешный read
        self._last_attempt_ts: Dict[str, float] = {}  # последний попытка read
        self._no_reply: Dict[int, int] = {}           # счётчик неответов по unit_id
        self._stats = defaultdict(int)                # (unit,"read_ok"/"read_err")->int

        # Подписка на команды записи (…/on) для rw-параметров
        self._setup_command_subscriptions()

        # Флаг остановки
        self._stop = threading.Event()

    # ───────────────────────── общая жизнедеятельность ─────────────────────────
    def stop(self):
        self._stop.set()
        for inst in self._instruments.values():
            try:
                if getattr(inst, "serial", None):
                    inst.serial.close()
            except Exception:
                pass

    # ───────────────────────── MQTT вспомогательные ────────────────────────────
    def _pub_topic_for(self, nd: NodeCfg, p: ParamCfg) -> str:
        """Публикационный топик (без /on). Может быть относительным или абсолютным."""
        if p.topic:
            return p.topic
        return f"{nd.object}/controls/{p.name}"

    def _cmd_topic_abs(self, nd: NodeCfg, p: ParamCfg) -> str:
        """Абсолютный командный топик (с учётом base_topic)."""
        base = getattr(self.mqtt, "base", "/devices")
        pub = self._pub_topic_for(nd, p)
        if pub.startswith("/"):
            return pub + "/on"
        return f"{base}/{pub}/on"

    def _abs_topic(self, topic_like: str) -> str:
        base = getattr(self.mqtt, "base", "/devices")
        return topic_like if topic_like.startswith("/") else f"{base}/{topic_like}"

    def _mqtt_publish(self, topic_like: str, value: Optional[float], code: int,
                      message: str, silent_for_s: int, ctx: Dict[str, Any]):
        """
        Унифицированная публикация JSON.
        Сначала пытаемся вызвать современный MqttBridge.publish(...),
        иначе собираем JSON вручную и шлём через client.publish.
        """
        payload = {
            "value": None if value is None else (value if isinstance(value, (int, float)) else str(value)),
            "metadata": {
                "timestamp": _iso_utc_ms(),
                "status_code": {"code": int(code), "message": str(message)},
                "silent_for_s": int(silent_for_s),
                "context": ctx
            }
        }

        # Новый Мост умеет publish(...)? — используем его
        if hasattr(self.mqtt, "publish") and callable(getattr(self.mqtt, "publish")):
            try:
                self.mqtt.publish(topic_like, value, code=code,
                                  status_details={"message": message, "silent_for_s": silent_for_s},
                                  context=ctx)
                return
            except Exception as e:
                self.log.warning(f"mqtt.publish failed, fallback to raw json: {e}")

        # Иначе публикуем сами
        topic_abs = self._abs_topic(topic_like)
        try:
            # paho client есть во всех версиях моста
            self.mqtt.client.publish(topic_abs, json.dumps(payload, ensure_ascii=False),
                                     qos=getattr(self.mqtt, "qos", 0),
                                     retain=getattr(self.mqtt, "retain", False))
        except Exception as e:
            self.log.error(f"MQTT publish error [{topic_abs}]: {e}")

    # ───────────────────────── подписка на команды записи ──────────────────────
    def _setup_command_subscriptions(self):
        if not hasattr(self.mqtt, "register_on_topic"):
            self.log.warning("mqtt bridge has no 'register_on_topic'; write commands disabled")
            return
        count = 0
        for nd in self.nodes:
            for p in nd.params:
                if str(p.mode).lower() != "rw":
                    continue
                topic_on = self._cmd_topic_abs(nd, p)
                self.mqtt.register_on_topic(topic_on, self._make_write_handler(nd, p))
                count += 1
        if count:
            self.log.info(f"subscribed write handlers: {count}")

    def _make_write_handler(self, nd: NodeCfg, p: ParamCfg):
        def handler(value_str: str) -> bool:
            try:
                inst = self._inst(nd.unit_id)
                if inst is None:
                    self.log.error("write: serial not ready")
                    return False
                try:
                    num = float(value_str)
                except Exception:
                    num = 0.0
                raw = int(round(num * _safe_scale(p.scale)))
                addr = self._normalize_addr(p)

                if p.register_type == "coil":
                    inst.write_bit(addr, 1 if raw != 0 else 0, functioncode=5)
                elif p.register_type == "holding":
                    inst.write_register(addr, raw, functioncode=6, signed=False)
                else:
                    self.log.error(f"{p.name}: write unsupported for type={p.register_type}")
                    return False

                # локально обновим last_values, чтобы on_change не спамил
                key = self._make_key(nd.unit_id, p.name)
                self._last_values[key] = num
                self._last_ok_ts[key] = _now_ms()
                return True
            except Exception as e:
                self.log.error(f"write error {nd.unit_id}/{p.name}: {e}")
                self._no_reply[nd.unit_id] = self._no_reply.get(nd.unit_id, 0) + 1
                return False
        return handler

    # ───────────────────────── порт/инструмент ────────────────────────────────
    def _inst(self, unit_id: int) -> Optional[minimalmodbus.Instrument]:
        now = _now_ms()
        if self._port_fault and now < self._port_retry_at:
            return None

        inst = self._instruments.get(unit_id)
        if inst:
            return inst

        try:
            port_name = self.port
            # Win COM10+ дружелюбие
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

            if self.rs485_toggle and RS485Settings is not None:
                try:
                    inst.serial.rs485_mode = RS485Settings(
                        rts_level_for_tx=True, rts_level_for_rx=False,
                        loopback=False, delay_before_tx=None, delay_before_rx=None
                    )
                except Exception as e:
                    self.log.warning(f"RS485 toggle failed: {e}")

            self._instruments[unit_id] = inst
            if self._port_fault:
                self.log.info(f"port {self.port} reopened")
            self._port_fault = False
            return inst

        except serial.serialutil.SerialException as e:
            self._port_fault = True
            self._port_retry_at = now + self._port_retry_backoff
            self.log.warning(f"port busy/unavailable {self.port}: {e}. retry in {self._port_retry_backoff:.1f}s")
            return None
        except Exception as e:
            self._port_fault = True
            self._port_retry_at = now + self._port_retry_backoff
            self.log.error(f"port open error {self.port}: {e}. retry in {self._port_retry_backoff:.1f}s")
            return None

    # ───────────────────────── адреса и нормализация ──────────────────────────
    def _normalize_addr(self, p: ParamCfg) -> int:
        """В debug.enabled нормализуем 40001/30001 и 1-based coils/discretes."""
        a = int(p.address)
        if not self.debug.get("enabled", False):
            return a
        na = a
        if p.register_type == "holding" and a >= 40001:
            na = a - 40001
            self.log.debug(f"{p.name}: holding address {a}→{na} (normalized)")
        elif p.register_type == "input" and a >= 30001:
            na = a - 30001
            self.log.debug(f"{p.name}: input address {a}→{na} (normalized)")
        elif p.register_type in ("coil", "discrete") and 1 <= a < 100000:
            na = a - 1
            self.log.debug(f"{p.name}: {p.register_type} address {a}→{na} (normalized)")
        return na

    def _make_key(self, unit_id: int, param_name: str) -> str:
        return f"{unit_id}:{param_name}"

    # ───────────────────────── чтение: одиночное/блочное ──────────────────────
    def _read_single(self, inst: minimalmodbus.Instrument, p: ParamCfg) -> Tuple[Optional[float], int, str]:
        addr = self._normalize_addr(p)
        try:
            if p.register_type == "coil":
                raw = inst.read_bit(addr, functioncode=1)
            elif p.register_type == "discrete":
                raw = inst.read_bit(addr, functioncode=2)
            elif p.register_type == "holding":
                raw = inst.read_register(addr, functioncode=3, signed=False)
            elif p.register_type == "input":
                raw = inst.read_register(addr, functioncode=4, signed=False)
            else:
                return None, 10, "CONFIG_ERROR"
            val = float(raw) / _safe_scale(p.scale)
            if self.debug.get("log_reads"):
                self.log.debug(f"read ok {p.register_type}[{addr}] raw={raw} -> {val}")
            return val, 0, "OK"
        except Exception as e:
            code, msg = self._map_ex(e)
            if self.debug.get("enabled"):
                self.log.error(f"{p.name}: read error at {p.register_type}[{addr}] → {e}")
            return None, code, msg

    def _read_block_bits(self, inst: minimalmodbus.Instrument, start: int, count: int, functioncode: int) -> List[int]:
        if hasattr(inst, "read_bits"):
            return inst.read_bits(start, count, functioncode=functioncode)
        # fallback — по одному
        out = []
        for i in range(count):
            out.append(inst.read_bit(start + i, functioncode=functioncode))
        return out

    def _read_block_regs(self, inst: minimalmodbus.Instrument, start: int, count: int, functioncode: int) -> List[int]:
        return inst.read_registers(start, count, functioncode=functioncode)

    def _group_sequential(self, params: List[ParamCfg]) -> List[Tuple[str, int, int, List[ParamCfg]]]:
        """Группирует параметры по типу и последовательным адресам для batch-read."""
        if not params:
            return []
        items = sorted(params, key=lambda x: (x.register_type, self._normalize_addr(x)))
        blocks: List[Tuple[str, int, int, List[ParamCfg]]] = []

        cur_type = items[0].register_type
        cur_start = self._normalize_addr(items[0])
        cur_params = [items[0]]
        prev_addr = cur_start

        def flush():
            nonlocal blocks, cur_type, cur_start, cur_params
            blocks.append((cur_type, cur_start, len(cur_params), cur_params))
            cur_params = []

        for p in items[1:]:
            rtype = p.register_type
            addr = self._normalize_addr(p)
            contiguous = (rtype == cur_type) and (addr == prev_addr + 1)

            if contiguous:
                # проверим лимиты блока
                next_count = len(cur_params) + 1
                over = (rtype in ("coil", "discrete") and next_count > self.batch_max_bits) or \
                       (rtype in ("holding", "input") and next_count > self.batch_max_regs)
                if over:
                    flush()
                    cur_type, cur_start, cur_params = rtype, addr, [p]
                else:
                    cur_params.append(p)
            else:
                flush()
                cur_type, cur_start, cur_params = rtype, addr, [p]
            prev_addr = addr

        if cur_params:
            flush()
        return blocks

    # ───────────────────────── публикация/heartbeat/«текущие» ─────────────────
    def _ctx(self, nd: NodeCfg, p: ParamCfg) -> Dict[str, Any]:
        return {
            "object": nd.object,
            "line": self.name_,
            "unit_id": nd.unit_id,
            "register_type": p.register_type,
            "address": self._normalize_addr(p),
            "param": p.name,
        }

    def _maybe_publish(self, nd: NodeCfg, p: ParamCfg, value: Optional[float], code: int, message: str,
                       last_ok_ts: Optional[float], last_attempt_ts: Optional[float]):
        """
        Логика публикации по режимам + обновление «текущих».
        При ошибке (code!=0) heartbeat шлём только если режим включает интервал и он наступил.
        """
        key = self._make_key(nd.unit_id, p.name)
        now = _now_ms()
        last_val = self._last_values.get(key)
        last_pub = self._last_pub_ts.get(key, 0.0)

        mode = (p.publish_mode or "on_change").lower()
        interval_ms = int(p.publish_interval_ms or 0)
        due_interval = interval_ms > 0 and ((now - last_pub) * 1000.0 >= interval_ms)
        changed = (value is not None) and (last_val is None or value != last_val)

        topic_like = self._pub_topic_for(nd, p)
        ctx = self._ctx(nd, p)

        def pub(val, c, msg, silent_s):
            self._mqtt_publish(topic_like, val, c, msg, int(silent_s), ctx)
            self._last_pub_ts[key] = now

        if code == 0:
            # OK-путь
            if mode in ("on_change_and_interval", "both"):
                if changed or due_interval:
                    pub(value, 0, "OK", 0)
            elif mode == "on_change":
                if changed:
                    pub(value, 0, "OK", 0)
            elif mode == "interval":
                if due_interval:
                    pub(value, 0, "OK", 0)
            else:
                # неизвестный режим → как on_change
                if changed:
                    pub(value, 0, "OK", 0)

            # обновим last_values и «текущие»
            self._last_values[key] = value  # type: ignore
            state.update(self.name_, nd.unit_id, nd.object, p.name,
                         topic_like if topic_like.startswith("/") else f"{getattr(self.mqtt,'base','/devices')}/{topic_like}",
                         p.register_type, self._normalize_addr(p),
                         value, 0, "OK",
                         last_ok_ts or now, last_attempt_ts or now)
            return

        # Ошибка: heartbeat в интервале
        silent_for = int(now - (last_ok_ts or now))
        if mode in ("on_change_and_interval", "both", "interval") and due_interval:
            pub(None, code, message, silent_for)

        # «текущие» при ошибке
        state.update(self.name_, nd.unit_id, nd.object, p.name,
                     topic_like if topic_like.startswith("/") else f"{getattr(self.mqtt,'base','/devices')}/{topic_like}",
                     p.register_type, self._normalize_addr(p),
                     None, code, message,
                     last_ok_ts or 0.0, last_attempt_ts or now)

    # ───────────────────────── маппинг ошибок ─────────────────────────────────
    def _map_ex(self, e: Exception) -> Tuple[int, str]:
        s = str(e).lower()
        if "timeout" in s or "no response" in s or "timed out" in s:
            return 1, "TIMEOUT"
        if "crc" in s or "invalid response" in s or "checksum" in s:
            return 2, "CRC_ERROR"
        if "illegal function" in s:
            return 3, "ILLEGAL_FUNCTION"
        if "illegal data address" in s:
            return 4, "ILLEGAL_DATA_ADDRESS"
        if "illegal data value" in s:
            return 5, "ILLEGAL_DATA_VALUE"
        if "slave device failure" in s:
            return 6, "SLAVE_DEVICE_FAILURE"
        if isinstance(e, serial.serialutil.SerialException) or "access is denied" in s or "permission" in s:
            return 7, "PORT_BUSY"
        return 12, "UNKNOWN_ERROR"

    # ───────────────────────── основной цикл ──────────────────────────────────
    def run(self):
        base_sleep = float(self.poll.get("interval_ms", 1000)) / 1000.0
        jitter = float(self.poll.get("jitter_ms", 0)) / 1000.0
        backoff_ms = float(self.poll.get("backoff_ms", 500)) / 1000.0
        max_err = int(self.poll.get("max_errors_before_backoff", 5))

        self.log.info(f"started on {self.port} @ {self.baudrate}")
        t_summary = _now_ms()
        summary_every = int(self.debug.get("summary_every_s", 0))

        while not self._stop.is_set():
            loop_start = _now_ms()
            for nd in self.nodes:
                inst = self._inst(nd.unit_id)
                if inst is None:
                    self._stats[(nd.unit_id, "read_err")] += 1
                    continue

                # Batch-read (если включено)
                if self.batch_enabled:
                    blocks = self._group_sequential(nd.params)
                    for rtype, start, count, group_params in blocks:
                        try:
                            if rtype == "coil":
                                block_vals = self._read_block_bits(inst, start, count, functioncode=1)
                            elif rtype == "discrete":
                                block_vals = self._read_block_bits(inst, start, count, functioncode=2)
                            elif rtype == "holding":
                                block_vals = self._read_block_regs(inst, start, count, functioncode=3)
                            elif rtype == "input":
                                block_vals = self._read_block_regs(inst, start, count, functioncode=4)
                            else:
                                raise ValueError(f"unknown register_type: {rtype}")

                            # успех блока
                            for i, p in enumerate(group_params):
                                raw = block_vals[i]
                                val = float(raw) / _safe_scale(p.scale)
                                key = self._make_key(nd.unit_id, p.name)
                                now = _now_ms()
                                self._last_ok_ts[key] = now
                                self._last_attempt_ts[key] = now
                                self._no_reply[nd.unit_id] = 0
                                self._stats[(nd.unit_id, "read_ok")] += 1
                                if self.debug.get("log_reads"):
                                    self.log.debug(f"read ok {rtype}[{start+i}] raw={raw} -> {val}")
                                self._maybe_publish(nd, p, val, 0, "OK", self._last_ok_ts[key], self._last_attempt_ts[key])

                        except Exception as e:
                            code, msg = self._map_ex(e)
                            if self.debug.get("enabled"):
                                self.log.error(f"block read error {rtype}[{start}..{start+count-1}] → {e}")
                            self._no_reply[nd.unit_id] = self._no_reply.get(nd.unit_id, 0) + 1
                            for i, p in enumerate(group_params):
                                key = self._make_key(nd.unit_id, p.name)
                                now = _now_ms()
                                self._last_attempt_ts[key] = now
                                self._stats[(nd.unit_id, "read_err")] += 1
                                self._maybe_publish(nd, p, None, code, msg, self._last_ok_ts.get(key), now)
                            if self._no_reply[nd.unit_id] >= max_err:
                                time.sleep(backoff_ms)
                    continue  # следующий узел

                # Одиночные чтения (если batch выключен)
                for p in nd.params:
                    key = self._make_key(nd.unit_id, p.name)
                    val, code, msg = self._read_single(inst, p)
                    now = _now_ms()
                    self._last_attempt_ts[key] = now
                    if val is None:
                        self._no_reply[nd.unit_id] = self._no_reply.get(nd.unit_id, 0) + 1
                        self._stats[(nd.unit_id, "read_err")] += 1
                        self._maybe_publish(nd, p, None, code, msg, self._last_ok_ts.get(key), now)
                        if self._no_reply[nd.unit_id] >= max_err:
                            time.sleep(backoff_ms)
                    else:
                        self._no_reply[nd.unit_id] = 0
                        self._stats[(nd.unit_id, "read_ok")] += 1
                        self._last_ok_ts[key] = now
                        self._maybe_publish(nd, p, val, 0, "OK", self._last_ok_ts[key], now)

            # Сводка по линии
            if summary_every and (_now_ms() - t_summary) >= summary_every:
                parts = []
                units = sorted({u for (u, k) in self._stats.keys()})
                for u in units:
                    ok = self._stats[(u, "read_ok")]
                    er = self._stats[(u, "read_err")]
                    parts.append(f"unit {u}: ok={ok} err={er} no_reply={self._no_reply.get(u,0)}")
                if parts:
                    self.log.info("summary: " + " | ".join(parts))
                t_summary = _now_ms()

            # Сон с джиттером
            elapsed = _now_ms() - loop_start
            time.sleep(max(0.0, base_sleep - elapsed) + random.uniform(0.0, jitter))
