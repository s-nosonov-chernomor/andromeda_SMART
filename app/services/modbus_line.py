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
import math, struct
import queue

from pymodbus.client import ModbusTcpClient
from pymodbus.exceptions import ModbusException


try:
    # RS485 режим доступен не везде — используем опционально
    from serial.rs485 import RS485Settings
except Exception:  # pragma: no cover
    RS485Settings = None  # type: ignore

from app.core.config import settings
from app.core import state  # state.update(...) для «текущих»
from app.services.current_store import current_store
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
    publish_interval_s: float = 0.0
    topic: Optional[str] = None     # относительный или абсолютный топик
    error_state: Optional[int] = None  # 0/1 или None
    display_error_text: Optional[str] = None
    mqttROM: Optional[str] = None
    step: Optional[float] = None
    hysteresis: Optional[float] = None
    words: int = 1
    data_type: str = "u16"         # u16|s16|u32|s32|u64|s64|f32|f64
    word_order: str = "AB"         # AB|BA|ABCD|DCBA|BADC|CDAB ...

@dataclass
class NodeCfg:
    unit_id: int
    object: str
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

def _func_code(reg_type: str) -> int:
    return {"coil": 1, "discrete": 2, "holding": 3, "input": 4}.get(reg_type, 0)

def _round_ndp(v: Optional[float], ndp: int = 3) -> Optional[float]:
    try:
        if v is None:
            return None
        # через формат, чтобы убрать хвосты, затем обратно в float
        return float(f"{float(v):.{ndp}f}")
    except Exception:
        return v

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

    def __init__(self, line_conf: dict, mqtt_bridge, poll_conf: dict, serial_echo: bool = False):
        """
        :param line_conf: секция одной линии из YAML
        :param mqtt_bridge: инстанс MqttBridge
        :param poll_conf: секция polling из YAML
        """
        super().__init__(daemon=True)

        self.name_ = line_conf.get("name") or f"line_{id(self)}"
        self.log = logging.getLogger(f"line.{self.name_}")

        # Тип транспорта: RTU по COM или Modbus TCP
        self.transport = str(line_conf.get("transport", "serial")).lower()

        if self.transport == "tcp":
            # Параметры Modbus TCP
            self.host = line_conf["host"]
            self.tcp_port = int(line_conf.get("port", 502))
            self.timeout = float(line_conf.get("timeout", 1.0))
            self.port = f"{self.host}:{self.tcp_port}"  # только для логов
            # Для TCP эти поля по сути не используются, но оставим заглушки
            self.baudrate = None
            self.parity = "N"
            self.stopbits = 1
            self.rs485_toggle = False
        else:
            # Аппаратные параметры порта (RTU по COM)
            self.port = line_conf["device"]
            self.baudrate = int(line_conf.get("baudrate", 115200))
            self.timeout = float(line_conf.get("timeout", 0.1))
            self.parity = str(line_conf.get("parity", "N")).upper()
            self.stopbits = int(line_conf.get("stopbits", 1))
            self.rs485_toggle = bool(line_conf.get("rs485_rts_toggle", False))

        self.serial_echo = bool(serial_echo)


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

        # Настройка пульса в current_store (last_ok_ts без публикации)
        cursec = (settings.get_cfg().get("current") or {})
        self.touch_read_every_s: float = float(cursec.get("touch_read_every_s", 3))
        self.precision_decimals: int = int(cursec.get("precision_decimals", 3))



        # Последний раз, когда мы «тыкали» current_store по данному параметру
        self._last_store_touch_ok: Dict[str, float] = {}

        # детальная трассировка
        self.trace_frames = bool(settings.debug.get("trace_frames", False))
        if self.trace_frames:
            # поднимем уровни логгеров, чтобы увидеть Tx/Rx и сериал
            logging.getLogger("minimalmodbus").setLevel(logging.DEBUG)
            logging.getLogger("serial").setLevel(logging.DEBUG)

        # Адреса: использовать «человеческие» (1-based/30001/40001) или «сырые»
        adr = settings.get_cfg().get("addressing", {}) or {}
        self.addr_normalize = bool(adr.get("normalize", True))

        # Batch-read лимиты
        self.batch = (self.poll.get("batch_read") or {})
        self.batch_enabled = bool(self.batch.get("enabled", True))
        self.batch_max_bits = int(self.batch.get("max_bits", 1968))
        self.batch_max_regs = int(self.batch.get("max_registers", 120))

        # Узлы
        self.nodes: List[NodeCfg] = []
        for n in line_conf.get("nodes", []):
            # params = [ParamCfg(**p) for p in n.get("params", [])]
            params = []
            for p in n.get("params", []):
                # обратная совместимость: старое поле publish_interval_ms → секунды
                if "publish_interval_ms" in p and "publish_interval_s" not in p:
                    try:
                        p["publish_interval_s"] = float(p["publish_interval_ms"]) / 1000.0
                    except Exception:
                        p["publish_interval_s"] = 0.0
                params.append(ParamCfg(**p))

            self.nodes.append(NodeCfg(unit_id=int(n["unit_id"]), object=n["object"], params=params))

        total_params = sum(len(n.params) for n in self.nodes)
        if not self.nodes:
            self.log.warning("no nodes configured on this line")
        else:
            self.log.info(f"configured: nodes={len(self.nodes)}, params={total_params}")

        # Runtime состояние
        self._instruments: Dict[int, minimalmodbus.Instrument] = {}
        self._tcp_client: Optional[ModbusTcpClient] = None   # для TCP
        self._current_unit_id: Optional[int] = None          # для TCP-операций

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

        self._bands: Dict[str, Tuple[float, float]] = {}  # key -> (low_adj, high_adj)

        self._io_lock = threading.RLock()  # единый лок на все транзакции порта
        self._write_q: "queue.Queue[dict]" = queue.Queue()  # очередь задач записи

        # Для TCP пока отключим batch-read, будем читать по одному параметру
        if self.transport == "tcp":
            self.batch_enabled = False

    # ───────────────────────── общая жизнедеятельность ─────────────────────────
    def stop(self):
        self._stop.set()
        # RTU-инструменты
        for inst in self._instruments.values():
            try:
                if getattr(inst, "serial", None):
                    inst.serial.close()
            except Exception:
                pass
        # TCP-клиент
        if self._tcp_client is not None:
            try:
                self._tcp_client.close()
            except Exception:
                pass


    def _do_write_task(self, t: dict) -> None:
        """Выполнить одну задачу записи (владение портом под локом)."""
        p: ParamCfg = t["param"]
        unit_id: int = int(t["unit_id"])
        num: float = float(t["value_num"])
        inst = self._inst(unit_id)
        if inst is None:
            raise RuntimeError("serial not ready")

        raw = int(round(num / _safe_scale(p.scale)))
        addr = self._normalize_addr(p)

        with self._io_lock:  # <<< ключевой момент: сериализация транзакций
            if self.transport == "tcp":
                # Modbus TCP
                if p.register_type == "coil":
                    r = inst.write_coil(
                        address=addr,
                        value=1 if raw != 0 else 0,
                        slave=unit_id,
                    )
                elif p.register_type == "holding":
                    r = inst.write_register(
                        address=addr,
                        value=raw,
                        slave=unit_id,
                    )
                else:
                    raise RuntimeError(f"write unsupported for type={p.register_type}")
                if hasattr(r, "isError") and r.isError():
                    raise ModbusException(r)
            else:
                # RTU / minimalmodbus
                if p.register_type == "coil":
                    inst.write_bit(addr, 1 if raw != 0 else 0, functioncode=5)
                elif p.register_type == "holding":
                    inst.write_register(addr, raw, functioncode=6, signed=False)
                else:
                    raise RuntimeError(f"write unsupported for type={p.register_type}")


        # локально обновим last_values, чтобы on_change не спамил
        key = self._make_key(unit_id, p.name)
        now = _now_ms()
        self._last_values[key] = num
        self._last_ok_ts[key] = now
        self._last_attempt_ts[key] = now
        self._no_reply[unit_id] = 0
        self._stats[(unit_id, "read_ok")] += 1  # условно считаем успешную операцию

    def _drain_writes(self, max_tasks: int = 100) -> None:
        """Быстро выгружаем очередь записей перед чтениями (приоритет управления)."""
        n = 0
        while n < max_tasks:
            try:
                t = self._write_q.get_nowait()
            except queue.Empty:
                break
            try:
                self._do_write_task(t)
            except Exception as e:
                unit_id = t.get("unit_id")
                p: ParamCfg = t.get("param")  # type: ignore
                self.log.error(f"write error {unit_id}/{getattr(p, 'name', '?')}: {e}")
                self._no_reply[unit_id] = self._no_reply.get(unit_id, 0) + 1
            finally:
                self._write_q.task_done()
            n += 1

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

    # ───────────────────────── MQTT публикация ─────────────────────────
    def _mqtt_publish(
            self,
            topic_like: str,
            value: Optional[float],
            code: int,
            message: str,
            silent_for_s: int,
            ctx: Dict[str, Any],
            trigger: Optional[str],
            no_reply: int,
    ):
        """
        Единая публикация JSON.
        - trigger: 'change' | 'interval' | 'heartbeat'
        - no_reply: текущий счётчик подряд неответов для данного unit_id
        """
        # payload для сырой публикации И для current_store.apply_publish
        payload = {
            "value": None if value is None else (value if isinstance(value, (int, float)) else str(value)),
            "metadata": {
                "timestamp": _iso_utc_ms(),
                "status_code": {"code": int(code), "message": str(message)},
                "silent_for_s": int(silent_for_s),
                "trigger": trigger,
                # ВАЖНО: кладём счётчик неответов именно в metadata, как ждёт current_store
                "no_reply": int(no_reply),
                # в context — полезные детали идентификации
                "context": dict(ctx),
            },
        }

        # 1) high-level publish(...) у моста
        if hasattr(self.mqtt, "publish") and callable(getattr(self.mqtt, "publish")):
            try:
                self.mqtt.publish(
                    topic_like,
                    value,
                    code=code,
                    status_details={
                        "message": message,
                        "silent_for_s": silent_for_s,
                        "trigger": trigger,
                        "no_reply": int(no_reply),
                    },
                    context={**ctx, "trigger": trigger, "no_reply": int(no_reply)},
                )
                # сразу отразим публикацию в current_store
                try:
                    current_store.apply_publish(ctx, payload, datetime.now(timezone.utc))
                except Exception as ie:
                    self.log.debug(f"current_store.apply_publish failed: {ie}")
                return
            except Exception as e:
                self.log.warning(f"mqtt.publish failed, fallback to raw json: {e}")

        # 2) fallback: paho client
        topic_abs = self._abs_topic(topic_like)
        try:
            self.mqtt.client.publish(
                topic_abs,
                json.dumps(payload, ensure_ascii=False),
                qos=getattr(self.mqtt, "qos", 0),
                retain=getattr(self.mqtt, "retain", False),
            )
            try:
                current_store.apply_publish(ctx, payload, datetime.now(timezone.utc))
            except Exception as ie:
                self.log.debug(f"current_store.apply_publish failed: {ie}")
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
                try:
                    num = float(value_str)
                except Exception:
                    num = 0.0
                # ставим задачу на запись; выполнит поток run()
                self._write_q.put({
                    "unit_id": nd.unit_id,
                    "param": p,
                    "value_num": num,
                    "ts": time.time(),
                })
                return True  # приняли к исполнению
            except Exception as e:
                self.log.error(f"enqueue write error {nd.unit_id}/{p.name}: {e}")
                return False

        return handler

    # ───────────────────────── порт/инструмент ────────────────────────────────

    def _decode_words(self, regs: List[int], data_type: str, word_order: str):
        # нормализуем 16-бит
        regs = [int(x) & 0xFFFF for x in regs]
        dt = (data_type or "u16").lower()
        order = (word_order or ("ABCD" if dt in ("u64", "s64", "f64") else "AB")).upper()

        def reorder(words: List[int], order_str: str) -> List[int]:
            # order_str типа "AB", "BA", "ABCD", "DCBA", "BADC", "CDAB"
            letters = list(order_str)
            n = len(words)
            if len(letters) != n:
                # если кто-то оставил AB для 4 слов — считаем это ABCD
                if n == 4 and order_str in ("AB", "BA"):
                    letters = list("ABCD" if order_str == "AB" else "DCBA")
                else:
                    letters = list("ABCD")[:n]

            idx = {ch: i for i, ch in enumerate("ABCD"[:n])}
            try:
                return [words[idx[ch]] for ch in letters]
            except Exception:
                return words  # fallback

        if dt in ("u16", "s16"):
            v = regs[0]
            if dt == "s16" and v & 0x8000:
                v -= 0x10000
            return int(v)

        if dt in ("u32", "s32", "f32"):
            if len(regs) < 2:
                raise ValueError("need 2 words for 32-bit value")
            w = reorder(regs[:2], order if len(order) == 2 else "AB")
            w0, w1 = w[0], w[1]

            if dt == "f32":
                b = struct.pack(">HH", w0, w1)
                return float(struct.unpack(">f", b)[0])

            u32 = (w0 << 16) | w1
            if dt == "s32" and (u32 & 0x80000000):
                u32 -= 0x100000000
            return int(u32)

        if dt in ("u64", "s64", "f64"):
            if len(regs) < 4:
                raise ValueError("need 4 words for 64-bit value")
            w = reorder(regs[:4], order if len(order) == 4 else "ABCD")
            w0, w1, w2, w3 = w

            if dt == "f64":
                b = struct.pack(">HHHH", w0, w1, w2, w3)
                return float(struct.unpack(">d", b)[0])

            u64 = (w0 << 48) | (w1 << 32) | (w2 << 16) | w3
            if dt == "s64" and (u64 & 0x8000000000000000):
                u64 -= 0x10000000000000000
            return int(u64)

        # default
        return int(regs[0])

    def _band_make(self, v: float, step: float, hyst: float) -> Tuple[float, float]:
        if step <= 0:
            return (float("-inf"), float("inf"))
        k = math.floor(v / step)
        low = k * step
        high = low + step
        return (low - hyst, high + hyst)

    def _analog_changed(self, key: str, v: float, step: float, hyst: float) -> bool:
        """True, если значение вышло за текущие границы (с учётом гистерезиса)."""
        if step is None or step <= 0:
            # нет дискретности → сравнение по != (как раньше)
            last = self._last_values.get(key)
            return (last is None) or (v != last)
        h = abs(hyst or 0.0)
        band = self._bands.get(key)
        if band is None:
            # первая инициализация — считаем изменением, чтобы зафиксировать старт
            self._bands[key] = self._band_make(v, step, h)
            return True
        lo, hi = band
        if v < lo or v > hi:
            # вышли — переносим окно к новой «ячейке»
            self._bands[key] = self._band_make(v, step, h)
            return True
        return False

    def _inst(self, unit_id: int):
        now = _now_ms()
        if self._port_fault and now < self._port_retry_at:
            return None

        # ───────────── Modbus TCP ─────────────
        if self.transport == "tcp":
            if self._tcp_client is None:
                try:
                    cli = ModbusTcpClient(
                        host=self.host,
                        port=self.tcp_port,
                        timeout=self.timeout,
                    )
                    if not cli.connect():
                        raise ConnectionError("Modbus TCP connect() failed")
                    self._tcp_client = cli
                    if self._port_fault:
                        self.log.info(f"TCP {self.host}:{self.tcp_port} reconnected")
                    self._port_fault = False
                except Exception as e:
                    self._port_fault = True
                    self._port_retry_at = now + self._port_retry_backoff
                    self.log.error(
                        f"Modbus TCP connect error {self.host}:{self.tcp_port}: {e}. "
                        f"retry in {self._port_retry_backoff:.1f}s"
                    )
                    return None
            return self._tcp_client

        # ───────────── RTU по COM (как было) ─────────────
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

            if self.serial_echo:
                try:
                    # minimalmodbus сам вычитает отправленные байты из RX
                    inst.handle_local_echo = True
                except Exception:
                    pass
                try:
                    # на всякий случай очистим мусор
                    inst.serial.reset_input_buffer()
                    inst.serial.reset_output_buffer()
                except Exception:
                    pass

            if self.rs485_toggle and RS485Settings is not None:
                try:
                    inst.serial.rs485_mode = RS485Settings(
                        rts_level_for_tx=True, rts_level_for_rx=False,
                        loopback=False, delay_before_tx=None, delay_before_rx=None
                    )
                except Exception as e:
                    self.log.warning(f"RS485 toggle failed: {e}")

            # сырые кадры TX/RX от minimalmodbus (печатает Tx:/Rx:)
            if self.trace_frames and hasattr(inst, "debug"):
                inst.debug = True

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
        if not getattr(self, "addr_normalize", True):
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
    def _read_single(self, inst, p: ParamCfg) -> Tuple[Optional[float], int, str]:
        addr = self._normalize_addr(p)

        # ───────────── TCP ─────────────
        if self.transport == "tcp":
            unit_id = int(self._current_unit_id or 0)
            try:
                if p.register_type == "coil":
                    resp = inst.read_coils(
                        address=addr,
                        count=1,
                        slave=unit_id,
                    )
                    if resp.isError():
                        raise ModbusException(resp)
                    raw = int(bool(resp.bits[0]))

                elif p.register_type == "discrete":
                    resp = inst.read_discrete_inputs(
                        address=addr,
                        count=1,
                        slave=unit_id,
                    )
                    if resp.isError():
                        raise ModbusException(resp)
                    raw = int(bool(resp.bits[0]))

                elif p.register_type in ("holding", "input"):
                    words = max(1, int(p.words or 1))
                    if p.register_type == "holding":
                        resp = inst.read_holding_registers(
                            address=addr,
                            count=words,
                            slave=unit_id,
                        )
                    else:
                        resp = inst.read_input_registers(
                            address=addr,
                            count=words,
                            slave=unit_id,
                        )
                    if resp.isError():
                        raise ModbusException(resp)
                    regs = list(resp.registers)

                    if (p.words <= 1) and (p.data_type in ("u16", "s16")):
                        raw = regs[0]
                        if p.data_type == "s16" and (raw & 0x8000):
                            raw = raw - 0x10000
                    else:
                        raw = self._decode_words(regs, p.data_type or "u16", p.word_order or "AB")

                else:
                    return None, 10, "CONFIG_ERROR"

                raw_v = raw
                scale = _safe_scale(p.scale)

                # сохраняем int без потери точности, если scale=1 и тип целочисленный
                is_int_type = str(p.data_type or "").lower() in ("u16", "s16", "u32", "s32", "u64", "s64")
                if is_int_type and isinstance(raw_v, (int,)) and float(scale) == 1.0:
                    val = raw_v
                else:
                    val = float(raw_v) * float(scale)
                    val = _round_ndp(val, self.precision_decimals)

                if self.debug.get("log_reads"):
                    self.log.debug(f"TCP read ok {p.register_type}[{addr}] raw={raw} -> {val}")
                return val, 0, "OK"

            except Exception as e:
                code, msg = self._map_ex(e)
                if self.debug.get("enabled"):
                    self.log.error(f"{p.name}: TCP read error at {p.register_type}[{addr}] → {e}")
                return None, code, msg

        # ───────────── RTU (как было) ─────────────
        try:
            if p.register_type == "coil":
                raw = inst.read_bit(addr, functioncode=1)
            elif p.register_type == "discrete":
                raw = inst.read_bit(addr, functioncode=2)
            elif p.register_type in ("holding", "input"):
                fc = 3 if p.register_type == "holding" else 4
                # 16-битные — как раньше
                if (p.words <= 1) and (p.data_type in ("u16", "s16")):
                    raw = inst.read_register(addr, functioncode=fc, signed=(p.data_type == "s16"))
                else:
                    regs = inst.read_registers(addr, max(1, int(p.words or 2)), functioncode=fc)
                    raw = self._decode_words(regs, p.data_type or "u16", p.word_order or "AB")
            else:
                return None, 10, "CONFIG_ERROR"

            raw_v = raw
            scale = _safe_scale(p.scale)

            # сохраняем int без потери точности, если scale=1 и тип целочисленный
            is_int_type = str(p.data_type or "").lower() in ("u16", "s16", "u32", "s32", "u64", "s64")
            if is_int_type and isinstance(raw_v, (int,)) and float(scale) == 1.0:
                val = raw_v
            else:
                val = float(raw_v) * float(scale)
                val = _round_ndp(val, self.precision_decimals)

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
        # items = sorted(params, key=lambda x: (x.register_type, self._normalize_addr(x)))
        items = sorted(
            [pp for pp in params if int(getattr(pp, "words", 1) or 1) == 1],
            key=lambda x: (x.register_type, self._normalize_addr(x))
        )

        if not items:
            return []  # <<< FIX: все параметры multi-word, batch-blocks не делаем

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

    # ───────────────────────── публикация/heartbeat/«текущие» ─────────────────
    def _maybe_publish(
            self,
            nd: NodeCfg,
            p: ParamCfg,
            value: Optional[float],
            code: int,
            message: str,
            last_ok_ts: Optional[float],
            last_attempt_ts: Optional[float],
    ):
        """
        Публикация по режимам + обновление «текущих».
        При ошибке (code!=0) делаем heartbeat по интервалу (если он задан).
        Также даём «пульс» в current_store (touch_read) при успешном чтении без публикации.
        """
        key = self._make_key(nd.unit_id, p.name)
        now = _now_ms()
        last_val = self._last_values.get(key)
        last_pub = self._last_pub_ts.get(key, 0.0)

        mode = (p.publish_mode or "on_change").lower()

        interval_s = float(getattr(p, "publish_interval_s", 0) or 0)
        due_interval = interval_s > 0 and ((now - last_pub) >= interval_s)

        # changed = (value is not None) and (last_val is None or value != last_val)
        if value is not None:
            if p.register_type in ("coil", "discrete"):
                changed = (value is not None) and (last_val is None or int(value) != int(last_val))
            else:
                step = float(p.step) if p.step not in (None, "") else 0.0
                hyst = float(p.hysteresis) if p.hysteresis not in (None, "") else 0.0
                changed = self._analog_changed(key, float(value), step, hyst) if (step > 0 or hyst > 0) \
                    else ((last_val is None) or (value != last_val))
        else:
            changed = False

        topic_like = self._pub_topic_for(nd, p)
        base_ctx = self._ctx(nd, p)
        no_reply = int(self._no_reply.get(nd.unit_id, 0))

        def pub(val, c, msg, silent_s, trig: str):
            self._mqtt_publish(topic_like, val, c, msg, int(silent_s), base_ctx, trig, no_reply)
            self._last_pub_ts[key] = now

        if code == 0:
            did_publish = False
            # OK-путь
            if mode in ("on_change_and_interval", "both"):
                if changed:
                    pub(value, 0, "OK", 0, "change")
                    did_publish = True
                if due_interval:
                    pub(value, 0, "OK", 0, "interval")
                    did_publish = True
            elif mode == "on_change":
                if changed:
                    pub(value, 0, "OK", 0, "change")
                    did_publish = True
            elif mode == "interval":
                if due_interval:
                    pub(value, 0, "OK", 0, "interval")
                    did_publish = True
            else:
                if changed:
                    pub(value, 0, "OK", 0, "change")
                    did_publish = True

            # обновить last_values и «текущие»
            self._last_values[key] = value  # type: ignore
            state.update(
                self.name_,
                nd.unit_id,
                nd.object,
                p.name,
                topic_like if topic_like.startswith("/") else f"{getattr(self.mqtt, 'base', '/devices')}/{topic_like}",
                p.register_type,
                self._normalize_addr(p),
                value,
                0,
                "OK",
                last_ok_ts or now,
                last_attempt_ts or now,
            )

            # Если публикации не было — раз в N секунд обновляем «текущие» значением (без публикации в брокер)
            if not did_publish and self.touch_read_every_s > 0:
                last_touch = self._last_store_touch_ok.get(key, 0.0)
                if (now - last_touch) >= self.touch_read_every_s:
                    try:
                        current_store.apply_read(base_ctx, value, datetime.now(timezone.utc))
                    finally:
                        self._last_store_touch_ok[key] = now
            return

        # Ошибка: heartbeat в интервале
        silent_for = int(now - (last_ok_ts or now))
        if mode in ("on_change_and_interval", "both", "interval") and due_interval:
            pub(None, code, message, silent_for, "heartbeat")

        # «текущие» при ошибке
        state.update(
            self.name_,
            nd.unit_id,
            nd.object,
            p.name,
            topic_like if topic_like.startswith("/") else f"{getattr(self.mqtt, 'base', '/devices')}/{topic_like}",
            p.register_type,
            self._normalize_addr(p),
            None,
            code,
            message,
            last_ok_ts or 0.0,
            last_attempt_ts or now,
        )

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

            # 0) приоритетно выполняем накопившиеся записи
            self._drain_writes(max_tasks=100)

            loop_start = _now_ms()
            for nd in self.nodes:
                # Запомним текущий unit_id для TCP-операций
                self._current_unit_id = nd.unit_id

                inst = self._inst(nd.unit_id)
                if inst is None:
                    self._stats[(nd.unit_id, "read_err")] += 1
                    continue

                # Batch-read (если включено)
                if self.batch_enabled:
                    if self.transport != "tcp" and self.serial_echo:
                        try:
                            inst.serial.reset_input_buffer()
                        except Exception:
                            pass

                    blocks = self._group_sequential(nd.params)
                    if self.serial_echo:
                        try:
                            inst.serial.reset_input_buffer()
                        except Exception:
                            pass

                    for rtype, start, count, group_params in blocks:
                        if self.debug.get("enabled"):
                            fc = _func_code(rtype)
                            # первый «сырой» адрес из YAML для наглядности
                            raw_first = group_params[0].address if group_params else start
                            self.log.debug(
                                f"REQ unit={nd.unit_id} fc={fc} type={rtype} "
                                f"start={start} count={count} (yaml_first={raw_first})"
                            )
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

                            if self.debug.get("enabled"):
                                self.log.debug(
                                    f"RSP unit={nd.unit_id} fc={_func_code(rtype)} "
                                    f"values={len(block_vals)} ok"
                                )

                            # успех блока
                            for i, p in enumerate(group_params):
                                raw = block_vals[i]

                                # привести типы так же, как в _read_single()
                                dt = (p.data_type or "u16").lower()

                                if rtype in ("coil", "discrete"):
                                    raw_v = 1 if bool(raw) else 0
                                elif dt == "s16":
                                    raw_v = int(raw) & 0xFFFF
                                    if raw_v & 0x8000:
                                        raw_v -= 0x10000
                                else:
                                    raw_v = int(raw) & 0xFFFF  # u16 по умолчанию

                                val = float(raw_v) * _safe_scale(p.scale)
                                val = _round_ndp(val, self.precision_decimals)

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
                                fc = _func_code(rtype)
                                self.log.error(
                                    f"block read error unit={nd.unit_id} fc={fc} type={rtype} "
                                    f"range=[{start}..{start + count - 1}] port={self.port} → {e}"
                                )
                                # небольшая подсказка при частом 0x02/0x10:
                                self.log.error(
                                    "hint: Illegal Data Address чаще всего = не тот адрес/диапазон. "
                                    "Проверь normalize-правило и off-by-one (например 1001 vs 1000), "
                                    "а также лимиты batch_read.max_bits/regs."
                                )

                            self._no_reply[nd.unit_id] = self._no_reply.get(nd.unit_id, 0) + 1
                            for i, p in enumerate(group_params):
                                key = self._make_key(nd.unit_id, p.name)
                                now = _now_ms()
                                self._last_attempt_ts[key] = now
                                self._stats[(nd.unit_id, "read_err")] += 1
                                self._maybe_publish(nd, p, None, code, msg, self._last_ok_ts.get(key), now)
                            if self._no_reply[nd.unit_id] >= max_err:
                                time.sleep(backoff_ms)
                    # дочитываем параметры с words > 1 одиночными чтениями
                    for p in (pp for pp in nd.params if int(getattr(pp, "words", 1) or 1) > 1):
                        key = self._make_key(nd.unit_id, p.name)

                        if self.debug.get("enabled"):
                            fc = _func_code(p.register_type)
                            addr_norm = self._normalize_addr(p)
                            self.log.debug(
                                f"REQ unit={nd.unit_id} fc={fc} type={p.register_type} "
                                f"addr={addr_norm} (yaml={p.address}) [multi-word]"
                            )

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

                    continue  # следующий узел

                # Одиночные чтения (если batch выключен)
                if self.transport != "tcp" and self.serial_echo:
                    try:
                        inst.serial.reset_input_buffer()
                    except Exception:
                        pass
                if self.serial_echo:
                    try:
                        inst.serial.reset_input_buffer()
                    except Exception:
                        pass
                for p in nd.params:

                    if self.debug.get("enabled"):
                        fc = _func_code(p.register_type)
                        addr_norm = self._normalize_addr(p)
                        self.log.debug(
                            f"REQ unit={nd.unit_id} fc={fc} type={p.register_type} "
                            f"addr={addr_norm} (yaml={p.address})"
                        )

                    key = self._make_key(nd.unit_id, p.name)
                    val, code, msg = self._read_single(inst, p)

                    if self.debug.get("enabled"):
                        if val is not None:
                            self.log.debug(
                                f"RSP unit={nd.unit_id} fc={_func_code(p.register_type)} val={val}"
                            )
                        else:
                            self.log.error(
                                f"single read error unit={nd.unit_id} fc={_func_code(p.register_type)} "
                                f"addr={self._normalize_addr(p)} (yaml={p.address}) → {msg}"
                            )

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
