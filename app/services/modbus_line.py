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

        fast_conf = (self.poll.get("fast_modbus") or {})

        self.fast_enabled = bool(fast_conf.get("enabled", False))
        self.fast_interval_s = float(fast_conf.get("interval_ms", 50) or 50) / 1000.0
        self.fast_bootstrap_pause_s = float(fast_conf.get("bootstrap_pause_ms", 20) or 20) / 1000.0
        self.fast_rx_gap_s = float(fast_conf.get("rx_gap_ms", 20) or 20) / 1000.0
        self.fast_rx_hard_timeout_s = float(fast_conf.get("rx_hard_timeout_ms", 350) or 350) / 1000.0
        self.fast_poll_idle_sleep_s = float(fast_conf.get("poll_idle_sleep_ms", 1000) or 1000) / 1000.0
        self.fast_poll_pending_sleep_s = float(fast_conf.get("poll_pending_sleep_ms", 200) or 200) / 1000.0
        self.fast_protocol_debug = bool(fast_conf.get("protocol_debug", False))
        self.fast_max_events_per_poll = int(fast_conf.get("max_events_per_poll", 200) or 200)

        self._fast_bootstrap_done = False
        self._fast_last_poll_ts = 0.0
        self._fast_confirm_slave_id = 0
        self._fast_confirm_flag = 0
        self._fast_next_poll_sleep_s = self.fast_poll_idle_sleep_s

        # карта подписанных блоков для декодирования обычных RTU-ответов,
        # пришедших в ответ на FAST POLL
        self._fast_blocks_by_sig: Dict[Tuple[int, int, int], List[Dict[str, Any]]] = defaultdict(list)
        self._fast_blocks_all: Dict[int, List[Dict[str, Any]]] = defaultdict(list)

        self.debug = {
            "enabled": bool(settings.debug.get("enabled", False)),
            "log_reads": bool(settings.debug.get("log_reads", False)),
            "summary_every_s": int(settings.debug.get("summary_every_s", 0)),
        }
        # задержка между Modbus-транзакциями (защита от "склейки" кадров / перегруза линии)
        self.inter_request_delay_s: float = float(self.poll.get("inter_request_delay_ms", 0) or 0) / 1000.0

        # retry при CRC (1 повтор по умолчанию)
        self.crc_retry_count: int = int(self.poll.get("crc_retry_count", 1) or 0)
        self.crc_retry_delay_s: float = float(self.poll.get("crc_retry_delay_ms", 30) or 0) / 1000.0

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

        # coalesce write (умная запись)
        self.write_coalesce_s: float = float(self.poll.get("write_coalesce_ms", 0) or 0) / 1000.0
        self.write_max_tasks: int = int(self.poll.get("write_max_tasks", 300) or 300)
        self.write_max_bits: int = int(self.poll.get("write_max_bits", self.batch_max_bits) or self.batch_max_bits)
        self.write_max_registers: int = int(self.poll.get("write_max_registers", self.batch_max_regs) or self.batch_max_regs)

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

        self._fast_pending: Dict[int, set[Tuple[str, int]]] = defaultdict(set)
        self._param_index: Dict[Tuple[int, str, int], ParamCfg] = {}

        # Быстрый индекс параметров: (unit_id, register_type, normalized_addr) -> ParamCfg
        for nd in self.nodes:
            for p in nd.params:
                try:
                    if int(getattr(p, "words", 1) or 1) != 1:
                        continue
                    addr_norm = self._normalize_addr(p)
                    self._param_index[(int(nd.unit_id), str(p.register_type), int(addr_norm))] = p
                except Exception:
                    pass

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

        # cooldown для unit_id после hard-error/timeout
        self._unit_skip_until: Dict[int, float] = {}

        # настройки деградации по неответам slave
        self.unit_error_skip_s: float = float(self.poll.get("unit_error_skip_s", 1.0) or 1.0)
        self.skip_node_on_timeout: bool = bool(self.poll.get("skip_node_on_timeout", True))

        # Подписка на команды записи (…/on) для rw-параметров
        self._setup_command_subscriptions()

        # Флаг остановки
        self._stop_event = threading.Event()

        self._bands: Dict[str, Tuple[float, float]] = {}  # key -> (low_adj, high_adj)

        self._io_lock = threading.RLock()  # единый лок на все транзакции порта
        self._write_q: "queue.Queue[dict]" = queue.Queue()  # очередь задач записи

        # Для TCP пока отключим batch-read, будем читать по одному параметру
        if self.transport == "tcp":
            self.batch_enabled = False

    # ───────────────────────── общая жизнедеятельность ─────────────────────────
    def stop(self):
        self._stop_event.set()
        self.log.info("stop requested")

        # RTU-инструменты
        for unit_id, inst in list(self._instruments.items()):
            try:
                ser = getattr(inst, "serial", None)
                if ser:
                    try:
                        ser.cancel_read()
                    except Exception:
                        pass
                    try:
                        ser.cancel_write()
                    except Exception:
                        pass
                    try:
                        ser.reset_input_buffer()
                    except Exception:
                        pass
                    try:
                        ser.reset_output_buffer()
                    except Exception:
                        pass
                    try:
                        ser.close()
                    except Exception:
                        pass
            except Exception:
                pass

        # TCP-клиент
        if self._tcp_client is not None:
            try:
                self._tcp_client.close()
            except Exception:
                pass

    def _mark_port_broken(
        self,
        reason: str,
        inst: Optional[minimalmodbus.Instrument] = None,
        unit_id: Optional[int] = None,
    ) -> None:
        """
        Централизованно помечаем COM-порт как сломанный, закрываем serial,
        удаляем instrument(ы) и включаем backoff на переоткрытие.
        """
        self._port_fault = True
        self._port_retry_at = _now_ms() + self._port_retry_backoff

        # 1) закрыть конкретный inst, если передали
        if inst is not None:
            try:
                ser = getattr(inst, "serial", None)
                if ser is not None:
                    ser.close()
            except Exception:
                pass

        # 2) если известен unit_id — убрать только его instrument
        if unit_id is not None:
            try:
                inst2 = self._instruments.pop(int(unit_id), None)
                if inst2 is not None:
                    try:
                        ser2 = getattr(inst2, "serial", None)
                        if ser2 is not None:
                            ser2.close()
                    except Exception:
                        pass
            except Exception:
                pass
        else:
            # 3) иначе закрыть и очистить всё по линии
            try:
                for uid, instx in list(self._instruments.items()):
                    try:
                        serx = getattr(instx, "serial", None)
                        if serx is not None:
                            serx.close()
                    except Exception:
                        pass
                self._instruments.clear()
            except Exception:
                pass

        self.log.warning(
            f"port marked broken [{self.port}] reason={reason}; "
            f"retry in {self._port_retry_backoff:.1f}s"
        )

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

        try:
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
        except serial.serialutil.SerialException as e:
            self._mark_port_broken(f"write serial exception: {e}", inst=inst, unit_id=unit_id)
            raise
        except OSError as e:
            self._mark_port_broken(f"write os error: {e}", inst=inst, unit_id=unit_id)
            raise

        self._no_reply[unit_id] = 0
        self._stats[(unit_id, "read_ok")] += 1  # условно считаем успешную операцию

        pub_value = 1.0 if (p.register_type == "coil" and raw != 0) else (0.0 if p.register_type == "coil" else float(num))
        self._publish_write_success(unit_id, p, pub_value)

    def _inter_delay(self):
        d = float(getattr(self, "inter_request_delay_s", 0.0) or 0.0)
        if d > 0:
            time.sleep(d)

    def _mark_unit_no_reply(self, unit_id: int, code: int, msg: str) -> None:
        """
        Помечает unit как проблемный.
        Для timeout/port busy/unknown error включаем короткий cooldown,
        чтобы не мучать slave до следующего круга.
        """
        self._no_reply[unit_id] = self._no_reply.get(unit_id, 0) + 1

        if code in (1, 7, 12):  # TIMEOUT / PORT_BUSY / UNKNOWN_ERROR
            self._unit_skip_until[unit_id] = _now_ms() + self.unit_error_skip_s

    def _unit_is_temporarily_skipped(self, unit_id: int) -> bool:
        until = float(self._unit_skip_until.get(unit_id, 0.0) or 0.0)
        return until > _now_ms()

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
        - trigger: 'change' | 'interval' | 'heartbeat' | 'write'
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

    # ───────────────────────── подписка на команды записи и группировки записи ──────────────────────
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

    def _find_node(self, unit_id: int) -> Optional[NodeCfg]:
        for nd in self.nodes:
            if int(nd.unit_id) == int(unit_id):
                return nd
        return None

    def _get_cached_value(self, unit_id: int, p: ParamCfg) -> Optional[float]:
        key = self._make_key(unit_id, p.name)
        return self._last_values.get(key)

    def _get_cached_raw_for_write(self, unit_id: int, p: ParamCfg) -> Optional[int]:
        """
        Возвращает текущее значение параметра в raw-виде для записи:
        - coil -> 0/1
        - holding(words=1) -> raw register value
        Если в кэше нет значения — None
        """
        v = self._get_cached_value(unit_id, p)
        if v is None:
            return None

        try:
            if p.register_type == "coil":
                return 1 if float(v) != 0 else 0

            if p.register_type == "holding":
                return int(round(float(v) / _safe_scale(p.scale))) & 0xFFFF

        except Exception:
            return None

        return None

    def _find_param_by_addr(self, unit_id: int, register_type: str, addr: int) -> Optional[ParamCfg]:
        return self._param_index.get((int(unit_id), str(register_type), int(addr)))

    def _push_fast_event(self, unit_id: int, register_type: str, addr: int) -> None:
        self.ingest_fast_event(unit_id, register_type, addr)

    def _drain_fast_reads(self, max_units: int = 2, max_blocks: int = 4) -> None:
        """
        Внеочередное дочитывание параметров, по которым пришли fast-события.
        """
        if not self._fast_pending:
            return

        handled_units = 0
        handled_blocks = 0

        for unit_id in list(self._fast_pending.keys()):
            if handled_units >= max_units or handled_blocks >= max_blocks:
                break

            pending = self._fast_pending.get(unit_id)
            if not pending:
                continue

            inst = self._inst(unit_id)
            if inst is None:
                continue

            nd = self._find_node(unit_id)
            if nd is None:
                self._fast_pending.pop(unit_id, None)
                continue

            # снимаем snapshot, чтобы новые события могли копиться параллельно
            items = list(pending)
            self._fast_pending[unit_id].difference_update(items)

            by_type: Dict[str, List[int]] = defaultdict(list)
            for rtype, addr in items:
                by_type[rtype].append(int(addr))

            for rtype, addrs in by_type.items():
                if handled_blocks >= max_blocks:
                    break

                # input можно оставить, но если хочешь только writable/event-driven:
                if rtype == "input":
                    continue

                unique_addrs = sorted(set(addrs))
                if not unique_addrs:
                    continue

                max_len = self.batch_max_bits if rtype in ("coil", "discrete") else self.batch_max_regs

                for start, end in self._split_ranges(unique_addrs, max_len):
                    if handled_blocks >= max_blocks:
                        break

                    count = end - start + 1

                    # ---- FAST REQ ----
                    self.log.info(
                        f"FAST REQ unit={unit_id} fc={_func_code(rtype)} "
                        f"type={rtype} start={start} count={count}"
                    )

                    try:
                        with self._io_lock:
                            block_vals = self._read_block(inst, rtype, start, count)

                        # ---- FAST RSP ----
                        self.log.info(
                            f"FAST RSP unit={unit_id} fc={_func_code(rtype)} "
                            f"values={len(block_vals)} ok"
                        )

                        for i in range(count):
                            addr = start + i
                            p = self._find_param_by_addr(unit_id, rtype, addr)
                            if p is None:
                                continue

                            raw = block_vals[i]
                            dt = (p.data_type or "u16").lower()

                            if rtype in ("coil", "discrete"):
                                raw_v = 1 if bool(raw) else 0
                            elif dt == "s16":
                                raw_v = int(raw) & 0xFFFF
                                if raw_v & 0x8000:
                                    raw_v -= 0x10000
                            else:
                                raw_v = int(raw) & 0xFFFF

                            val = float(raw_v) * _safe_scale(p.scale)
                            val = _round_ndp(val, self.precision_decimals)

                            key = self._make_key(unit_id, p.name)
                            now = _now_ms()
                            self._last_ok_ts[key] = now
                            self._last_attempt_ts[key] = now
                            self._no_reply[unit_id] = 0
                            self._stats[(unit_id, "read_ok")] += 1

                            self._maybe_publish(nd, p, val, 0, "OK", now, now)

                        handled_blocks += 1
                        self._inter_delay()

                    except Exception as e:
                        code, msg = self._map_ex(e)

                        if self.debug.get("enabled"):
                            self.log.error(
                                f"fast read error unit={unit_id} fc={_func_code(rtype)} type={rtype} "
                                f"range=[{start}..{end}] port={self.port} → {e}"
                            )

                        # при ошибке возвращаем адреса обратно в pending, чтобы попробовать позже
                        for addr in unique_addrs:
                            if start <= addr <= end:
                                self._fast_pending[unit_id].add((rtype, addr))

                        # no_reply тут лучше не раздувать сильно, но единично можно
                        self._no_reply[unit_id] = self._no_reply.get(unit_id, 0) + 1

            handled_units += 1

    def _publish_write_success(self, unit_id: int, p: ParamCfg, value: float) -> None:
        nd = self._find_node(unit_id)
        if nd is None:
            return

        topic_like = self._pub_topic_for(nd, p)
        ctx = self._ctx(nd, p)
        now = _now_ms()
        key = self._make_key(unit_id, p.name)

        # кэши
        self._last_values[key] = value
        self._last_ok_ts[key] = now
        self._last_attempt_ts[key] = now
        self._last_pub_ts[key] = now
        self._no_reply[int(unit_id)] = 0

        # MQTT (trigger=write)
        self._mqtt_publish(topic_like, value, 0, "OK", 0, ctx, "write", 0)

        # state/current_store — чтобы UI обновился мгновенно
        try:
            state.update(
                self.name_,
                int(unit_id),
                nd.object,
                p.name,
                topic_like if topic_like.startswith("/") else f"{getattr(self.mqtt, 'base', '/devices')}/{topic_like}",
                p.register_type,
                self._normalize_addr(p),
                value,
                0,
                "OK",
                now,
                now,
            )
        except Exception:
            pass

        try:
            current_store.apply_read(ctx, value, datetime.now(timezone.utc))
        except Exception:
            pass

    def _collect_write_tasks(self) -> List[dict]:
        tasks: List[dict] = []
        try:
            first = self._write_q.get_nowait()
        except queue.Empty:
            return tasks

        tasks.append(first)

        deadline = time.time() + max(0.0, self.write_coalesce_s)
        while len(tasks) < self.write_max_tasks:
            timeout = max(0.0, deadline - time.time())
            if timeout <= 0:
                break
            try:
                t = self._write_q.get(timeout=timeout)
                tasks.append(t)
            except queue.Empty:
                break

        return tasks

    def _split_ranges(self, addrs: List[int], max_len: int) -> List[Tuple[int, int]]:
        if not addrs:
            return []
        addrs = sorted(addrs)
        out: List[Tuple[int, int]] = []
        s = addrs[0]
        prev = s
        for a in addrs[1:]:
            if a == prev + 1 and (a - s + 1) <= max_len:
                prev = a
            else:
                out.append((s, prev))
                s = a
                prev = a
        out.append((s, prev))
        return out

    def _write_coils_range(self, inst: minimalmodbus.Instrument, start: int, bits01: List[int]) -> None:
        # Пытаемся FC15 (write multiple coils)
        if hasattr(inst, "write_bits"):
            inst.write_bits(start, [1 if int(b) else 0 for b in bits01])
            return

        # fallback: по одному FC5
        for i, b in enumerate(bits01):
            inst.write_bit(start + i, 1 if int(b) else 0, functioncode=5)

    def _execute_write_tasks_smart(self, tasks: List[dict]) -> None:

        if self.transport == "tcp":
            for t in tasks:
                try:
                    self._do_write_task(t)
                except Exception as e:
                    p: ParamCfg = t.get("param")  # type: ignore
                    self.log.error(f"write error {t.get('unit_id')}/{getattr(p, 'name', '?')}: {e}")
            return

        if not tasks:
            return

        # группируем по unit
        by_unit: Dict[int, List[dict]] = defaultdict(list)
        for t in tasks:
            try:
                by_unit[int(t["unit_id"])].append(t)
            except Exception:
                continue

        for unit_id, unit_tasks in by_unit.items():
            inst = self._inst(unit_id)
            if inst is None:
                for t in unit_tasks:
                    p: ParamCfg = t.get("param")
                    if p:
                        self.log.error(f"write skipped: serial not ready unit={unit_id}/{p.name}")
                continue

            # отделяем по типам
            coil_tasks: List[dict] = []
            hold_tasks: List[dict] = []
            other_tasks: List[dict] = []

            for t in unit_tasks:
                p: ParamCfg = t.get("param")
                if not p:
                    continue
                rt = str(p.register_type)
                if rt == "coil":
                    coil_tasks.append(t)
                elif rt == "holding" and int(getattr(p, "words", 1) or 1) == 1:
                    hold_tasks.append(t)
                else:
                    other_tasks.append(t)

            # 1) всё нестандартное — по старинке одиночными
            for t in other_tasks:
                try:
                    self._do_write_task(t)  # у тебя уже “write==success”
                except Exception as e:
                    p: ParamCfg = t.get("param")  # type: ignore
                    self.log.error(f"write error {unit_id}/{getattr(p,'name','?')}: {e}")

            # 2) COIL — основной кейс
            if coil_tasks:
                # addr -> (ParamCfg, desired01) (последняя команда выигрывает)
                desired: Dict[int, Tuple[ParamCfg, int]] = {}
                for t in coil_tasks:
                    p: ParamCfg = t["param"]
                    v = float(t.get("value_num", 0.0))
                    desired[self._normalize_addr(p)] = (p, 1 if v != 0 else 0)

                addrs = sorted(desired.keys())
                for start, end in self._split_ranges(addrs, self.write_max_bits):

                    with self._io_lock:
                        out: List[int] = []
                        missing_cache = False

                        for a in range(start, end + 1):
                            if a in desired:
                                # новое значение пришло командой
                                _, new01 = desired[a]
                                out.append(int(new01))
                                continue

                            # "дыра" — пробуем взять из кэша
                            p_gap = self._find_param_by_addr(unit_id, "coil", a)
                            if p_gap is None:
                                missing_cache = True
                                break

                            raw_cur = self._get_cached_raw_for_write(unit_id, p_gap)
                            if raw_cur is None:
                                missing_cache = True
                                break

                            out.append(1 if int(raw_cur) else 0)

                        if missing_cache:
                            if self.debug.get("enabled"):
                                self.log.debug(
                                    f"skip coil block write unit={unit_id} [{start}..{end}] "
                                    f"because cache is incomplete"
                                )
                            # fallback: пишем только адреса, которые реально пришли
                            for a in range(start, end + 1):
                                if a not in desired:
                                    continue
                                p, new01 = desired[a]
                                cur_raw = self._get_cached_raw_for_write(unit_id, p)
                                if cur_raw is not None and int(cur_raw) == int(new01):
                                    self._publish_write_success(unit_id, p, float(new01))
                                    continue
                                try:
                                    self._do_write_task({
                                        "unit_id": unit_id,
                                        "param": p,
                                        "value_num": float(new01),
                                    })
                                except Exception as e:
                                    self.log.error(f"write error {unit_id}/{p.name}: {e}")
                            continue

                        changed_any = False
                        for a in range(start, end + 1):
                            if a not in desired:
                                continue
                            p, new01 = desired[a]
                            idx = a - start
                            p_cur = self._find_param_by_addr(unit_id, "coil", a)
                            cur_raw = self._get_cached_raw_for_write(unit_id, p_cur) if p_cur else None

                            if cur_raw is not None and int(cur_raw) == int(new01):
                                self._publish_write_success(unit_id, p, float(new01))
                            else:
                                changed_any = True
                                out[idx] = int(new01)

                        if not changed_any:
                            continue

                        try:
                            self._write_coils_range(inst, start, out)
                        except Exception as e:
                            self.log.error(f"write block coils error unit={unit_id} [{start}..{end}]: {e}")
                            continue

                    # после успешной пачки — публикуем успехи для адресов, которые реально пришли командами
                    for a in range(start, end + 1):
                        if a not in desired:
                            continue
                        p, new01 = desired[a]
                        self._publish_write_success(unit_id, p, float(new01))

            # 3) HOLDING words=1 — FC16
            if hold_tasks:
                desired_h: Dict[int, Tuple[ParamCfg, float]] = {}
                for t in hold_tasks:
                    p: ParamCfg = t["param"]
                    v_num = float(t.get("value_num", 0.0))
                    desired_h[self._normalize_addr(p)] = (p, v_num)

                addrs = sorted(desired_h.keys())
                for start, end in self._split_ranges(addrs, self.write_max_registers):

                    with self._io_lock:
                        out_regs: List[int] = []
                        missing_cache = False

                        for a in range(start, end + 1):
                            if a in desired_h:
                                p, v_num = desired_h[a]
                                raw_new = int(round(float(v_num) / _safe_scale(p.scale))) & 0xFFFF
                                out_regs.append(raw_new)
                                continue

                            p_gap = self._find_param_by_addr(unit_id, "holding", a)
                            if p_gap is None:
                                missing_cache = True
                                break

                            raw_cur = self._get_cached_raw_for_write(unit_id, p_gap)
                            if raw_cur is None:
                                missing_cache = True
                                break

                            out_regs.append(int(raw_cur) & 0xFFFF)

                        if missing_cache:
                            if self.debug.get("enabled"):
                                self.log.debug(
                                    f"skip holding block write unit={unit_id} [{start}..{end}] "
                                    f"because cache is incomplete"
                                )
                            # fallback: по одному
                            for a in range(start, end + 1):
                                if a not in desired_h:
                                    continue
                                p, v_num = desired_h[a]
                                cur_raw = self._get_cached_raw_for_write(unit_id, p)
                                raw_new = int(round(float(v_num) / _safe_scale(p.scale))) & 0xFFFF

                                if cur_raw is not None and int(cur_raw) == raw_new:
                                    self._publish_write_success(unit_id, p, float(v_num))
                                    continue

                                try:
                                    self._do_write_task({
                                        "unit_id": unit_id,
                                        "param": p,
                                        "value_num": float(v_num),
                                    })
                                except Exception as e:
                                    self.log.error(f"write error {unit_id}/{p.name}: {e}")
                            continue

                        changed_any = False
                        for a in range(start, end + 1):
                            if a not in desired_h:
                                continue
                            p, v_num = desired_h[a]
                            idx = a - start
                            raw_new = int(round(float(v_num) / _safe_scale(p.scale))) & 0xFFFF

                            p_cur = self._find_param_by_addr(unit_id, "holding", a)
                            cur_raw = self._get_cached_raw_for_write(unit_id, p_cur) if p_cur else None

                            if cur_raw is not None and int(cur_raw) == raw_new:
                                self._publish_write_success(unit_id, p, float(v_num))
                            else:
                                changed_any = True
                                out_regs[idx] = raw_new

                        if not changed_any:
                            continue

                        try:
                            inst.write_registers(start, out_regs)
                        except Exception as e:
                            self.log.error(f"write block holding error unit={unit_id} [{start}..{end}]: {e}")
                            continue

                    for a in range(start, end + 1):
                        if a not in desired_h:
                            continue
                        p, v_num = desired_h[a]
                        self._publish_write_success(unit_id, p, float(v_num))



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
            try:
                ser = getattr(inst, "serial", None)
                if ser is not None and getattr(ser, "is_open", True):
                    return inst
            except Exception:
                pass

            try:
                self._instruments.pop(unit_id, None)
            except Exception:
                pass

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
            inst.serial.write_timeout = max(0.2, float(self.timeout))
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
    def _read_single(self, unit_id: int, inst, p: ParamCfg) -> Tuple[Optional[float], int, str]:
        addr = self._normalize_addr(p)

        # ───────────── TCP ─────────────
        if self.transport == "tcp":

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

            s = str(e).lower()
            if (
                ("неверный дескриптор" in s)
                or ("bad file descriptor" in s)
                or ("writefile failed" in s)
                or ("device not functioning" in s)
                or ("i/o error" in s)
            ):
                self._mark_port_broken(reason=s, inst=inst, unit_id=unit_id)

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

    def _read_block(self, inst: minimalmodbus.Instrument, rtype: str, start: int, count: int) -> List[int]:
        if rtype == "coil":
            return self._read_block_bits(inst, start, count, functioncode=1)
        if rtype == "discrete":
            return self._read_block_bits(inst, start, count, functioncode=2)
        if rtype == "holding":
            return self._read_block_regs(inst, start, count, functioncode=3)
        if rtype == "input":
            return self._read_block_regs(inst, start, count, functioncode=4)
        raise ValueError(f"unknown register_type: {rtype}")

    def _is_fast_eligible_param(self, p: ParamCfg) -> bool:
        """
        Можно ли использовать параметр в fast-событиях.
        Пока берём только single-word и не input.
        """
        try:
            if int(getattr(p, "words", 1) or 1) != 1:
                return False
            if str(p.register_type) == "input":
                return False
            return True
        except Exception:
            return False

    def ingest_fast_event(self, unit_id: int, register_type: str, addr: int) -> bool:
        """
        Принять одно внешнее fast-событие и поставить в очередь внеочередного дочитывания.
        addr ожидается уже в normalized-виде.
        """
        try:
            rt = str(register_type)

            if rt not in ("coil", "discrete", "holding", "input"):
                return False

            p = self._find_param_by_addr(unit_id, rt, int(addr))
            if p is None:
                return False

            if not self._is_fast_eligible_param(p):
                return False

            self.log.info(
                f"FAST EVENT unit={unit_id} type={register_type} addr={addr}"
            )

            self._fast_pending[int(unit_id)].add((rt, int(addr)))
            return True
        except Exception:
            return False

    def ingest_fast_events(self, events: List[dict]) -> int:
        """
        Принять пачку fast-событий.
        Формат элемента:
          {
            "unit_id": 44,
            "register_type": "coil",
            "addr": 1002
          }

        addr должен быть уже normalized.
        Возвращает число реально принятых событий.
        """
        accepted = 0

        for ev in events:
            try:
                unit_id = int(ev["unit_id"])
                register_type = str(ev["register_type"])
                addr = int(ev["addr"])

                if self.ingest_fast_event(unit_id, register_type, addr):
                    accepted += 1
            except Exception:
                continue

        return accepted

    def get_fast_subscription_map(self) -> Dict[int, Dict[str, List[int]]]:
        """
        Карта fast-подписок:
        {
          unit_id: {
            "coil": [...],
            "discrete": [...],
            "holding": [...],
          }
        }
        Только fast-eligible параметры.
        """
        out: Dict[int, Dict[str, List[int]]] = {}

        for nd in self.nodes:
            per_type: Dict[str, List[int]] = defaultdict(list)

            for p in nd.params:
                if not self._is_fast_eligible_param(p):
                    continue

                rtype = str(p.register_type)
                if rtype == "input":
                    continue

                per_type[rtype].append(int(self._normalize_addr(p)))

            clean = {
                rtype: sorted(set(addrs))
                for rtype, addrs in per_type.items()
                if addrs
            }

            if clean:
                out[int(nd.unit_id)] = clean

        return out

    # ───────────────────────── FAST MODBUS (single-thread) ─────────────────────

    def _fast_build_cfg_events_frame(self, slave_id: int, per_type: Dict[str, List[int]]) -> Optional[bytes]:
        REGTYPE_TO_ID = {"coil": 1, "discrete": 2, "holding": 3, "input": 4}
        CMD_EXT = 0x46
        SUB_CFG = 0x18
        PRIORITY_HIGH = 2

        blocks = []

        for rtype in ("coil", "discrete", "holding"):
            addrs = sorted(set(int(a) for a in (per_type.get(rtype) or [])))
            if not addrs:
                continue

            start = addrs[0]
            prev = start
            settings = [PRIORITY_HIGH]

            for a in addrs[1:]:
                if a == prev + 1:
                    settings.append(PRIORITY_HIGH)
                    prev = a
                else:
                    blocks.append((rtype, start, settings))
                    start = prev = a
                    settings = [PRIORITY_HIGH]

            blocks.append((rtype, start, settings))

        if not blocks:
            return None

        payload = bytearray()

        for rtype, start, settings in blocks:
            type_id = REGTYPE_TO_ID[rtype]
            count = len(settings)
            payload += struct.pack(">BHB", type_id, start & 0xFFFF, count)
            payload += bytes(settings)

        frame_wo_crc = bytes([slave_id, CMD_EXT, SUB_CFG, len(payload)]) + bytes(payload)
        return self._fast_add_crc(frame_wo_crc)

    def _fast_build_poll_frame(self, confirm_slave_id: int, confirm_flag: int) -> bytes:
        BCAST_EVENTS_SLAVE = 0xFD
        CMD_EXT = 0x46
        SUB_POLL = 0x10

        frame_wo_crc = bytes([
            BCAST_EVENTS_SLAVE,
            CMD_EXT,
            SUB_POLL,
            0,
            200,
            confirm_slave_id & 0xFF,
            confirm_flag & 0xFF,
        ])
        return self._fast_add_crc(frame_wo_crc)

    def _fast_crc16(self, data: bytes) -> int:
        crc = 0xFFFF
        for b in data:
            crc ^= b
            for _ in range(8):
                if crc & 1:
                    crc = (crc >> 1) ^ 0xA001
                else:
                    crc >>= 1
        return crc & 0xFFFF

    def _fast_add_crc(self, frame_wo_crc: bytes) -> bytes:
        c = self._fast_crc16(frame_wo_crc)
        return frame_wo_crc + struct.pack("<H", c)

    def _fast_check_crc(self, frame: bytes) -> bool:
        if len(frame) < 4:
            return False
        body, got = frame[:-2], frame[-2:]
        return got == struct.pack("<H", self._fast_crc16(body))

    def _fast_no_events_frame(self) -> bytes:
        return bytes.fromhex("fd4612525d")

    def _fast_pick_serial(self):
        for inst in self._instruments.values():
            ser = getattr(inst, "serial", None)
            if ser is not None:
                return ser
        return None

    def _fast_read_blob(self, ser, gap_s: float, hard_timeout_s: float, max_bytes: int = 4096) -> bytes:
        t0 = time.time()
        buf = bytearray()
        last_rx = None

        while True:
            now = time.time()

            if (now - t0) >= hard_timeout_s:
                break

            # Сначала проверяем, есть ли что читать, чтобы не зависать лишний раз в read(1)
            try:
                waiting = int(getattr(ser, "in_waiting", 0) or 0)
            except Exception:
                waiting = 0

            if waiting <= 0:
                # если уже что-то приняли и выдержали gap — считаем пакет завершённым
                if buf and last_rx and (now - last_rx) >= gap_s:
                    break

                time.sleep(0.001)
                continue

            try:
                chunk = ser.read(min(waiting, max(1, max_bytes - len(buf))))
            except serial.serialutil.SerialException as e:
                self._mark_port_broken(f"fast read serial exception: {e}")
                break
            except OSError as e:
                self._mark_port_broken(f"fast read os error: {e}")
                break
            except Exception as e:
                # Не всякая ошибка = смерть порта, но логируем
                self.log.error(f"FAST read blob error: {e}")
                break

            now = time.time()

            if chunk:
                buf += chunk
                last_rx = now
                if len(buf) >= max_bytes:
                    break
                continue

            if buf and last_rx and (now - last_rx) >= gap_s:
                break

        return bytes(buf)

    def _fast_cfg_expected_len(self, frame_prefix: bytes) -> Optional[int]:
        fp = frame_prefix.lstrip(b"\xFF")
        if len(fp) < 4:
            return None
        if len(fp) < 4:
            return None
        paylen = fp[3]
        return (len(frame_prefix) - len(fp)) + 4 + paylen + 2

    def _fast_split_frames(self, blob: bytes) -> List[bytes]:
        """
        Разбивает blob на:
        1) special FAST no-events frame: fd 46 12 ...
        2) special FAST events frame: <slave> 46 11 flag pending paylen payload crc
        3) обычные Modbus RTU response кадры
        """
        out: List[bytes] = []
        i = 0
        n = len(blob)

        while i < n:
            while i < n and blob[i] == 0xFF:
                i += 1
            if i >= n:
                break

            if i + 3 > n:
                break

            # FAST special frames
            if blob[i + 1] == 0x46:
                sub = blob[i + 2]

                # NO_EVENTS: fd 46 12 crc_lo crc_hi
                if sub == 0x12:
                    need = 5
                    if i + need <= n:
                        out.append(blob[i:i + need])
                        i += need
                        continue
                    break

                # EVENTS: slave 46 11 flag pending paylen payload crc_lo crc_hi
                if sub == 0x11:
                    if i + 6 > n:
                        break
                    paylen = blob[i + 5]
                    need = 6 + paylen + 2
                    if i + need <= n:
                        out.append(blob[i:i + need])
                        i += need
                        continue
                    break

                # CFG response: slave 46 18 len payload crc_lo crc_hi
                if sub == 0x18:
                    if i + 4 > n:
                        break
                    paylen = blob[i + 3]
                    need = 4 + paylen + 2
                    if i + need <= n:
                        out.append(blob[i:i + need])
                        i += need
                        continue
                    break

            slave = blob[i]
            fc = blob[i + 1]

            if fc & 0x80:
                need = 5
                if i + need <= n:
                    out.append(blob[i:i + need])
                    i += need
                    continue
                break

            if fc in (1, 2, 3, 4):
                bytecount = blob[i + 2]
                need = 3 + bytecount + 2
                if i + need <= n:
                    out.append(blob[i:i + need])
                    i += need
                    continue
                break

            if fc in (5, 6, 15, 16):
                need = 8
                if i + need <= n:
                    out.append(blob[i:i + need])
                    i += need
                    continue
                break

            i += 1

        return out

    def _fast_register_block_signature(self, unit_id: int, rtype: str, start: int, count: int):
        fc = _func_code(rtype)
        bytecount = ((count + 7) // 8) if rtype in ("coil", "discrete") else (count * 2)

        block = {
            "unit_id": int(unit_id),
            "register_type": str(rtype),
            "start": int(start),
            "count": int(count),
            "bytecount": int(bytecount),
        }

        sig = (int(unit_id), int(fc), int(bytecount))
        self._fast_blocks_by_sig[sig].append(block)
        self._fast_blocks_all[int(unit_id)].append(block)

    def _fast_prepare_block_index(self) -> None:
        self._fast_blocks_by_sig.clear()
        self._fast_blocks_all.clear()

        subs = self.get_fast_subscription_map()
        for unit_id, per_type in subs.items():
            for rtype in ("coil", "discrete", "holding"):
                addrs = sorted(set(int(a) for a in (per_type.get(rtype) or [])))
                if not addrs:
                    continue

                start = addrs[0]
                prev = start

                for a in addrs[1:]:
                    if a != prev + 1:
                        count = prev - start + 1
                        self._fast_register_block_signature(unit_id, rtype, start, count)
                        start = a
                    prev = a

                count = prev - start + 1
                self._fast_register_block_signature(unit_id, rtype, start, count)

    def _fast_bootstrap_once(self) -> None:
        if not self.fast_enabled or self.transport == "tcp" or self._fast_bootstrap_done:
            return

        ser = self._fast_pick_serial()
        if ser is None:
            return

        self._fast_prepare_block_index()
        subs = self.get_fast_subscription_map()
        if not subs:
            self._fast_bootstrap_done = True
            self.log.info("FAST bootstrap: no eligible params")
            return

        total_units = len(subs)
        total_regs = sum(len(addrs) for per_type in subs.values() for addrs in per_type.values())
        self.log.info(f"FAST bootstrap subscriptions prepared: units={total_units} regs={total_regs}")

        for unit_id, per_type in subs.items():
            self.log.info(f"FAST bootstrap unit={unit_id} map={per_type}")

            frame = self._fast_build_cfg_events_frame(unit_id, per_type)
            if frame is None:
                continue

            with self._io_lock:
                try:
                    ser.reset_input_buffer()
                except Exception:
                    pass

                try:
                    ser.write(frame)
                    ser.flush()
                    if self.fast_protocol_debug:
                        self.log.info(f"FAST CFG unit={unit_id} TX: {frame.hex(' ')}")
                except serial.serialutil.SerialException as e:
                    self._mark_port_broken(f"fast cfg serial exception: {e}", unit_id=unit_id)
                    self.log.error(f"FAST CFG unit={unit_id} write error: {e}")
                    return
                except OSError as e:
                    self._mark_port_broken(f"fast cfg os error: {e}", unit_id=unit_id)
                    self.log.error(f"FAST CFG unit={unit_id} write error: {e}")
                    return
                except Exception as e:
                    self.log.error(f"FAST CFG unit={unit_id} write error: {e}")
                    continue

                resp = self._fast_read_blob(
                    ser,
                    gap_s=self.fast_rx_gap_s,
                    hard_timeout_s=self.fast_rx_hard_timeout_s,
                    max_bytes=256,
                )

                if self.fast_protocol_debug:
                    self.log.info(
                        f"FAST CFG unit={unit_id} RX: {resp.hex(' ') if resp else '<empty>'}"
                    )

            # CFG resp: unit 46 18 len payload crc
            ok = False
            if resp:
                fp = resp.lstrip(b"\xFF")
                if len(fp) >= 6 and fp[0] == (unit_id & 0xFF) and fp[1] == 0x46 and fp[2] == 0x18:
                    exp_len = 4 + fp[3] + 2
                    if len(fp) >= exp_len and self._fast_check_crc(fp[:exp_len]):
                        ok = True

            if ok:
                self.log.info(f"FAST CFG unit={unit_id}: OK")
            else:
                self.log.warning(f"FAST CFG unit={unit_id}: no/invalid response")

            if self.fast_bootstrap_pause_s > 0:
                time.sleep(self.fast_bootstrap_pause_s)

        self._fast_bootstrap_done = True
        self._fast_last_poll_ts = 0.0
        self._fast_next_poll_sleep_s = self.fast_poll_idle_sleep_s

    def _fast_decode_data_to_events(
        self,
        unit_id: int,
        rtype: str,
        start: int,
        count: int,
        data: bytes,
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []

        if rtype in ("coil", "discrete"):
            for i in range(count):
                byte_i = i // 8
                bit_i = i % 8
                if byte_i >= len(data):
                    break
                val = 1 if (data[byte_i] >> bit_i) & 0x01 else 0

                p = self._find_param_by_addr(unit_id, rtype, start + i)
                if p is None:
                    continue

                old = self._get_cached_value(unit_id, p)
                if old is None or int(old) != int(val):
                    out.append({
                        "unit_id": unit_id,
                        "register_type": rtype,
                        "addr": start + i,
                    })

        elif rtype == "holding":
            regs = []
            for i in range(0, len(data), 2):
                if i + 1 >= len(data):
                    break
                regs.append((data[i] << 8) | data[i + 1])

            for i in range(min(count, len(regs))):
                addr = start + i
                p = self._find_param_by_addr(unit_id, rtype, addr)
                if p is None:
                    continue

                raw = regs[i]
                dt = (p.data_type or "u16").lower()
                if dt == "s16":
                    raw_v = raw & 0xFFFF
                    if raw_v & 0x8000:
                        raw_v -= 0x10000
                else:
                    raw_v = raw & 0xFFFF

                val = _round_ndp(float(raw_v) * _safe_scale(p.scale), self.precision_decimals)
                old = self._get_cached_value(unit_id, p)

                changed = False
                if old is None:
                    changed = True
                else:
                    step = float(p.step) if p.step not in (None, "") else 0.0
                    hyst = float(p.hysteresis) if p.hysteresis not in (None, "") else 0.0
                    if step > 0 or hyst > 0:
                        changed = self._analog_changed(self._make_key(unit_id, p.name), float(val), step, hyst)
                    else:
                        changed = float(old) != float(val)

                if changed:
                    out.append({
                        "unit_id": unit_id,
                        "register_type": rtype,
                        "addr": addr,
                    })

        return out

    def _fast_decode_rtu_response_frame(self, frame: bytes) -> List[Dict[str, Any]]:
        if frame == self._fast_no_events_frame():
            return []

        if not self._fast_check_crc(frame):
            if self.fast_protocol_debug:
                self.log.info(f"FAST bad crc: {frame.hex(' ')}")
            return []

        if len(frame) < 5:
            return []

        unit_id = frame[0]
        fc = frame[1]

        if fc not in (1, 2, 3, 4):
            return []

        bytecount = frame[2]
        data = frame[3:3 + bytecount]
        sig = (int(unit_id), int(fc), int(bytecount))
        candidates = self._fast_blocks_by_sig.get(sig) or []

        if not candidates:
            if self.fast_protocol_debug:
                self.log.info(
                    f"FAST unmatched frame unit={unit_id} fc={fc} bytecount={bytecount} raw={frame.hex(' ')}"
                )
            return []

        # если кандидатов несколько — перестраховываемся и генерим события по всем
        # на практике почти всегда будет 1 кандидат
        out: List[Dict[str, Any]] = []
        for block in candidates:
            out.extend(
                self._fast_decode_data_to_events(
                    unit_id=block["unit_id"],
                    rtype=block["register_type"],
                    start=block["start"],
                    count=block["count"],
                    data=data,
                )
            )
        return out

    def _fast_poll_once(self) -> List[Dict[str, Any]]:
        if not self.fast_enabled or self.transport == "tcp" or not self._fast_bootstrap_done:
            return []

        now = _now_ms()
        if (now - self._fast_last_poll_ts) < max(self.fast_interval_s, self._fast_next_poll_sleep_s):
            return []

        ser = self._fast_pick_serial()
        if ser is None:
            return []

        confirm_slave_id = self._fast_confirm_slave_id
        confirm_flag = self._fast_confirm_flag

        frame = self._fast_build_poll_frame(
            confirm_slave_id=confirm_slave_id,
            confirm_flag=confirm_flag,
        )

        blob = b""
        with self._io_lock:
            try:
                ser.reset_input_buffer()
            except Exception:
                pass

            try:
                ser.write(frame)
                ser.flush()

                # ACK считаем отправленным
                self._fast_confirm_slave_id = 0
                self._fast_confirm_flag = 0

                if self.fast_protocol_debug:
                    self.log.info(f"FAST POLL TX: {frame.hex(' ')}")
            except serial.serialutil.SerialException as e:
                self._mark_port_broken(f"fast poll serial exception: {e}")
                self.log.error(f"FAST POLL write error: {e}")
                self._fast_last_poll_ts = _now_ms()
                self._fast_next_poll_sleep_s = self.fast_poll_idle_sleep_s
                return []
            except OSError as e:
                self._mark_port_broken(f"fast poll os error: {e}")
                self.log.error(f"FAST POLL write error: {e}")
                self._fast_last_poll_ts = _now_ms()
                self._fast_next_poll_sleep_s = self.fast_poll_idle_sleep_s
                return []
            except Exception as e:
                self.log.error(f"FAST POLL write error: {e}")
                self._fast_last_poll_ts = _now_ms()
                self._fast_next_poll_sleep_s = self.fast_poll_idle_sleep_s
                return []

            blob = self._fast_read_blob(
                ser,
                gap_s=self.fast_rx_gap_s,
                hard_timeout_s=self.fast_rx_hard_timeout_s,
                max_bytes=4096,
            )

        self._fast_last_poll_ts = _now_ms()

        if self.fast_protocol_debug:
            self.log.info(f"FAST POLL RX: {blob.hex(' ') if blob else '<empty>'}")

        if not blob:
            self._fast_next_poll_sleep_s = self.fast_poll_idle_sleep_s
            return []

        frames = self._fast_split_frames(blob)
        if not frames:
            self._fast_next_poll_sleep_s = self.fast_poll_idle_sleep_s
            return []

        if len(frames) == 1 and frames[0] == self._fast_no_events_frame():
            self._fast_next_poll_sleep_s = self.fast_poll_idle_sleep_s
            return []

        events: List[Dict[str, Any]] = []

        for fr in frames:
            if self.fast_protocol_debug:
                self.log.info(f"FAST frame: {fr.hex(' ')}")

            if fr == self._fast_no_events_frame():
                continue

            fp = fr.lstrip(b"\xFF")

            # 1) сперва fast EVENTS
            if len(fp) >= 3 and len(fp) >= 8 and fp[1] == 0x46 and fp[2] == 0x11:
                events.extend(self._fast_handle_events_frame(fp))
                continue

            # 2) при желании можно поддержать и обычные RTU responses
            events.extend(self._fast_decode_rtu_response_frame(fp))

        self._fast_next_poll_sleep_s = (
            self.fast_poll_pending_sleep_s if events else self.fast_poll_idle_sleep_s
        )

        if self.fast_protocol_debug and events:
            self.log.info(f"FAST decoded events: {events}")

        return events[: self.fast_max_events_per_poll]

    def _fast_maybe_poll_and_ingest(self) -> None:
        try:
            events = self._fast_poll_once()
            if events:
                accepted = self.ingest_fast_events(events)
                if accepted:
                    self.log.info(f"FAST accepted events: {accepted}/{len(events)}")
        except Exception as e:
            self.log.error(f"FAST single-thread poll error: {e}")

    def _fast_parse_events_frame(self, frame: bytes) -> Optional[Dict[str, Any]]:
        if not frame:
            return None

        fp = frame.lstrip(b"\xFF")
        if len(fp) < 8:
            return None

        if not self._fast_check_crc(fp):
            if self.fast_protocol_debug:
                self.log.info(f"FAST bad crc: {fp.hex(' ')}")
            return None

        slave = fp[0]
        cmd = fp[1]
        sub = fp[2]

        if cmd != 0x46 or sub != 0x11:
            return None

        flag = fp[3]
        pending = fp[4]
        paylen = fp[5]

        if len(fp) < 6 + paylen + 2:
            return None

        payload = fp[6:6 + paylen]

        return {
            "slave": int(slave),
            "flag": int(flag),
            "pending": int(pending),
            "payload": payload,
            "raw": fp,
        }

    def _fast_parse_events_payload(self, payload: bytes) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        i = 0
        n = len(payload)

        while i + 4 <= n:
            extra_len = payload[i]
            ev_type = payload[i + 1]
            ev_id = (payload[i + 2] << 8) | payload[i + 3]

            extra_end = i + 4 + extra_len
            if extra_end > n:
                break

            extra = payload[i + 4:extra_end]

            out.append({
                "ev_type": int(ev_type),
                "ev_id": int(ev_id),
                "extra": extra,
            })

            i = extra_end

        return out

    def _fast_handle_events_frame(self, frame: bytes) -> List[Dict[str, Any]]:
        parsed = self._fast_parse_events_frame(frame)
        if not parsed:
            return []

        slave = parsed["slave"]
        flag = parsed["flag"]
        pending = parsed["pending"]
        payload = parsed["payload"]

        # ВАЖНО: подтверждаем этот пакет следующим poll
        self._fast_confirm_slave_id = slave
        self._fast_confirm_flag = flag

        decoded = self._fast_parse_events_payload(payload)

        if self.fast_protocol_debug:
            self.log.info(
                f"FAST EVENTS slave={slave} flag={flag} pending={pending} decoded={decoded}"
            )

        ID_TO_REGTYPE = {
            1: "coil",
            2: "discrete",
            3: "holding",
            4: "input",
        }

        out: List[Dict[str, Any]] = []

        for ev in decoded:
            ev_type = int(ev["ev_type"])
            ev_id = int(ev["ev_id"])

            # системное событие reboot и т.п.
            if ev_type == 0x0F and ev_id == 0:
                self.log.info(f"FAST reboot/system event from unit={slave}")
                continue

            register_type = ID_TO_REGTYPE.get(ev_type)
            if not register_type:
                # неизвестное событие все равно ACK уже будет отправлен
                self.log.info(
                    f"FAST unknown event type from unit={slave}: ev_type={ev_type} ev_id={ev_id}"
                )
                continue

            out.append({
                "unit_id": slave,
                "register_type": register_type,
                "addr": ev_id,
            })

        self._fast_next_poll_sleep_s = (
            self.fast_poll_pending_sleep_s if pending else self.fast_poll_idle_sleep_s
        )

        return out

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

        if self.fast_enabled and self.transport != "tcp":
            self.log.info("fast modbus single-thread mode enabled")

        t_summary = _now_ms()
        summary_every = int(self.debug.get("summary_every_s", 0))

        while not self._stop_event.is_set():

            # 0) приоритетно выполняем накопившиеся записи
            tasks = self._collect_write_tasks()
            if tasks:
                try:
                    self._execute_write_tasks_smart(tasks)
                finally:
                    for _ in tasks:
                        try:
                            self._write_q.task_done()
                        except Exception:
                            pass

            # 0.05) одноразовый bootstrap fast-modbus в этом же потоке
            if self.fast_enabled and not self._fast_bootstrap_done:
                self._fast_bootstrap_once()

            # 0.06) попытка fast-poll в этом же потоке
            if self.fast_enabled and self._fast_bootstrap_done:
                self._fast_maybe_poll_and_ingest()

            # 0.1) внеочередно дочитываем fast-события
            self._drain_fast_reads(max_units=2, max_blocks=4)

            loop_start = _now_ms()
            for nd in self.nodes:
                if self._stop.is_set():
                    break
                # Запомним текущий unit_id для TCP-операций
                self._current_unit_id = nd.unit_id

                if self._unit_is_temporarily_skipped(nd.unit_id):
                    continue

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
                        if self._stop.is_set():
                            break
                        if self.debug.get("enabled"):
                            fc = _func_code(rtype)
                            raw_first = group_params[0].address if group_params else start
                            self.log.debug(
                                f"REQ unit={nd.unit_id} fc={fc} type={rtype} "
                                f"start={start} count={count} (yaml_first={raw_first})"
                            )

                        try:
                            # 1) первая попытка
                            block_vals = self._read_block(inst, rtype, start, count)

                            # 2) успех: обработка значений (твой существующий код)
                            if self.debug.get("enabled"):
                                self.log.debug(
                                    f"RSP unit={nd.unit_id} fc={_func_code(rtype)} values={len(block_vals)} ok"
                                )

                            for i, p in enumerate(group_params):
                                raw = block_vals[i]
                                dt = (p.data_type or "u16").lower()

                                if rtype in ("coil", "discrete"):
                                    raw_v = 1 if bool(raw) else 0
                                elif dt == "s16":
                                    raw_v = int(raw) & 0xFFFF
                                    if raw_v & 0x8000:
                                        raw_v -= 0x10000
                                else:
                                    raw_v = int(raw) & 0xFFFF

                                val = float(raw_v) * _safe_scale(p.scale)
                                val = _round_ndp(val, self.precision_decimals)

                                key = self._make_key(nd.unit_id, p.name)
                                now = _now_ms()
                                self._last_ok_ts[key] = now
                                self._last_attempt_ts[key] = now
                                self._no_reply[nd.unit_id] = 0
                                self._unit_skip_until.pop(nd.unit_id, None)
                                self._stats[(nd.unit_id, "read_ok")] += 1

                                if self.debug.get("log_reads"):
                                    self.log.debug(f"read ok {rtype}[{start + i}] raw={raw} -> {val}")

                                self._maybe_publish(nd, p, val, 0, "OK", self._last_ok_ts[key], self._last_attempt_ts[key])

                        except Exception as e:
                            code, msg = self._map_ex(e)

                            # ---- retry once только для CRC ----
                            if msg == "CRC_ERROR" and self.crc_retry_count > 0:
                                if self.debug.get("enabled"):
                                    self.log.warning(
                                        f"CRC retry: unit={nd.unit_id} fc={_func_code(rtype)} type={rtype} "
                                        f"range=[{start}..{start + count - 1}] attempt=2"
                                    )

                                if self.crc_retry_delay_s > 0:
                                    time.sleep(self.crc_retry_delay_s)

                                try:
                                    inst.serial.reset_input_buffer()
                                except Exception:
                                    pass

                                try:
                                    # повторная попытка
                                    block_vals = self._read_block(inst, rtype, start, count)

                                    # обработка успеха после retry (тот же код, можно вынести в helper, но оставим просто)
                                    if self.debug.get("enabled"):
                                        self.log.debug(
                                            f"RSP unit={nd.unit_id} fc={_func_code(rtype)} values={len(block_vals)} ok [retry]"
                                        )

                                    for i, p in enumerate(group_params):
                                        raw = block_vals[i]
                                        dt = (p.data_type or "u16").lower()

                                        if rtype in ("coil", "discrete"):
                                            raw_v = 1 if bool(raw) else 0
                                        elif dt == "s16":
                                            raw_v = int(raw) & 0xFFFF
                                            if raw_v & 0x8000:
                                                raw_v -= 0x10000
                                        else:
                                            raw_v = int(raw) & 0xFFFF

                                        val = float(raw_v) * _safe_scale(p.scale)
                                        val = _round_ndp(val, self.precision_decimals)

                                        key = self._make_key(nd.unit_id, p.name)
                                        now = _now_ms()
                                        self._last_ok_ts[key] = now
                                        self._last_attempt_ts[key] = now
                                        self._no_reply[nd.unit_id] = 0
                                        self._unit_skip_until.pop(nd.unit_id, None)
                                        self._stats[(nd.unit_id, "read_ok")] += 1
                                        self._maybe_publish(nd, p, val, 0, "OK", self._last_ok_ts[key], self._last_attempt_ts[key])

                                    # важно: если retry успешен — НЕ падать дальше
                                    continue

                                except Exception as e2:
                                    # retry не помог — считаем, что это исходная ошибка (можно залогировать e2)
                                    e = e2
                                    code, msg = self._map_ex(e)

                            # ---- стандартная обработка ошибки блока ----
                            if self.debug.get("enabled"):
                                fc = _func_code(rtype)
                                self.log.error(
                                    f"block read error unit={nd.unit_id} fc={fc} type={rtype} "
                                    f"range=[{start}..{start + count - 1}] port={self.port} → {e}"
                                )
                                if msg == "ILLEGAL_DATA_ADDRESS":
                                    self.log.error(
                                        "hint: Illegal Data Address чаще всего = не тот адрес/диапазон. "
                                        "Проверь normalize-правило и off-by-one (например 1001 vs 1000), "
                                        "а также лимиты batch_read.max_bits/regs."
                                    )

                            self._mark_unit_no_reply(nd.unit_id, code, msg)
                            for i, p in enumerate(group_params):
                                key = self._make_key(nd.unit_id, p.name)
                                now = _now_ms()
                                self._last_attempt_ts[key] = now
                                self._stats[(nd.unit_id, "read_err")] += 1
                                self._maybe_publish(nd, p, None, code, msg, self._last_ok_ts.get(key), now)

                            if self._no_reply[nd.unit_id] >= max_err:
                                time.sleep(backoff_ms)

                            if self.skip_node_on_timeout and code in (1, 7, 12):
                                # не мучаем этот slave дальше в текущем проходе
                                break

                        finally:
                            self._inter_delay()

                            if self.fast_enabled and self._fast_bootstrap_done:
                                self._fast_maybe_poll_and_ingest()

                            self._drain_fast_reads(max_units=1, max_blocks=1)

                    # дочитываем параметры с words > 1 одиночными чтениями
                    for p in (pp for pp in nd.params if int(getattr(pp, "words", 1) or 1) > 1):
                        if self._stop.is_set():
                            break
                        key = self._make_key(nd.unit_id, p.name)

                        if self.debug.get("enabled"):
                            fc = _func_code(p.register_type)
                            addr_norm = self._normalize_addr(p)
                            self.log.debug(
                                f"REQ unit={nd.unit_id} fc={fc} type={p.register_type} "
                                f"addr={addr_norm} (yaml={p.address}) [multi-word]"
                            )

                        val, code, msg = self._read_single(nd.unit_id, inst, p)
                        now = _now_ms()
                        self._last_attempt_ts[key] = now

                        if val is None:
                            self._mark_unit_no_reply(nd.unit_id, code, msg)
                            self._stats[(nd.unit_id, "read_err")] += 1
                            self._maybe_publish(nd, p, None, code, msg, self._last_ok_ts.get(key), now)
                            if self._no_reply[nd.unit_id] >= max_err:
                                time.sleep(backoff_ms)

                            if self.skip_node_on_timeout and code in (1, 7, 12):
                                break
                        else:
                            self._no_reply[nd.unit_id] = 0
                            self._unit_skip_until.pop(nd.unit_id, None)
                            self._stats[(nd.unit_id, "read_ok")] += 1
                            self._last_ok_ts[key] = now
                            self._maybe_publish(nd, p, val, 0, "OK", self._last_ok_ts[key], now)
                        if self.fast_enabled and self._fast_bootstrap_done:
                            self._fast_maybe_poll_and_ingest()

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
                    if self._stop.is_set():
                        break

                    if self.debug.get("enabled"):
                        fc = _func_code(p.register_type)
                        addr_norm = self._normalize_addr(p)
                        self.log.debug(
                            f"REQ unit={nd.unit_id} fc={fc} type={p.register_type} "
                            f"addr={addr_norm} (yaml={p.address})"
                        )

                    key = self._make_key(nd.unit_id, p.name)
                    val, code, msg = self._read_single(nd.unit_id, inst, p)

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
                        self._mark_unit_no_reply(nd.unit_id, code, msg)
                        self._stats[(nd.unit_id, "read_err")] += 1
                        self._maybe_publish(nd, p, None, code, msg, self._last_ok_ts.get(key), now)
                        if self._no_reply[nd.unit_id] >= max_err:
                            time.sleep(backoff_ms)

                        if self.skip_node_on_timeout and code in (1, 7, 12):
                            break
                    else:
                        self._no_reply[nd.unit_id] = 0
                        self._unit_skip_until.pop(nd.unit_id, None)
                        self._stats[(nd.unit_id, "read_ok")] += 1
                        self._last_ok_ts[key] = now
                        self._maybe_publish(nd, p, val, 0, "OK", self._last_ok_ts[key], now)
                    self._inter_delay()
                    if self.fast_enabled and self._fast_bootstrap_done:
                        self._fast_maybe_poll_and_ingest()
                    self._drain_fast_reads(max_units=1, max_blocks=1)

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
