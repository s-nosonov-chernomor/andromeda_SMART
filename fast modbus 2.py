#!/usr/bin/env python3
# fast_modbus_gw.py
# Python 3.8 compatible
# pymodbus==3.6.9

from __future__ import annotations

import argparse
import json
import logging
import re
import struct
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import yaml
import paho.mqtt.client as mqtt
from pymodbus.client import ModbusSerialClient


# -----------------------------
# logging
# -----------------------------
log = logging.getLogger("fastmodbus")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


# -----------------------------
# helpers
# -----------------------------
def now_s() -> float:
    return time.monotonic()


def safe_slug(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9а-яё_]+", "_", s, flags=re.IGNORECASE)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "param"


def to_float(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return float("nan")


# -----------------------------
# decoding
# -----------------------------
_WORD_ORDER_MAP_2 = {
    "AB": [0, 1],
    "BA": [1, 0],
}

_WORD_ORDER_MAP_4 = {
    "ABCD": [0, 1, 2, 3],
    "DCBA": [3, 2, 1, 0],
    "BADC": [1, 0, 3, 2],
    "CDAB": [2, 3, 0, 1],
}

_WORD_ORDER_MAP_1 = {"A": [0], "AB": [0]}  # иногда встречается мусор в yaml


def reorder_registers(registers: List[int], word_order: str) -> List[int]:
    wo = (word_order or "").strip().upper()
    n = len(registers)

    if n <= 1:
        return registers

    if n == 2:
        order = _WORD_ORDER_MAP_2.get(wo)
        if order is None:
            # по умолчанию AB
            order = [0, 1]
        return [registers[i] for i in order]

    if n == 4:
        order = _WORD_ORDER_MAP_4.get(wo)
        if order is None:
            order = [0, 1, 2, 3]
        return [registers[i] for i in order]

    # для нестандартных длин — не трогаем
    return registers


def regs_to_bytes_be(registers: List[int]) -> bytes:
    # каждый регистр — big-endian 16-bit
    b = bytearray()
    for r in registers:
        b += struct.pack(">H", int(r) & 0xFFFF)
    return bytes(b)


def decode_value(registers: List[int], data_type: str, word_order: str, scale: float) -> Any:
    dt = (data_type or "u16").strip().lower()
    regs = reorder_registers(registers, word_order)
    raw = regs_to_bytes_be(regs)

    try:
        if dt == "u16":
            v = struct.unpack(">H", raw[:2])[0]
        elif dt == "s16":
            v = struct.unpack(">h", raw[:2])[0]
        elif dt == "u32":
            v = struct.unpack(">I", raw[:4])[0]
        elif dt == "s32":
            v = struct.unpack(">i", raw[:4])[0]
        elif dt == "u64":
            v = struct.unpack(">Q", raw[:8])[0]
        elif dt == "s64":
            v = struct.unpack(">q", raw[:8])[0]
        elif dt == "f32":
            v = struct.unpack(">f", raw[:4])[0]
        elif dt == "f64":
            v = struct.unpack(">d", raw[:8])[0]
        else:
            # неизвестный тип — отдаем как список регистров
            v = regs

        if isinstance(v, (int, float)) and scale not in (None, 0, 1, 1.0):
            v = v * float(scale)

        return v
    except Exception:
        # безопасный fallback
        return None


# -----------------------------
# config structures
# -----------------------------
@dataclass
class ParamSpec:
    name: str
    register_type: str  # coil / discrete / holding / input
    address: int
    words: int
    data_type: str
    word_order: str
    scale: float
    mode: str
    publish_mode: str
    publish_interval_s: float
    topic: Optional[str] = None


@dataclass
class NodeSpec:
    unit_id: int
    object_name: str
    params: List[ParamSpec]


@dataclass
class LineSpec:
    name: str
    device: str
    baudrate: int
    timeout: float
    parity: str
    stopbits: int
    rs485_rts_toggle: bool
    nodes: List[NodeSpec]


@dataclass
class BatchReadCfg:
    enabled: bool
    max_bits: int
    max_registers: int


@dataclass
class PollingCfg:
    interval_ms: int
    jitter_ms: int
    backoff_ms: int
    max_errors_before_backoff: int
    port_retry_backoff_s: float
    batch_read: BatchReadCfg


@dataclass
class MqttCfg:
    host: str
    port: int
    base_topic: str
    qos: int
    retain: bool
    client_id: str


@dataclass
class DebugCfg:
    enabled: bool
    log_reads: bool
    summary_every_s: int


@dataclass
class RootCfg:
    debug: DebugCfg
    mqtt: MqttCfg
    polling: PollingCfg
    lines: List[LineSpec]


def load_cfg(path: str) -> RootCfg:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    debug = raw.get("debug", {}) or {}
    mqtt_raw = raw.get("mqtt", {}) or {}
    polling_raw = raw.get("polling", {}) or {}
    batch_raw = (polling_raw.get("batch_read", {}) or {})

    cfg_lines: List[LineSpec] = []
    for l in raw.get("lines", []) or []:
        nodes: List[NodeSpec] = []
        for n in l.get("nodes", []) or []:
            params: List[ParamSpec] = []
            for p in n.get("params", []) or []:
                params.append(
                    ParamSpec(
                        name=str(p.get("name", "")),
                        register_type=str(p.get("register_type", "")).strip().lower(),
                        address=int(p.get("address", 0)),
                        words=int(p.get("words", 1) or 1),
                        data_type=str(p.get("data_type", "u16")),
                        word_order=str(p.get("word_order", "AB")),
                        scale=float(p.get("scale", 1.0) or 1.0),
                        mode=str(p.get("mode", "r")),
                        publish_mode=str(p.get("publish_mode", "on_change_and_interval")),
                        publish_interval_s=float(p.get("publish_interval_s", 300.0) or 300.0),
                        topic=p.get("topic", None),
                    )
                )

            nodes.append(
                NodeSpec(
                    unit_id=int(n.get("unit_id", 1)),
                    object_name=str(n.get("object", "")),
                    params=params,
                )
            )

        cfg_lines.append(
            LineSpec(
                name=str(l.get("name", "line")),
                device=str(l.get("device", "")),
                baudrate=int(l.get("baudrate", 9600)),
                timeout=float(l.get("timeout", 0.2)),
                parity=str(l.get("parity", "N")),
                stopbits=int(l.get("stopbits", 1)),
                rs485_rts_toggle=bool(l.get("rs485_rts_toggle", False)),
                nodes=nodes,
            )
        )

    return RootCfg(
        debug=DebugCfg(
            enabled=bool(debug.get("enabled", False)),
            log_reads=bool(debug.get("log_reads", False)),
            summary_every_s=int(debug.get("summary_every_s", 20)),
        ),
        mqtt=MqttCfg(
            host=str(mqtt_raw.get("host", "127.0.0.1")),
            port=int(mqtt_raw.get("port", 1883)),
            base_topic=str(mqtt_raw.get("base_topic", "/devices")).rstrip("/"),
            qos=int(mqtt_raw.get("qos", 0)),
            retain=bool(mqtt_raw.get("retain", False)),
            client_id=str(mqtt_raw.get("client_id", "uspd-modbus-gw")),
        ),
        polling=PollingCfg(
            interval_ms=int(polling_raw.get("interval_ms", 100)),
            jitter_ms=int(polling_raw.get("jitter_ms", 0)),
            backoff_ms=int(polling_raw.get("backoff_ms", 0)),
            max_errors_before_backoff=int(polling_raw.get("max_errors_before_backoff", 999999)),
            port_retry_backoff_s=float(polling_raw.get("port_retry_backoff_s", 1.0)),
            batch_read=BatchReadCfg(
                enabled=bool(batch_raw.get("enabled", True)),
                max_bits=int(batch_raw.get("max_bits", 256)),
                max_registers=int(batch_raw.get("max_registers", 120)),
            ),
        ),
        lines=cfg_lines,
    )


# -----------------------------
# batching
# -----------------------------
def group_bits(params: List[ParamSpec], max_bits: int) -> List[Tuple[int, int, List[ParamSpec]]]:
    """
    Для coil/discrete: каждый параметр == 1 бит по address.
    Возвращает список блоков: (start, count, params_in_block)
    """
    ps = sorted(params, key=lambda x: x.address)
    blocks: List[Tuple[int, int, List[ParamSpec]]] = []

    i = 0
    while i < len(ps):
        start = ps[i].address
        block_params = [ps[i]]
        end = start

        j = i + 1
        while j < len(ps):
            a = ps[j].address
            # расширяем блок только если входит по длине
            new_end = max(end, a)
            new_count = (new_end - start) + 1
            if new_count > max_bits:
                break
            block_params.append(ps[j])
            end = new_end
            j += 1

        count = (end - start) + 1
        blocks.append((start, count, block_params))
        i = j

    return blocks


def group_registers(params: List[ParamSpec], max_regs: int) -> List[Tuple[int, int, List[ParamSpec]]]:
    """
    Для holding: params имеют words>=1.
    Делает блоки по диапазону регистров, чтобы суммарная длина <= max_regs.
    Возвращает (start, count, params_in_block)
    """
    ps = sorted(params, key=lambda x: x.address)
    blocks: List[Tuple[int, int, List[ParamSpec]]] = []

    i = 0
    while i < len(ps):
        start = ps[i].address
        # end регистр включительно
        end = ps[i].address + max(1, ps[i].words) - 1
        block_params = [ps[i]]

        j = i + 1
        while j < len(ps):
            p = ps[j]
            p_start = p.address
            p_end = p.address + max(1, p.words) - 1

            new_start = start
            new_end = max(end, p_end)
            new_count = (new_end - new_start) + 1

            if new_count > max_regs:
                break

            block_params.append(p)
            end = new_end
            j += 1

        count = (end - start) + 1
        blocks.append((start, count, block_params))
        i = j

    return blocks


# -----------------------------
# MQTT
# -----------------------------
class MqttPub:
    def __init__(self, cfg: MqttCfg):
        self.cfg = cfg
        self.client = mqtt.Client(client_id=cfg.client_id, clean_session=True)

    def connect(self) -> None:
        self.client.connect(self.cfg.host, self.cfg.port, keepalive=60)
        self.client.loop_start()
        log.info("MQTT connected %s:%s base_topic=%s", self.cfg.host, self.cfg.port, self.cfg.base_topic)

    def publish(self, topic: str, payload: Dict[str, Any]) -> None:
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        self.client.publish(topic, data, qos=self.cfg.qos, retain=self.cfg.retain)


# -----------------------------
# poller
# -----------------------------
class FastModbusGw:
    def __init__(self, cfg: RootCfg):
        self.cfg = cfg
        self.mqtt = MqttPub(cfg.mqtt)

        # last_value / last_publish
        self.last_value: Dict[str, Any] = {}
        self.last_pub_ts: Dict[str, float] = {}

        self.stats_reads_ok = 0
        self.stats_reads_err = 0
        self.stats_pubs = 0
        self.stats_skipped_input = 0

        self._last_summary = now_s()

    def topic_for(self, line: LineSpec, node: NodeSpec, p: ParamSpec) -> str:
        if p.topic:
            # если topic задан — считаем абсолютным/готовым
            t = str(p.topic)
            return t
        base = self.cfg.mqtt.base_topic
        return f"{base}/{safe_slug(line.name)}/{node.unit_id}/{safe_slug(p.name)}"

    def key_for(self, line: LineSpec, node: NodeSpec, p: ParamSpec) -> str:
        return f"{line.name}|{node.unit_id}|{p.register_type}|{p.address}|{p.name}"

    def maybe_publish(self, line: LineSpec, node: NodeSpec, p: ParamSpec, value: Any) -> None:
        k = self.key_for(line, node, p)
        t = now_s()

        last_v = self.last_value.get(k, None)
        last_pub = self.last_pub_ts.get(k, 0.0)

        # publish_mode: on_change_and_interval
        changed = (last_v != value)
        due = (t - last_pub) >= float(p.publish_interval_s or 0)

        if changed or due:
            topic = self.topic_for(line, node, p)
            payload = {
                "ts": int(time.time()),
                "line": line.name,
                "unit_id": node.unit_id,
                "object": node.object_name,
                "name": p.name,
                "register_type": p.register_type,
                "address": p.address,
                "value": value,
            }
            self.mqtt.publish(topic, payload)
            self.last_pub_ts[k] = t
            self.stats_pubs += 1

        self.last_value[k] = value

    def _open_client(self, line: LineSpec) -> ModbusSerialClient:
        # strict for pymodbus 3.6.9
        client = ModbusSerialClient(
            method="rtu",
            port=line.device,
            baudrate=line.baudrate,
            parity=line.parity,
            stopbits=line.stopbits,
            bytesize=8,
            timeout=line.timeout,
            retries=0,  # быстрый режим, без внутренних ретраев
        )
        return client

    def _read_coils_block(self, client: ModbusSerialClient, unit: int, start: int, count: int) -> Optional[List[int]]:
        rr = client.read_coils(address=start, count=count, unit=unit)
        if rr is None or rr.isError():
            return None
        bits = getattr(rr, "bits", None)
        if not isinstance(bits, list):
            return None
        return [1 if bool(b) else 0 for b in bits[:count]]

    def _read_discrete_block(self, client: ModbusSerialClient, unit: int, start: int, count: int) -> Optional[List[int]]:
        rr = client.read_discrete_inputs(address=start, count=count, unit=unit)
        if rr is None or rr.isError():
            return None
        bits = getattr(rr, "bits", None)
        if not isinstance(bits, list):
            return None
        return [1 if bool(b) else 0 for b in bits[:count]]

    def _read_holding_block(self, client: ModbusSerialClient, unit: int, start: int, count: int) -> Optional[List[int]]:
        rr = client.read_holding_registers(address=start, count=count, unit=unit)
        if rr is None or rr.isError():
            return None
        regs = getattr(rr, "registers", None)
        if not isinstance(regs, list):
            return None
        return [int(x) & 0xFFFF for x in regs[:count]]

    def poll_once_line(self, line: LineSpec) -> None:
        client = self._open_client(line)

        if not client.connect():
            self.stats_reads_err += 1
            log.warning("Port connect failed: %s (sleep %.2fs)", line.device, self.cfg.polling.port_retry_backoff_s)
            time.sleep(float(self.cfg.polling.port_retry_backoff_s))
            return

        try:
            for node in line.nodes:
                # ФИЛЬТР: пропускаем input целиком
                coil_params = [p for p in node.params if p.register_type == "coil"]
                disc_params = [p for p in node.params if p.register_type == "discrete"]
                hold_params = [p for p in node.params if p.register_type == "holding"]
                input_params = [p for p in node.params if p.register_type == "input"]
                self.stats_skipped_input += len(input_params)

                # --- coils ---
                if coil_params:
                    if self.cfg.polling.batch_read.enabled:
                        blocks = group_bits(coil_params, self.cfg.polling.batch_read.max_bits)
                    else:
                        blocks = [(p.address, 1, [p]) for p in coil_params]

                    for start, count, ps in blocks:
                        t0 = now_s()
                        bits = self._read_coils_block(client, node.unit_id, start, count)
                        dt = (now_s() - t0) * 1000.0

                        if bits is None:
                            self.stats_reads_err += 1
                            if self.cfg.debug.enabled:
                                log.warning("coil read ERR unit=%s start=%s count=%s (%.1f ms)", node.unit_id, start, count, dt)
                            continue

                        self.stats_reads_ok += 1
                        if self.cfg.debug.enabled and self.cfg.debug.log_reads:
                            log.info("coil read OK unit=%s start=%s count=%s (%.1f ms)", node.unit_id, start, count, dt)

                        for p in ps:
                            idx = p.address - start
                            v = bits[idx] if 0 <= idx < len(bits) else None
                            self.maybe_publish(line, node, p, v)

                # --- discrete ---
                if disc_params:
                    if self.cfg.polling.batch_read.enabled:
                        blocks = group_bits(disc_params, self.cfg.polling.batch_read.max_bits)
                    else:
                        blocks = [(p.address, 1, [p]) for p in disc_params]

                    for start, count, ps in blocks:
                        t0 = now_s()
                        bits = self._read_discrete_block(client, node.unit_id, start, count)
                        dt = (now_s() - t0) * 1000.0

                        if bits is None:
                            self.stats_reads_err += 1
                            if self.cfg.debug.enabled:
                                log.warning("discrete read ERR unit=%s start=%s count=%s (%.1f ms)", node.unit_id, start, count, dt)
                            continue

                        self.stats_reads_ok += 1
                        if self.cfg.debug.enabled and self.cfg.debug.log_reads:
                            log.info("discrete read OK unit=%s start=%s count=%s (%.1f ms)", node.unit_id, start, count, dt)

                        for p in ps:
                            idx = p.address - start
                            v = bits[idx] if 0 <= idx < len(bits) else None
                            self.maybe_publish(line, node, p, v)

                # --- holding ---
                if hold_params:
                    if self.cfg.polling.batch_read.enabled:
                        blocks = group_registers(hold_params, self.cfg.polling.batch_read.max_registers)
                    else:
                        blocks = [(p.address, max(1, p.words), [p]) for p in hold_params]

                    for start, count, ps in blocks:
                        t0 = now_s()
                        regs = self._read_holding_block(client, node.unit_id, start, count)
                        dt = (now_s() - t0) * 1000.0

                        if regs is None:
                            self.stats_reads_err += 1
                            if self.cfg.debug.enabled:
                                log.warning("holding read ERR unit=%s start=%s count=%s (%.1f ms)", node.unit_id, start, count, dt)
                            continue

                        self.stats_reads_ok += 1
                        if self.cfg.debug.enabled and self.cfg.debug.log_reads:
                            log.info("holding read OK unit=%s start=%s count=%s (%.1f ms)", node.unit_id, start, count, dt)

                        for p in ps:
                            off = p.address - start
                            n = max(1, p.words)
                            slice_regs = regs[off : off + n] if 0 <= off < len(regs) else []
                            v = decode_value(slice_regs, p.data_type, p.word_order, p.scale)
                            self.maybe_publish(line, node, p, v)

        finally:
            try:
                client.close()
            except Exception:
                pass

    def summary(self) -> None:
        t = now_s()
        if (t - self._last_summary) >= float(self.cfg.debug.summary_every_s):
            self._last_summary = t
            log.info(
                "summary: reads_ok=%d reads_err=%d pubs=%d skipped_input=%d",
                self.stats_reads_ok,
                self.stats_reads_err,
                self.stats_pubs,
                self.stats_skipped_input,
            )

    def run_forever(self) -> None:
        self.mqtt.connect()

        interval_s = max(0.001, float(self.cfg.polling.interval_ms) / 1000.0)

        while True:
            t0 = now_s()

            for line in self.cfg.lines:
                self.poll_once_line(line)

            self.summary()

            elapsed = now_s() - t0
            sleep_s = interval_s - elapsed
            if sleep_s > 0:
                time.sleep(sleep_s)


# -----------------------------
# main
# -----------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", "-c", required=True, help="Path to yaml config")
    args = ap.parse_args()

    cfg = load_cfg(args.config)

    # ВАЖНО: input мы специально не читаем вообще — это соответствует твоему требованию
    # (подписываемся на все регистры кроме input)

    gw = FastModbusGw(cfg)
    gw.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())