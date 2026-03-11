from __future__ import annotations

import time
import struct
import logging
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Any
import yaml
import serial

# -------------------------------------------------
# logging
# -------------------------------------------------
log = logging.getLogger("fastmodbus")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

# -------------------------------------------------
# CRC16 Modbus RTU
# -------------------------------------------------
def crc16_modbus(data: bytes) -> int:
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc & 0xFFFF


def add_crc(frame_wo_crc: bytes) -> bytes:
    c = crc16_modbus(frame_wo_crc)
    return frame_wo_crc + struct.pack("<H", c)


def check_crc(frame: bytes) -> bool:
    if len(frame) < 4:
        return False
    body, got = frame[:-2], frame[-2:]
    return got == struct.pack("<H", crc16_modbus(body))


# -------------------------------------------------
# Fast Modbus constants (Wiren Board)
# -------------------------------------------------
BCAST_EVENTS_SLAVE = 0xFD
CMD_EXT = 0x46
SUB_CFG = 0x18
SUB_POLL = 0x10
SUB_EVENTS = 0x11
SUB_NO_EVENTS = 0x12

PRIORITY_OFF = 0
PRIORITY_LOW = 1
PRIORITY_HIGH = 2

REGTYPE_TO_ID = {"coil": 1, "discrete": 2, "holding": 3, "input": 4}
ID_TO_REGTYPE = {v: k for k, v in REGTYPE_TO_ID.items()}

REGTYPE_RU = {
    "coil": "дискретный выход (coil)",
    "discrete": "дискретный вход (DI)",
    "holding": "holding-регистр (HR)",
    "input": "input-регистр (IR)",
    "?": "неизвестный тип",
}

# -------------------------------------------------
# Config models
# -------------------------------------------------
@dataclass
class Param:
    name: str
    register_type: str
    address: int


@dataclass
class UnitMap:
    unit_id: int
    params: Dict[Tuple[str, int], Param]


# -------------------------------------------------
# YAML loader
# -------------------------------------------------
def load_from_yaml(cfg_path: str) -> Tuple[str, int, List[UnitMap]]:
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    lines = cfg.get("lines", []) or []
    serial_line = next((ln for ln in lines if ln.get("transport") == "serial"), None)
    if not serial_line:
        raise RuntimeError("No serial line found in YAML")

    port = serial_line["device"]
    baud = int(serial_line.get("baudrate", 115200))

    units: List[UnitMap] = []
    for nd in serial_line.get("nodes", []) or []:
        unit_id = int(nd["unit_id"])
        pmap: Dict[Tuple[str, int], Param] = {}
        for p in nd.get("params", []) or []:
            rtype = str(p.get("register_type", "")).strip()
            addr = int(p.get("address", 0))
            name = str(p.get("name", ""))
            pmap[(rtype, addr)] = Param(name=name, register_type=rtype, address=addr)
        units.append(UnitMap(unit_id=unit_id, params=pmap))

    return port, baud, units


# -------------------------------------------------
# Fast Modbus frame builders
# -------------------------------------------------
def build_cfg_events_frame(slave_id: int,
                           blocks: List[Tuple[str, int, List[int]]]) -> bytes:
    payload = bytearray()

    for (rtype, start, settings) in blocks:
        type_id = REGTYPE_TO_ID[rtype]
        count = len(settings)
        payload += struct.pack(">BHB", type_id, start & 0xFFFF, count)
        payload += bytes(settings)

    frame_wo_crc = bytes([slave_id, CMD_EXT, SUB_CFG, len(payload)]) + bytes(payload)
    return add_crc(frame_wo_crc)


def build_poll_events_frame(confirm_slave_id: int,
                             confirm_flag: int) -> bytes:
    frame_wo_crc = bytes([
        BCAST_EVENTS_SLAVE,
        CMD_EXT,
        SUB_POLL,
        0,
        200,
        confirm_slave_id & 0xFF,
        confirm_flag & 0xFF,
    ])
    return add_crc(frame_wo_crc)


# -------------------------------------------------
# Serial read helper
# -------------------------------------------------
def read_frame_gap(ser: serial.Serial,
                   gap_s: float = 0.02,
                   hard_timeout_s: float = 0.35) -> Optional[bytes]:

    t0 = time.time()
    buf = bytearray()
    last_rx = None

    while True:
        b = ser.read(1)
        now = time.time()

        if b:
            buf += b
            last_rx = now
            continue

        if buf and last_rx and (now - last_rx) >= gap_s:
            break

        if (now - t0) >= hard_timeout_s:
            break

    return bytes(buf) if buf else None


# -------------------------------------------------
# Parsers
# -------------------------------------------------
def parse_fast_frame(frame: bytes) -> Optional[Dict[str, Any]]:
    if not frame:
        return None

    frame = frame.lstrip(b"\xFF")

    if len(frame) < 4:
        return None

    if not check_crc(frame):
        return None

    slave = frame[0]
    sub = frame[2]

    if sub == SUB_NO_EVENTS:
        return {"type": "no_events"}

    if sub == SUB_EVENTS:
        flag = frame[3]
        pending = frame[4]
        paylen = frame[5]
        payload = frame[6:6 + paylen]
        return {
            "type": "events",
            "slave": slave,
            "flag": flag,
            "pending": pending,
            "payload": payload,
        }

    if sub == SUB_CFG:
        return {"type": "cfg_resp"}

    return None


def parse_events_payload(payload: bytes) -> List[Dict[str, Any]]:
    out = []
    i = 0
    n = len(payload)

    while i + 4 <= n:
        extra_len = payload[i]
        ev_type = payload[i + 1]
        ev_id = (payload[i + 2] << 8) | payload[i + 3]
        extra = payload[i + 4:i + 4 + extra_len]
        out.append({
            "ev_type": ev_type,
            "ev_id": ev_id,
            "extra": extra,
        })
        i += 4 + extra_len

    return out


# -------------------------------------------------
# Build blocks helper
# -------------------------------------------------
def build_blocks_for_unit(unit: UnitMap) -> List[Tuple[str, int, List[int]]]:
    blocks = []

    grouped: Dict[str, List[int]] = {"coil": [], "discrete": [], "holding": []}

    for (rtype, addr), _ in unit.params.items():
        if rtype in grouped:
            grouped[rtype].append(addr)

    for rtype, addrs in grouped.items():
        if not addrs:
            continue
        addrs = sorted(set(addrs))
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

    return blocks


# -------------------------------------------------
# MAIN
# -------------------------------------------------
def main():
    cfg_path = "config.yaml"

    port, baud, units = load_from_yaml(cfg_path)

    # подписываемся ТОЛЬКО на 94 и 138
    TARGET_UNITS = {94, 138}
    units = [u for u in units if u.unit_id in TARGET_UNITS]

    log.info(f"Open serial {port} @ {baud}")

    ser = serial.Serial(
        port=port,
        baudrate=baud,
        bytesize=8,
        parity=serial.PARITY_NONE,
        stopbits=1,
        timeout=0.02,
    )

    # -------------------------------------------------
    # SUBSCRIBE
    # -------------------------------------------------
    for unit in units:
        blocks = build_blocks_for_unit(unit)
        if not blocks:
            continue

        frame = build_cfg_events_frame(unit.unit_id, blocks)

        ser.reset_input_buffer()
        log.info(f"TX CFG unit={unit.unit_id}")
        ser.write(frame)

        resp = read_frame_gap(ser)
        parsed = parse_fast_frame(resp)

        if parsed and parsed["type"] == "cfg_resp":
            log.info(f"RX CFG unit={unit.unit_id}: OK")
        else:
            log.warning(f"CFG unit={unit.unit_id}: no/invalid response")

    log.info("Subscription complete. Start polling...")

    # -------------------------------------------------
    # POLL LOOP
    # -------------------------------------------------
    confirm_slave_id = 0
    confirm_flag = 0

    while True:
        frame = build_poll_events_frame(confirm_slave_id, confirm_flag)

        ser.reset_input_buffer()
        # log.info(
        #     f"Опрос событий (ACK: устройство={confirm_slave_id or '-'}, флаг={confirm_flag})"
        # )
        ser.write(frame)

        resp = read_frame_gap(ser)
        if not resp:
            time.sleep(1.0)
            continue

        parsed = parse_fast_frame(resp)
        if not parsed:
            time.sleep(1.0)
            continue

        # 1) когда нет событий — молчим
        if parsed["type"] == "no_events":
            time.sleep(1.0)
            continue

        # 2) нас интересуют только события
        if parsed["type"] != "events":
            time.sleep(1.0)
            continue

        slave = parsed["slave"]
        flag = parsed["flag"]

        # Сразу подтверждаем пакеты от всех, чтобы “посторонние”
        # устройства тоже успокоились
        confirm_slave_id = slave
        confirm_flag = flag

        pending = parsed["pending"]
        payload = parsed["payload"]

        # 3) фильтр: игнорируем нецелевые устройства (57 и любые другие)
        if slave not in TARGET_UNITS:
            # Временно: показываем, что на линии кто-то ещё шлёт события,
            # но без расшифровки адресов и без подтверждения.
            log.info(f"События от постороннего устройства {slave} (игнорируем, ACK не отправляем)")
            time.sleep(0.2)
            continue

        # 4) показываем понятный заголовок
        more_txt = "есть ещё в очереди" if pending else "очередь пуста"
        log.info(f"События от устройства {slave}: {more_txt} (флаг пакета={flag})")

        # 5) подтверждение: подтверждаем ровно то, что получили
        confirm_slave_id = slave
        confirm_flag = flag

        events = parse_events_payload(payload)

        for ev in events:
            ev_type = ev["ev_type"]
            ev_id = ev["ev_id"]

            # спец-событие: reboot (как у тебя)
            if ev_type == 0x0F and ev_id == 0:
                log.info(f"  • Перезагрузка устройства {slave}")
                continue

            rtype = ID_TO_REGTYPE.get(ev_type, "?")
            rtype_ru = REGTYPE_RU.get(rtype, rtype)

            log.info(f"  • Изменение: {rtype_ru}, адрес {ev_id}")

        # если pending=1 — можно опрашивать быстрее, чтобы быстрее выгрузить очередь
        time.sleep(0.2 if pending else 1.0)

if __name__ == "__main__":
    main()