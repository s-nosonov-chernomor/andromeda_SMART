# app/services/fast_modbus_observer.py
from __future__ import annotations

import time
import struct
import random
import threading
import logging
from typing import Any, List, Dict, Optional, Tuple

log = logging.getLogger("fast_modbus")


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


class FastModbusObserver(threading.Thread):
    """
    Реальный observer fast-modbus поверх той же RS485 линии.

    Работает так:
    - bootstrap: подписка на события (CFG)
    - loop:
        poll(with confirm of previous packet)
        parse response
        yield events to line.ingest_fast_events(...)
    """

    def __init__(self, line, fast_conf: Optional[dict] = None):
        super().__init__(daemon=True)

        self.line = line
        self.fast_conf = fast_conf or {}

        self.enabled = bool(self.fast_conf.get("enabled", False))
        self.interval_s = float(self.fast_conf.get("interval_ms", 50) or 50) / 1000.0
        self.jitter_s = float(self.fast_conf.get("jitter_ms", 0) or 0) / 1000.0
        self.max_events_per_poll = int(self.fast_conf.get("max_events_per_poll", 200) or 200)

        self.bootstrap_pause_s = float(self.fast_conf.get("bootstrap_pause_ms", 20) or 20) / 1000.0
        self.rx_gap_s = float(self.fast_conf.get("rx_gap_ms", 20) or 20) / 1000.0
        self.rx_hard_timeout_s = float(self.fast_conf.get("rx_hard_timeout_ms", 350) or 350) / 1000.0
        self.poll_idle_sleep_s = float(self.fast_conf.get("poll_idle_sleep_ms", 1000) or 1000) / 1000.0
        self.poll_pending_sleep_s = float(self.fast_conf.get("poll_pending_sleep_ms", 200) or 200) / 1000.0

        self.protocol_debug = bool(self.fast_conf.get("protocol_debug", False))

        # тестовый режим оставляем
        self.test_enabled = bool(self.fast_conf.get("test_enabled", False))
        self.test_every_s = float(self.fast_conf.get("test_every_s", 5.0) or 5.0)
        self._last_test_ts = 0.0

        # confirm для следующего POLL
        self._confirm_slave_id = 0
        self._confirm_flag = 0

        self._stop = threading.Event()
        self.log = logging.getLogger(f"fast_modbus.{getattr(line, 'name_', 'line')}")

    def stop(self):
        self._stop.set()

    def run(self):
        if not self.enabled:
            self.log.info("fast modbus observer disabled")
            return

        self.log.info("fast modbus observer started")

        # ждем, пока основной ModbusLine сам откроет хотя бы один instrument
        while not self._stop.is_set():
            inst = self._pick_fast_instrument()
            if inst is not None:
                break
            time.sleep(0.2)

        if self._stop.is_set():
            self.log.info("fast modbus observer stopped before bootstrap")
            return

        try:
            self._bootstrap_subscriptions()
        except Exception as e:
            self.log.error(f"FAST bootstrap failed: {e}")

        while not self._stop.is_set():
            try:
                events = self._poll_fast_events()
                if events:
                    accepted = self.line.ingest_fast_events(events)
                    if accepted:
                        self.log.info(f"FAST accepted events: {accepted}/{len(events)}")
            except Exception as e:
                self.log.error(f"fast observer loop error: {e}")

            sleep_s = self.interval_s
            if self.jitter_s > 0:
                sleep_s += random.uniform(0.0, self.jitter_s)
            time.sleep(max(0.001, sleep_s))

        self.log.info("fast modbus observer stopped")

    # -------------------------------------------------
    # bootstrap
    # -------------------------------------------------
    def _bootstrap_subscriptions(self) -> None:
        subs = self.line.get_fast_subscription_map()
        if not subs:
            self.log.info("FAST bootstrap: no eligible params")
            return

        total_units = len(subs)
        total_regs = sum(
            len(addrs)
            for per_type in subs.values()
            for addrs in per_type.values()
        )

        self.log.info(f"FAST bootstrap subscriptions prepared: units={total_units} regs={total_regs}")

        for unit_id, per_type in subs.items():
            if self._stop.is_set():
                break

            try:
                self.log.info(f"FAST bootstrap unit={unit_id} map={per_type}")

                frame = self._build_cfg_events_frame(unit_id, per_type)
                if frame is None:
                    continue

                resp = self._send_and_recv(frame, tag=f"FAST CFG unit={unit_id}")
                parsed = self._parse_fast_frame(resp) if resp else None

                if parsed and parsed.get("type") == "cfg_resp":
                    if self.protocol_debug:
                        self.log.info(f"FAST CFG unit={unit_id}: OK")
                else:
                    self.log.warning(f"FAST CFG unit={unit_id}: no/invalid response")

            except Exception as e:
                self.log.error(f"FAST CFG unit={unit_id} failed: {e}")

            if self.bootstrap_pause_s > 0:
                slept = 0.0
                while slept < self.bootstrap_pause_s and not self._stop.is_set():
                    time.sleep(0.01)
                    slept += 0.01

    # -------------------------------------------------
    # main polling
    # -------------------------------------------------
    def _poll_fast_events(self) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []

        # test mode
        if self.test_enabled:
            now = time.time()
            if (now - self._last_test_ts) >= self.test_every_s:
                self._last_test_ts = now
                test_events = self._build_test_events()
                if test_events:
                    self.log.info(f"FAST TEST generated events: {test_events}")
                    return test_events[: self.max_events_per_poll]
            return []

        # real mode
        poll_frame = self._build_poll_events_frame(
            confirm_slave_id=self._confirm_slave_id,
            confirm_flag=self._confirm_flag,
        )

        resp = self._send_and_recv(poll_frame, tag="FAST POLL")
        if not resp:
            return []

        parsed = self._parse_fast_frame(resp)
        if not parsed:
            return []

        if parsed["type"] == "no_events":
            return []

        if parsed["type"] != "events":
            return []

        slave = int(parsed["slave"])
        flag = int(parsed["flag"])
        pending = int(parsed["pending"])
        payload = parsed["payload"]

        # ВАЖНО:
        # подтверждаем этот пакет в следующем poll
        self._confirm_slave_id = slave
        self._confirm_flag = flag

        decoded_events = self._parse_events_payload(payload)

        for ev in decoded_events:
            ev_type = int(ev["ev_type"])
            ev_id = int(ev["ev_id"])

            # reboot / системное событие
            if ev_type == 0x0F and ev_id == 0:
                self.log.info(f"FAST reboot event from unit={slave}")
                continue

            register_type = ID_TO_REGTYPE.get(ev_type)
            if not register_type:
                continue

            events.append({
                "unit_id": slave,
                "register_type": register_type,
                "addr": ev_id,
            })

        if self.protocol_debug:
            self.log.info(
                f"FAST POLL parsed: slave={slave} flag={flag} pending={pending} "
                f"events={events}"
            )

        # если pending=1, устройство говорит что в очереди есть еще события
        if pending:
            time.sleep(self.poll_pending_sleep_s)
        else:
            time.sleep(self.poll_idle_sleep_s)

        return events[: self.max_events_per_poll]

    # -------------------------------------------------
    # frame builders
    # -------------------------------------------------
    def _build_cfg_events_frame(self, slave_id: int, per_type: Dict[str, List[int]]) -> Optional[bytes]:
        blocks = self._build_blocks_from_subscription_map(per_type)
        if not blocks:
            return None

        payload = bytearray()

        for (rtype, start, settings) in blocks:
            type_id = REGTYPE_TO_ID[rtype]
            count = len(settings)
            payload += struct.pack(">BHB", type_id, start & 0xFFFF, count)
            payload += bytes(settings)

        frame_wo_crc = bytes([slave_id, CMD_EXT, SUB_CFG, len(payload)]) + bytes(payload)
        return add_crc(frame_wo_crc)

    def _build_poll_events_frame(self, confirm_slave_id: int, confirm_flag: int) -> bytes:
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

    def _build_blocks_from_subscription_map(self, per_type: Dict[str, List[int]]) -> List[Tuple[str, int, List[int]]]:
        blocks: List[Tuple[str, int, List[int]]] = []

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

        return blocks

    # -------------------------------------------------
    # serial helpers
    # -------------------------------------------------
    def _pick_fast_instrument(self):
        """
        Берем ТОЛЬКО уже открытый instrument из ModbusLine.
        НИЧЕГО не открываем сами, чтобы не было гонки за COM-порт.
        """
        try:
            for nd in self.line.nodes:
                inst = self.line._instruments.get(int(nd.unit_id))
                if inst is not None and getattr(inst, "serial", None) is not None:
                    return inst
        except Exception:
            pass
        return None

    def _send_and_recv(self, frame: bytes, tag: str = "FAST TXRX") -> Optional[bytes]:
        if self.line.transport == "tcp":
            return None

        inst = self._pick_fast_instrument()
        if inst is None:
            return None

        ser = getattr(inst, "serial", None)
        if ser is None:
            return None

        with self.line._io_lock:
            old_timeout = getattr(ser, "timeout", None)
            try:
                # ВАЖНО: только неблокирующий режим
                ser.timeout = 0

                try:
                    ser.reset_input_buffer()
                except Exception:
                    pass

                try:
                    ser.write(frame)
                    ser.flush()
                    if self.protocol_debug:
                        self.log.info(f"{tag} TX: {frame.hex(' ')}")
                except Exception as e:
                    self.log.error(f"{tag} write error: {e}")
                    return None

                resp = self._read_frame_gap(
                    ser,
                    gap_s=self.rx_gap_s,
                    hard_timeout_s=self.rx_hard_timeout_s,
                    max_bytes=512,
                )

                if self.protocol_debug:
                    if resp:
                        self.log.info(f"{tag} RX: {resp.hex(' ')}")
                    else:
                        self.log.info(f"{tag} RX: <empty>")

                return resp

            finally:
                try:
                    ser.timeout = old_timeout
                except Exception:
                    pass

    def _read_frame_gap(
        self,
        ser,
        gap_s: float = 0.02,
        hard_timeout_s: float = 0.35,
        max_bytes: int = 512,
    ) -> Optional[bytes]:
        """
        Неблокирующее чтение одного fast-modbus кадра.
        Не висит на ser.read(1), корректно останавливается по timeout и stop().
        """
        t0 = time.time()
        buf = bytearray()
        last_rx = None

        while not self._stop.is_set():
            now = time.time()

            # жесткий timeout
            if (now - t0) >= hard_timeout_s:
                break

            try:
                waiting = int(getattr(ser, "in_waiting", 0) or 0)
            except Exception:
                waiting = 0

            if waiting > 0:
                try:
                    chunk = ser.read(waiting)
                except Exception:
                    break

                if chunk:
                    buf += chunk
                    last_rx = now

                    if len(buf) >= max_bytes:
                        break

                    exp_len = self._fast_expected_len(bytes(buf))
                    if exp_len is not None and len(buf) >= exp_len:
                        break

                    continue

            # если уже что-то приняли и пауза между байтами выдержана — считаем кадр завершенным
            if buf and last_rx and (now - last_rx) >= gap_s:
                break

            time.sleep(0.001)

        return bytes(buf) if buf else None

    def _fast_expected_len(self, frame_prefix: bytes) -> Optional[int]:
        """
        Пытаемся определить полную длину fast-modbus кадра по уже принятым байтам.
        Возвращает общую длину кадра ВКЛЮЧАЯ CRC, либо None если пока рано.
        """
        if not frame_prefix:
            return None

        # возможны лидирующие FF
        fp = frame_prefix.lstrip(b"\xFF")
        if len(fp) < 3:
            return None

        sub = fp[2]

        # CFG response: в твоем логе пришло 7 байт:
        # 13 46 18 01 3f 75 0c
        # То есть: slave, cmd, sub, len, payload..., crc2
        if sub == SUB_CFG:
            if len(fp) < 4:
                return None
            paylen = fp[3]
            return len(frame_prefix) - len(fp) + 4 + paylen + 2

        # NO_EVENTS обычно короткий фиксированный кадр
        # fd 46 12 ... crc
        if sub == SUB_NO_EVENTS:
            # тут safest путь: короткий кадр, обычно 5 байт тела + 2 crc = 7
            return len(frame_prefix) - len(fp) + 7

        # EVENTS:
        # slave, cmd, sub, flag, pending, paylen, payload..., crc2
        if sub == SUB_EVENTS:
            if len(fp) < 6:
                return None
            paylen = fp[5]
            return len(frame_prefix) - len(fp) + 6 + paylen + 2

        return None


    # -------------------------------------------------
    # parsers
    # -------------------------------------------------
    def _parse_fast_frame(self, frame: bytes) -> Optional[Dict[str, Any]]:
        if not frame:
            return None

        frame = frame.lstrip(b"\xFF")

        if len(frame) < 4:
            return None

        if not check_crc(frame):
            if self.protocol_debug:
                self.log.info(f"FAST bad crc: {frame.hex(' ')}")
            return None

        slave = frame[0]
        sub = frame[2]

        if sub == SUB_NO_EVENTS:
            return {"type": "no_events"}

        if sub == SUB_EVENTS:
            if len(frame) < 6:
                return None

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

    def _parse_events_payload(self, payload: bytes) -> List[Dict[str, Any]]:
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
    # test mode
    # -------------------------------------------------
    def _build_test_events(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []

        try:
            picked = 0
            for nd in self.line.nodes:
                for p in nd.params:
                    if not self.line._is_fast_eligible_param(p):
                        continue

                    out.append({
                        "unit_id": int(nd.unit_id),
                        "register_type": str(p.register_type),
                        "addr": int(self.line._normalize_addr(p)),
                    })
                    picked += 1

                    if picked >= 3:
                        return out
        except Exception as e:
            self.log.error(f"build test events error: {e}")

        return out