import json
import time
import random
import threading
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional, Any, List

import pandas as pd
import paho.mqtt.client as mqtt


# =========================
# Config
# =========================
MQTT_HOST = "127.0.0.1"
MQTT_PORT = 1883
MQTT_USERNAME = None  # "user"
MQTT_PASSWORD = None  # "pass"
MQTT_CLIENT_ID = "fake-persay"

EXCEL_PATH = r"topics.xlsx"
SHEET_NAME = 0  # None = первый лист

# Если нужно добавлять префикс ко всем топикам:
TOPIC_PREFIX = ""  # например "site1/"


# =========================
# Logging
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("fake_persay")


@dataclass
class TopicSpec:
    topic: str
    mode: str  # R or RW
    vmin: float
    vmax: float
    interval_s: float


def iso_utc_now() -> str:
    # "2026-02-13T09:38:16.977Z"
    dt = datetime.now(timezone.utc)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def build_payload(value: Any, trigger: str, silent_for_s: int = 0) -> str:
    payload = {
        "value": str(value),
        "metadata": {
            "timestamp": iso_utc_now(),
            "status_code": {
                "source": "persay",
                "code": 0,
                "message": "OK",
                "silent_for_s": silent_for_s,
                "trigger": trigger,
            },
        },
    }
    return json.dumps(payload, ensure_ascii=False)


def read_specs_from_excel(path: str, sheet_name: Optional[str]) -> List[TopicSpec]:
    df = pd.read_excel(path, sheet_name=sheet_name)

    # Если вернулся dict (sheet_name=None) — берём первый лист
    if isinstance(df, dict):
        df = list(df.values())[0]

    df.columns = [str(c).strip().lower() for c in df.columns]

    required = {"topic", "mode", "min", "max", "interval_s"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"В Excel не хватает колонок: {sorted(missing)}")

    specs: List[TopicSpec] = []
    for i, row in df.iterrows():
        topic = str(row["topic"]).strip()
        if not topic or topic.lower() == "nan":
            continue

        mode = str(row["mode"]).strip().upper()
        if mode not in ("R", "RW"):
            raise ValueError(f"Строка {i+2}: mode должен быть R или RW, а не {mode}")

        vmin = float(row["min"])
        vmax = float(row["max"])
        interval_s = float(row["interval_s"])

        full_topic = f"{TOPIC_PREFIX}{topic}" if TOPIC_PREFIX else topic
        specs.append(TopicSpec(full_topic, mode, vmin, vmax, interval_s))

    return specs



class FakePersay:
    def __init__(self, specs: List[TopicSpec]):
        self.specs = specs
        self.client = mqtt.Client(client_id=MQTT_CLIENT_ID, clean_session=True)

        if MQTT_USERNAME:
            self.client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

        self._rw_last_value: Dict[str, str] = {}  # base_topic -> last cmd value as string
        self._rw_last_cmd_ts: Dict[str, float] = {}  # base_topic -> unix ts

        self._lock = threading.Lock()
        self._stop = threading.Event()

        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    def on_connect(self, client, userdata, flags, rc):
        log.info("MQTT connected rc=%s", rc)
        # подписки на RW управление
        for s in self.specs:
            if s.mode == "RW":
                cmd_topic = s.topic.rstrip("/") + "/on"
                client.subscribe(cmd_topic, qos=0)
                log.info("Subscribed: %s", cmd_topic)

    def on_message(self, client, userdata, msg):
        topic = msg.topic
        payload_raw = msg.payload.decode("utf-8", errors="replace")

        # ожидаем base/on
        if not topic.endswith("/on"):
            return
        base = topic[:-3]  # remove "/on"

        try:
            data = json.loads(payload_raw)
            value = data.get("value")
            if value is None:
                raise ValueError("нет поля value")
        except Exception as e:
            log.warning("Bad cmd payload on %s: %s (%s)", topic, payload_raw, e)
            return

        with self._lock:
            self._rw_last_value[base] = str(value)
            self._rw_last_cmd_ts[base] = time.time()

        # мгновенная публикация в основной топик trigger=change
        out = build_payload(value=str(value), trigger="change", silent_for_s=0)
        self.client.publish(base, out, qos=0, retain=False)
        log.info("CMD %s -> publish %s value=%s", topic, base, value)

    def start(self):
        self.client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        self.client.loop_start()

        threads = []
        for s in self.specs:
            t = threading.Thread(target=self._publisher_loop, args=(s,), daemon=True)
            t.start()
            threads.append(t)

        log.info("Fake PERSAY started. Topics=%d", len(self.specs))
        try:
            while True:
                time.sleep(0.5)
        except KeyboardInterrupt:
            log.info("Stopping...")
            self._stop.set()
            self.client.loop_stop()
            self.client.disconnect()

    def _publisher_loop(self, spec: TopicSpec):
        next_t = time.time()
        while not self._stop.is_set():
            now = time.time()
            if now < next_t:
                time.sleep(min(0.2, next_t - now))
                continue

            if spec.mode == "R":
                val = random.uniform(spec.vmin, spec.vmax)
                # по желанию можешь округлять:
                val = round(val, 2)
                payload = build_payload(value=val, trigger="interval", silent_for_s=0)
                self.client.publish(spec.topic, payload, qos=0, retain=False)
                # log.debug(...)
                log.info("PUB R  %s = %s", spec.topic, val)

            else:  # RW
                with self._lock:
                    if spec.topic in self._rw_last_value:
                        val = self._rw_last_value[spec.topic]
                        last_ts = self._rw_last_cmd_ts.get(spec.topic, time.time())
                    else:
                        val = str(spec.vmin)
                        last_ts = None

                silent_for_s = 0
                if last_ts is not None:
                    silent_for_s = int(max(0, time.time() - last_ts))

                payload = build_payload(value=val, trigger="interval", silent_for_s=silent_for_s)
                self.client.publish(spec.topic, payload, qos=0, retain=False)
                log.info("PUB RW %s = %s (silent_for_s=%s)", spec.topic, val, silent_for_s)

            next_t = time.time() + spec.interval_s


if __name__ == "__main__":
    specs = read_specs_from_excel(EXCEL_PATH, SHEET_NAME)
    app = FakePersay(specs)
    app.start()
