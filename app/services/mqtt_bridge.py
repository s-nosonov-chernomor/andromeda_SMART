# app/services/mqtt_bridge.py
import json, queue, threading
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any
import paho.mqtt.client as mqtt
from sqlalchemy.orm import Session
from app.db.session import SessionLocal
from app.db.models import TelemetryEvent
from app.core.config import settings
from app.services.current_store import current_store  # ← ДОБАВИЛИ
import logging

log = logging.getLogger("mqtt")

class MqttBridge:
    def __init__(self, conf: dict):
        self.conf = conf
        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=conf.get("client_id", ""),
            protocol=mqtt.MQTTv311,
        )
        self.client.on_connect = lambda c,u,f,rc,p=None: log.info(f"[mqtt] connected rc={rc}")
        self.base = conf.get("base_topic", "/devices").rstrip("/")
        if not self.base.startswith("/"): self.base = "/" + self.base
        self.qos = int(conf.get("qos", 0)); self.retain = bool(conf.get("retain", False))
        self.out_queue = queue.Queue()

    def connect(self):
        self.client.connect(self.conf["host"], int(self.conf["port"]))
        threading.Thread(target=self.client.loop_forever, daemon=True).start()
        threading.Thread(target=self._publisher_loop, daemon=True).start()

    def publish(self, topic_like: str, value, code: int = 0, status_details: Optional[dict] = None, context: Optional[dict] = None):
        topic = topic_like if topic_like.startswith("/") else f"{self.base}/{topic_like}"
        meta = {"timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                "status_code": {"code": int(code)}}
        if status_details: meta["status_code"].update(status_details)
        payload = {"value": (value if value is None else str(value)), "metadata": meta}
        self.out_queue.put((topic, payload, context or {}))

    def _publisher_loop(self):
        H = settings.history
        cleanup_every = int(H.get("cleanup_every", 500) or 500)
        ttl_days = int(H.get("ttl_days", 0) or 0)
        max_rows = int(H.get("max_rows", 0) or 0)
        i = 0
        while True:
            topic, payload, ctx = self.out_queue.get()
            try:
                self.client.publish(topic, json.dumps(payload), qos=self.qos, retain=self.retain)

                # обновляем «текущие» (ts — только при code==0)
                current_store.apply_publish(ctx, payload, datetime.now(timezone.utc))

                # пишем историю
                with SessionLocal() as s:
                    evt = TelemetryEvent(
                        topic=topic,
                        object=ctx.get("object",""),
                        line=ctx.get("line",""),
                        unit_id=ctx.get("unit_id",0),
                        register_type=ctx.get("register_type",""),
                        address=ctx.get("address",0),
                        param=ctx.get("param",""),
                        value=payload["value"],
                        code=int(payload["metadata"]["status_code"]["code"]),
                        message=str(payload["metadata"]["status_code"].get("message","OK")),
                        silent_for_s=int(payload["metadata"]["status_code"].get("silent_for_s",0)),
                        ts=datetime.utcnow(),
                    )
                    s.add(evt); s.commit()
                    i += 1
                    if i % cleanup_every == 0:
                        if ttl_days>0:
                            s.execute("DELETE FROM telemetry_events WHERE ts < :cutoff",
                                      {"cutoff": datetime.utcnow()-timedelta(days=ttl_days)})
                        if max_rows>0:
                            s.execute("""
                                DELETE FROM telemetry_events
                                WHERE id IN (
                                  SELECT id FROM telemetry_events
                                  ORDER BY id DESC
                                  LIMIT -1 OFFSET :keep
                                )""", {"keep": max_rows})
                        s.commit()
            except Exception as e:
                log.error(f"publish error: {e}")
