# app/services/mqtt_bridge.py
import json, queue, threading
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, Callable
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

        self._handlers: Dict[str, Callable[[str], bool]] = {}  # topic -> handler(value_str)->bool
        self._lock = threading.RLock()

        def _on_connect(c, u, flags, rc, properties=None):
            log.info(f"[mqtt] connected rc={rc}")
            # после реконнекта — заново подпишемся на все темы команд
            with self._lock:
                for t in self._handlers.keys():
                    try:
                        self.client.subscribe(t, qos=self.qos)
                    except Exception as e:
                        log.warning(f"[mqtt] resubscribe failed for {t}: {e}")

        self.client.on_connect = _on_connect
        self.client.on_message = self._on_message  # см. метод ниже

        self.base = conf.get("base_topic", "/devices").rstrip("/")
        if not self.base.startswith("/"): self.base = "/" + self.base
        self.qos = int(conf.get("qos", 0)); self.retain = bool(conf.get("retain", False))
        self.out_queue = queue.Queue()

    def connect(self):
        # не валим процесс, если брокер недоступен
        try:
            # асинхронное подключение + автоповтор внутри paho
            self.client.connect_async(self.conf["host"], int(self.conf["port"]))
        except Exception as e:
            log.error(f"[mqtt] initial connect failed: {e}")
        self.client.loop_start()  # неблокирующий цикл
        threading.Thread(target=self._publisher_loop, daemon=True).start()

    def publish(self, topic_like: str, value, code: int = 0, status_details: Optional[dict] = None, context: Optional[dict] = None):
        topic = topic_like if topic_like.startswith("/") else f"{self.base}/{topic_like}"
        meta = {"timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                "status_code": {"code": int(code)}}
        if status_details: meta["status_code"].update(status_details)
        payload = {"value": (value if value is None else str(value)), "metadata": meta}
        self.out_queue.put((topic, payload, context or {}))

    def register_on_topic(self, topic: str, handler: Callable[[str], bool]) -> None:
        """
        Регистрируем обработчик для команды записи.
        topic может быть относительным — тогда префиксуем base_topic.
        handler получает строковое payload (как есть).
        """
        if not topic.startswith("/"):
            topic = f"{self.base}/{topic}".replace("//", "/")
        with self._lock:
            self._handlers[topic] = handler
        try:
            self.client.subscribe(topic, qos=self.qos)
            log.info(f"[mqtt] subscribed: {topic}")
        except Exception as e:
            # если ещё не подключены — подпишемся в on_connect
            log.debug(f"[mqtt] subscribe deferred for {topic}: {e}")

    def unregister_on_topic(self, topic: str) -> None:
        if not topic.startswith("/"):
            topic = f"{self.base}/{topic}".replace("//", "/")
        with self._lock:
            self._handlers.pop(topic, None)
        try:
            self.client.unsubscribe(topic)
            log.info(f"[mqtt] unsubscribed: {topic}")
        except Exception:
            pass

    def _on_message(self, client, userdata, msg):
        topic = msg.topic
        payload = msg.payload
        try:
            s = payload.decode("utf-8", errors="ignore")
        except Exception:
            s = str(payload)
        handler = None
        with self._lock:
            handler = self._handlers.get(topic)
        if handler:
            try:
                ok = handler(s)
                log.debug(f"[mqtt] handler for {topic} returned {ok}")
            except Exception as e:
                log.error(f"[mqtt] handler error for {topic}: {e}")

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
