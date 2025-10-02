# app/api/routes/mock.py
from fastapi import APIRouter, HTTPException
import json
from app.services.hot_reload import mqtt_bridge_instance
from app.core.config import settings

router = APIRouter()

@router.post("/api/mock/send_control")
def mock_send_control(topic: str, value: str):
    """
    Эмуляция команды управления: публикует {"value": "..."} в заданный топик.
    Используй для проверки записи без реального фронта/SCADA.
    +
+    Примеры (mosquitto_pub):
+      1) Отправка простым значением:
+         mosquitto_pub -h <broker> -t "<topic>" -m "1"
+
+      2) Отправка JSON-объектом (эквивалентно):
+         mosquitto_pub -h <broker> -t "<topic>" -m '{"value":"1"}'
+
+      Где <topic> — топик команды. Если в конфиге задан base_topic=/devices,
+      и параметр публикуется как /devices/line1/obj/param, то команда обычно идёт
+      в соответствующий топик записи (например, /devices/line1/obj/param/set — см. конфиг).

    """
    if not (getattr(settings, "debug", {}) or {}).get("enabled", False):
        raise HTTPException(403, "Mock доступен только при debug.enabled=true")
    mb = mqtt_bridge_instance()
    if mb is None:
        raise HTTPException(503, "MQTT bridge not ready")
    try:
        mb.client.publish(topic, json.dumps({"value": str(value)}), qos=mb.qos, retain=False)
        return {"ok": True}
    except Exception as e:
        raise HTTPException(500, f"publish failed: {e}")
