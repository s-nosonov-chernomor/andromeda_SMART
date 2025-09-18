# app/api/routes/mock.py
from fastapi import APIRouter, HTTPException
import json
from app.services.hot_reload import mqtt_bridge_instance

router = APIRouter()

@router.post("/api/mock/send_control")
def mock_send_control(topic: str, value: str):
    """
    Эмуляция команды управления: публикует {"value": "..."} в заданный топик.
    Используй для проверки записи без реального фронта/SCADA.
    """
    mb = mqtt_bridge_instance()
    if mb is None:
        raise HTTPException(503, "MQTT bridge not ready")
    try:
        mb.client.publish(topic, json.dumps({"value": str(value)}), qos=mb.qos, retain=False)
        return {"ok": True}
    except Exception as e:
        raise HTTPException(500, f"publish failed: {e}")
