# app/api/routes/mock.py
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Any, Optional
import json

from app.services.hot_reload import mqtt_bridge_instance
from app.core.config import settings

from fastapi.responses import JSONResponse
import binascii

router = APIRouter()


class SendControlDTO(BaseModel):
    # Вариант 1: отдать готовый topic
    topic: Optional[str] = None
    # Вариант 2: собрать topic из составляющих
    line: Optional[str] = None
    unit_id: Optional[int] = None
    param: Optional[str] = None

    value: Any                     # что отправляем (будет приведено к строке)
    as_json: bool = False          # true -> {"value":"..."}; false -> "..."
    qos: Optional[int] = None      # если не задано, используем mb.qos
    retain: Optional[bool] = None  # по умолчанию False


def _normalize_base_topic(s: str) -> str:
    if not s:
        s = "/devices"
    s = s.strip()
    if not s.startswith("/"):
        s = "/" + s
    return s.rstrip("/")


@router.post("/api/mock/send_control")
def mock_send_control(dto: SendControlDTO):
    # Защита
    if not (getattr(settings, "debug", {}) or {}).get("enabled", False):
        raise HTTPException(403, "Mock доступен только при debug.enabled=true")

    mb = mqtt_bridge_instance()
    if mb is None:
        raise HTTPException(503, "MQTT bridge not ready")

    # 1) topic
    topic = dto.topic
    if not topic:
        if not dto.line or dto.unit_id is None or not dto.param:
            raise HTTPException(400, "Укажите либо 'topic', либо (line, unit_id, param)")
        base_cfg = getattr(settings, "mqtt", {}) if hasattr(settings, "mqtt") else {}
        base = _normalize_base_topic(base_cfg.get("base_topic", "/devices"))
        topic = f"{base}/write/{dto.line}/{int(dto.unit_id)}/{dto.param}"
    if not topic.startswith("/"):
        topic = "/" + topic

    # 2) payload
    val_str = "" if dto.value is None else str(dto.value)
    if dto.as_json:
        payload_obj = {"value": val_str}
        payload = json.dumps(payload_obj, ensure_ascii=False)
        payload_for_echo = payload_obj
    else:
        payload = val_str
        payload_for_echo = val_str

    qos = dto.qos if dto.qos is not None else getattr(mb, "qos", 0)
    retain = dto.retain if dto.retain is not None else False

    try:
        # ВНИМАНИЕ: paho принимает str и сам кодирует в UTF-8
        mb.client.publish(topic, payload, qos=qos, retain=retain)
    except Exception as e:
        raise HTTPException(500, f"publish failed: {e}")

    # Для проверки в консоли: гекс-дамп UTF-8 байт топика
    topic_utf8_hex = binascii.hexlify(topic.encode("utf-8")).decode("ascii")

    return JSONResponse(
        content={
            "ok": True,
            "topic": topic,
            "topic_utf8_hex": topic_utf8_hex,
            "payload": payload_for_echo,
            "qos": qos,
            "retain": retain,
        },
        media_type="application/json; charset=utf-8",
    )

# Invoke-RestMethod -Method POST "http://localhost:8080/api/mock/send_control" `
#   -ContentType "application/json" `
#   -Body (@{ topic="/devices/RELE_1/controls/contact1/on"; value="1"; as_json=$true } | ConvertTo-Json)
