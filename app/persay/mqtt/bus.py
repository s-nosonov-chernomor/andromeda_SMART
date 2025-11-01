# persay/mqtt/bus.py
import json
import logging
from typing import Any

_logger = logging.getLogger("persay.mqtt")


def publish(topic: str, payload: Any) -> None:
    """
    Универсальный адаптер публикации сообщений.

    В проде это будет реальная отправка в брокер MQTT.
    Пока просто логирует или печатает JSON.
    """
    try:
        data = payload
        if not isinstance(payload, str):
            data = json.dumps(payload, ensure_ascii=False)
        _logger.info("MQTT PUBLISH: %s → %s", topic, data)
        # тут будет вызов mqtt_client.publish(topic, data, qos=0, retain=False)
    except Exception as exc:  # noqa: BLE001
        _logger.exception("Ошибка публикации в MQTT: %s", exc)
