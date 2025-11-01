# persay/mqtt/__init__.py
"""
Подпакет MQTT — адаптер для публикации сообщений.
В реальном проекте тут подключается paho-mqtt или aio-mqtt.
"""
from .bus import publish

__all__ = ["publish"]
