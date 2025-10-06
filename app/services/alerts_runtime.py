# app/services/alerts_runtime.py
from __future__ import annotations
from typing import Optional
import threading

from app.services.alerts_engine import alerts_engine, AlertsEngine  # singleton и класс

_LOCK = threading.Lock()
_STARTED = False

def ensure_started() -> AlertsEngine:
    """Гарантированно запускает движок один раз и возвращает инстанс."""
    global _STARTED
    with _LOCK:
        if not _STARTED:
            alerts_engine.start()   # он сам перечитает alerts.yaml
            _STARTED = True
    return alerts_engine

def engine_instance() -> Optional[AlertsEngine]:
    """Если уже запущен — вернёт инстанс, иначе None (для честной диагностики)."""
    return alerts_engine if _STARTED else None

def stop_if_running() -> None:
    global _STARTED
    with _LOCK:
        if _STARTED:
            try:
                alerts_engine.stop()
            finally:
                _STARTED = False
