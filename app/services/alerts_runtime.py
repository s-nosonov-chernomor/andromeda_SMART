# app/services/alerts_runtime.py
from __future__ import annotations
from typing import Optional
import threading

from app.core.alerts_config import read_alerts_cfg
from app.services.alerts_engine import AlertsEngine  # <- это ваш файл alerts_engine.py

_ENGINE: Optional[AlertsEngine] = None
_LOCK = threading.Lock()

def engine_instance() -> Optional[AlertsEngine]:
    return _ENGINE

def start_engine_if_needed() -> None:
    global _ENGINE
    with _LOCK:
        if _ENGINE is not None:
            return
        cfg = read_alerts_cfg()
        eng = AlertsEngine(cfg)   # конструктор вы уже делали
        eng.start()               # запуск потоков/тасков внутри
        _ENGINE = eng

def stop_engine_if_running() -> None:
    global _ENGINE
    with _LOCK:
        if _ENGINE is None:
            return
        try:
            _ENGINE.stop()
        finally:
            _ENGINE = None
