# app/services/hot_reload.py
"""
Управление линиями опроса Modbus (старт/стоп/горячий перезапуск).

Использование:
  from app.services.hot_reload import start_lines, stop_lines, hot_reload_lines

  # при старте приложения (после инициализации MQTT-моста):
  start_lines(settings.cfg, mqtt_bridge)

  # при изменении конфигурации через веб (меняем только polling/lines):
  hot_reload_lines(new_cfg)

Примечания:
- Горячая перезагрузка затрагивает ТОЛЬКО секции 'lines' и 'polling'.
  Изменения 'mqtt' и 'db' сохраняются в конфиг, но применяются
  только после рестарта процесса (безопасность соединений).
- Потоки аккуратно останавливаются (stop + join) с таймаутами.
- Все операции атомарны относительно LINES_LOCK.
"""

from __future__ import annotations
from typing import List, Optional, Dict, Any
import threading
import logging
import time

from app.services.modbus_line import ModbusLine
from app.services.current_store import current_store

# Глобальные объекты состояния
_LINES: List[ModbusLine] = []
_LINES_LOCK = threading.Lock()
_CURRENT_CFG: Optional[Dict[str, Any]] = None
_MQTT_BRIDGE = None  # тип: app.services.mqtt_bridge.MqttBridge (строковый хинт, чтобы избежать циклов)

_log = logging.getLogger("hot")


def _stop_all_lines_unlocked() -> None:
    """Остановить все активные линии. Предполагается, что LOCK уже взят."""
    global _LINES
    if not _LINES:
        return
    for ln in _LINES:
        try:
            ln.stop()
        except Exception as e:
            _log.warning(f"line stop error ({getattr(ln, 'name_', '?')}): {e}")
    # дождаться окончания потоков (не зависать бесконечно)
    for ln in _LINES:
        try:
            ln.join(timeout=2.0)
        except Exception:
            pass
    _LINES = []


def start_lines(cfg: Dict[str, Any], mqtt_bridge) -> None:
    """
    Полный старт линий на основе cfg и уже созданного mqtt_bridge.
    Если линии были запущены ранее — они будут остановлены и запущены заново.
    """
    current_store.reset_from_cfg(cfg) # новое: пересобираем список «ожидаемых» параметров
    global _CURRENT_CFG, _MQTT_BRIDGE, _LINES

    if mqtt_bridge is None:
        raise RuntimeError("MQTT bridge is not initialized (passed None to start_lines)")

    polling = cfg.get("polling", {}) or {}
    lines_conf = cfg.get("lines", []) or []
    serial_echo = bool(cfg.get("serial", {}).get("echo", False))


    with _LINES_LOCK:
        # Остановить то, что было
        _stop_all_lines_unlocked()

        # Запустить заново
        _LINES = []
        started = 0
        for lc in lines_conf:
            try:
                line = ModbusLine(lc, mqtt_bridge, polling, serial_echo=serial_echo)
                line.start()
                _LINES.append(line)
                started += 1
            except Exception as e:
                _log.error(f"can't start line '{lc.get('name','?')}': {e}")

        _CURRENT_CFG = cfg
        _MQTT_BRIDGE = mqtt_bridge

    _log.info(f"lines started: {started}/{len(lines_conf)}")


def stop_lines() -> None:
    """Полная остановка всех линий (используется при shutdown приложения)."""
    with _LINES_LOCK:
        _stop_all_lines_unlocked()
    _log.info("all lines stopped")


def hot_reload_lines(new_cfg: Dict[str, Any]) -> None:
    """
    Горячая перезагрузка ТОЛЬКО секций 'lines' и 'polling'.
    Остальные изменения (например, mqtt/db) сохраняются в конфиге вызывающей стороной,
    но физически будут применены после рестарта процесса.

    Поведение:
    - Останавливаем все текущие линии
    - Запускаем по списку из new_cfg
    - Обновляем _CURRENT_CFG ссылкой на new_cfg
    """
    global _CURRENT_CFG

    with _LINES_LOCK:
        if _MQTT_BRIDGE is None:
            # Если мост ещё не инициализирован (например, ранний вызов),
            # просто обновим конфиг — старт произойдет при первом start_lines().
            _CURRENT_CFG = new_cfg
            _log.warning("hot_reload_lines called before MQTT bridge init; lines not started yet.")
            return

        _stop_all_lines_unlocked()
        # синхронизируем список параметров «текущих» с новым YAML
        current_store.reset_from_cfg(new_cfg)


        polling = new_cfg.get("polling", {}) or {}
        lines_conf = new_cfg.get("lines", []) or []
        serial_echo = bool(new_cfg.get("serial", {}).get("echo", False))

        started = 0
        for lc in lines_conf:
            try:
                line = ModbusLine(lc, _MQTT_BRIDGE, polling, serial_echo=serial_echo)
                line.start()
                _LINES.append(line)
                started += 1
            except Exception as e:
                _log.error(f"can't start line '{lc.get('name','?')}' on reload: {e}")

        _CURRENT_CFG = new_cfg

    _log.info(f"hot reload complete: lines started {started}/{len(lines_conf)}")


def get_lines_status() -> Dict[str, Any]:
    """
    Небольшой сервисный хелпер: вернуть статус по линиям — имена/живы ли потоки.
    Можно повесить на отладочный эндпоинт при необходимости.
    """
    with _LINES_LOCK:
        return {
            "count": len(_LINES),
            "lines": [
                {
                    "name": getattr(ln, "name_", "?"),
                    "alive": ln.is_alive(),
                    "port": getattr(ln, "port", "?"),
                    "baudrate": getattr(ln, "baudrate", "?"),
                }
                for ln in _LINES
            ]
        }


def current_cfg() -> Optional[Dict[str, Any]]:
    """Текущий активный конфиг (ссылка)."""
    return _CURRENT_CFG


def mqtt_bridge_instance():
    """Текущий MQTT-мост (для отладочных задач)."""
    return _MQTT_BRIDGE
