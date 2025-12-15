# app/persay/runtime.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional, Dict, Callable
import logging

from app.persay.rules.engine import RuleEngine
from app.persay.rules.actions import ActionExecutor
from app.persay.rules.virtual_tags import VirtualTagRegistry
from app.persay.rules.repositories import InMemoryRuleStorage, InMemoryActionLogStorage
from app.persay.rules.storage import RulesRepository
from app.persay.rules.types import TagValue, TagSource, TagQuality

log = logging.getLogger("automation")

# Глобальные синглтоны
_ENGINE: Optional[RuleEngine] = None
_VTAGS: Optional[VirtualTagRegistry] = None
_REPO: Optional[RulesRepository] = None


def _make_mqtt_publish_adapter(mqtt_bridge) -> Callable[[str, Any], None]:
    """
    Адаптер: ActionExecutor ожидает mqtt_publish(topic, payload),
    мы внутри используем твой MqttBridge.publish(...)

    ДОПОЛНИТЕЛЬНО:
    - для топиков вида /devices/.../controls/<tag>/on
      сразу же "отзеркаливаем" команду в движок правил через notify_tag_update,
      чтобы он мгновенно знал новое значение тега, не дожидаясь опроса.
    """
    def _publish(topic: str, payload: Any) -> None:
        try:
            mqtt_bridge.publish(topic, payload, code=0)
        except Exception as e:
            log.error("mqtt_publish adapter error: %s", e)
            return

        # Попробуем сразу обновить тег в RuleEngine, если это командный топик controls/.../on
        try:
            parts = topic.split("/")
            # ожидаем что-то вроде: /devices/<obj>/controls/<tag>/on
            if len(parts) >= 5 and parts[-1] == "on" and parts[-3] == "controls":
                tag_name = parts[-2]

                # Достаём значение
                if isinstance(payload, dict):
                    raw_val = payload.get("value", payload.get("v", None))
                else:
                    raw_val = payload

                if raw_val is None:
                    return

                # Немного нормализуем: "1"/"0" → числа, остальные оставляем как есть
                val = raw_val
                if isinstance(raw_val, str):
                    s = raw_val.strip().replace(",", ".")
                    try:
                        val = float(s)
                    except Exception:
                        val = raw_val  # пусть остаётся строкой

                # Используем уже существующий вход в движок
                notify_tag_update(
                    name=tag_name,
                    value=val,
                    source="mqtt",
                    meta={"from": "automation_action", "topic": topic},
                )

        except Exception as e:
            # На всякий случай не роняем ничего из-за этого
            log.debug("mirror tag_update from action failed: %s", e)

    return _publish


def ensure_automation_started(mqtt_bridge: Any) -> None:
    """
    Инициализировать движок автоматизации, если ещё не инициализирован.
    Вызываем один раз на старте приложения (после создания MqttBridge).
    """
    global _ENGINE, _VTAGS, _REPO
    if _ENGINE is not None:
        return

    # Хранилища правил и журнала — пока in-memory
    rules_storage = InMemoryRuleStorage()
    log_storage = InMemoryActionLogStorage()
    repo = RulesRepository(rules_storage, log_storage)

    vtags = VirtualTagRegistry()

    actions = ActionExecutor(
        mqtt_publish=_make_mqtt_publish_adapter(mqtt_bridge),
        trigger_rule=None,  # будет использовать RuleEngine.trigger_rule
        write_action_log=repo.append_action_log,
    )

    engine = RuleEngine(
        rules_repo=repo,
        actions=actions,
        virtual_tags=vtags,
    )

    _ENGINE = engine
    _VTAGS = vtags
    _REPO = repo


def engine_instance() -> Optional[RuleEngine]:
    """Вернёт текущий инстанс RuleEngine (или None, если не инициализирован)."""
    return _ENGINE

def rules_repo() -> Optional[RulesRepository]:
    """Вернёт репозиторий правил/журнала, если автоматика инициализирована."""
    return _REPO


def notify_tag_update(
    name: str,
    value: Any,
    *,
    source: str = "mqtt",
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Внешняя точка входа: «обновился тег name со значением value».

    ВАЖНО: здесь name = ИМЯ ПАРАМЕТРА ИЗ YAML, например "мощ1".
    Никаких line.unit.object — только param.
    """
    if not name:
        return

    engine = _ENGINE
    if engine is None:
        return

    src = TagSource.MQTT if source.lower() == "mqtt" else TagSource.DEVICE

    # Попробуем привести value к числу, если возможно
    val = value
    if isinstance(value, str):
        vs = value.strip().replace(",", ".")
        try:
            val = float(vs)
        except Exception:
            # остаётся строкой
            val = value

    tv = TagValue(
        name=name,
        value=val,
        ts=datetime.utcnow(),
        quality=TagQuality.GOOD,
        source=src,
        meta=meta or {},
    )

    engine.handle_tag_update(tv)
