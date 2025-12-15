# persay/rules/actions.py
from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Optional

from .types import (
    Action,
    ActionLogEntry,
    ActionResult,
    ActionStatus,
    ActionType,
    Rule,
)


# ---- типы коллбеков, которые нам нужно будет передать снаружи --------------

# Публикация в MQTT: topic, payload(dict|str)
MqttPublishFunc = Callable[[str, Any], None]

# Запуск другого правила: rule_id, контекст
TriggerRuleFunc = Callable[[str, Dict[str, Any]], None]

# Запись в журнал: ActionLogEntry
ActionLogWriter = Callable[[ActionLogEntry], None]


class ActionExecutor:
    """
    Исполняет действия правила.

    Мы специально не шьём сюда конкретный MQTT или конкретную БД.
    Всё, что нужно — передаётся в __init__ коллбеками.
    """

    def __init__(
        self,
        *,
        mqtt_publish: Optional[MqttPublishFunc] = None,
        trigger_rule: Optional[TriggerRuleFunc] = None,
        write_action_log: Optional[ActionLogWriter] = None,
    ) -> None:
        self._mqtt_publish = mqtt_publish
        self._trigger_rule = trigger_rule
        self._write_action_log = write_action_log

    # --------------------------------------------------------------------- #
    # ПУБЛИЧНЫЙ МЕТОД: выполнить ОДНО действие
    # --------------------------------------------------------------------- #
    def execute_action(
        self,
        rule: Rule,
        action: Action,
        context: Optional[Dict[str, Any]] = None,
    ) -> ActionResult:
        """
        Выполнить одно действие и вернуть результат.
        Тут же пишем в журнал (если задан writer).
        """
        started_at = datetime.utcnow()

        # задержка, если указана
        if action.delay_ms and action.delay_ms > 0:
            time.sleep(action.delay_ms / 1000.0)

        try:
            if action.type == ActionType.MQTT_PUBLISH:
                self._do_mqtt_publish(action, context)
            elif action.type == ActionType.TRIGGER_RULE:
                self._do_trigger_rule(action, context)
            elif action.type == ActionType.LOG:
                self._do_log(action, context)
            else:
                # неизвестный тип действия — считаем ошибкой
                raise ValueError(f"Unsupported action type: {action.type}")

            result = ActionResult(
                action_id=action.id,
                type=action.type,
                status=ActionStatus.SUCCESS,
                ts=started_at,
                details={},
            )

        except Exception as exc:  # noqa: BLE001
            # тут мы не падаем, а возвращаем FAILED
            err_txt = str(exc)
            result = ActionResult(
                action_id=action.id,
                type=action.type,
                status=ActionStatus.FAILED,
                ts=started_at,
                error=err_txt,
                details={},
            )

        # пишем в журнал, если надо
        if self._write_action_log is not None:
            entry = ActionLogEntry(
                ts=result.ts,
                rule_id=rule.id,
                rule_name=rule.name,
                action_id=action.id,
                action_type=action.type,
                status=result.status,
                error=result.error,
                payload_preview=self._build_payload_preview(action),
                context=context or {},
            )
            self._write_action_log(entry)

        return result

    # --------------------------------------------------------------------- #
    # ПУБЛИЧНЫЙ МЕТОД: выполнить НЕСКОЛЬКО действий (ветка)
    # --------------------------------------------------------------------- #
    def execute_actions(
        self,
        rule: Rule,
        actions: Iterable[Action],
        *,
        sequential: bool = True,
        context: Optional[Dict[str, Any]] = None,
    ) -> List[ActionResult]:
        """
        Выполнить список действий.
        sequential=True → выполняем по порядку (если одно упало — идём дальше, но фиксируем).
        sequential=False → в лоб по списку (фактически то же самое, т.к. мы синхронны).
        """
        results: List[ActionResult] = []

        for act in actions:
            res = self.execute_action(rule, act, context=context)
            results.append(res)

            # если последовательный режим, и вдруг нужно когда-то останавливать
            # на ошибке — вот тут можно будет добавить условие.
            # пока по твоему желанию — "за этим следит пользователь", поэтому не стопаем.

        return results

    # --------------------------------------------------------------------- #
    # ВНУТРЕННИЕ: конкретные действия
    # --------------------------------------------------------------------- #
    def _do_mqtt_publish(self, action: Action, context: Optional[Dict[str, Any]]) -> None:
        if self._mqtt_publish is None:
            raise RuntimeError("MQTT publish function is not provided")

        if action.mqtt is None:
            raise ValueError("mqtt payload is not set for action")

        topic = action.mqtt.topic
        payload = action.mqtt.payload

        # Специальный случай: payload = {"value": "..."} → это команда управления
        # Шлём ТОЛЬКО само значение, чтобы ModbusLine увидел "1"/"0", а не dict.
        if isinstance(payload, dict) and "value" in payload:
            value = payload["value"]
            # можно при желании логировать context отдельно
            self._mqtt_publish(topic, value)
            return

        # Остальные случаи — как раньше (общий MQTT)
        if isinstance(payload, dict):
            merged = dict(payload)
            if context:
                merged.update({"ctx": context})
            self._mqtt_publish(topic, merged)
        else:
            self._mqtt_publish(topic, payload)

    def _do_trigger_rule(self, action: Action, context: Optional[Dict[str, Any]]) -> None:
        if self._trigger_rule is None:
            raise RuntimeError("Trigger rule function is not provided")
        if action.trigger is None:
            raise ValueError("trigger payload is not set for action")

        target_rule_id = action.trigger.rule_id

        # объединим контексты: что было в action + что пришло сейчас
        trigger_ctx = dict(action.trigger.context)
        if context:
            trigger_ctx.update(context)

        self._trigger_rule(target_rule_id, trigger_ctx)

    def _do_log(self, action: Action, context: Optional[Dict[str, Any]]) -> None:
        """
        LOG-действие — это не то же самое, что запись в журнал выполнения действий.
        Это "оперативный" лог/аудит...
        """
        if action.log is None:
            raise ValueError("log payload is not set for action")

        # --- 1) отправка в alerts_engine (Логи автоматики) ------------------
        try:
            from app.services.alerts_runtime import engine_instance
        except Exception:
            engine_instance = None

        if engine_instance is not None:
            try:
                eng = engine_instance()
            except Exception:
                eng = None

            if eng is not None:
                # вынимаем source_param:
                # приоритет: extra.source_param → context.source_param/source_tag/tag
                src = None
                if isinstance(action.log.extra, dict):
                    src = action.log.extra.get("source_param")
                if not src and context:
                    src = (
                        context.get("source_param")
                        or context.get("source_tag")
                        or context.get("tag")
                    )

                if src:
                    ts_now = datetime.now().astimezone()
                    eng.notify_automation_log(
                        source_param=str(src),
                        level=str(action.log.level or "INFO"),
                        msg=str(action.log.message or ""),
                        ts=ts_now,
                        ctx=context or {},
                    )

        # --- 2) MQTT-лог, как было -----------------------------------------
        if self._mqtt_publish is not None:
            topic = "persay/rules/log"
            payload = {
                "ts": datetime.now().astimezone().isoformat(timespec="milliseconds"),
                "level": action.log.level,
                "msg": action.log.message,
                "extra": action.log.extra,
            }
            if context:
                payload["ctx"] = context
            self._mqtt_publish(topic, payload)
        # если MQTT нет — просто тихо выходим


    # --------------------------------------------------------------------- #
    # ВСПОМОГАТЕЛЬНЫЕ -------------------------------------------------------
    # --------------------------------------------------------------------- #
    @staticmethod
    def _build_payload_preview(action: Action) -> str:
        """
        Короткое представление действия для журнала.
        """
        if action.type == ActionType.MQTT_PUBLISH and action.mqtt:
            return f"MQTT {action.mqtt.topic}"
        if action.type == ActionType.TRIGGER_RULE and action.trigger:
            return f"Trigger rule {action.trigger.rule_id}"
        if action.type == ActionType.LOG and action.log:
            return f"LOG {action.log.level}: {action.log.message}"
        return action.type.value
