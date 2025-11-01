# persay/rules/engine.py
from __future__ import annotations

from datetime import datetime
from threading import RLock
from typing import Any, Dict, Optional, List

from .types import (
    TagValue,
    Rule,
    ActionLogEntry,
    ActionStatus,
    ActionType,
)
from .evaluator import ConditionEvaluator
from .actions import ActionExecutor
from .virtual_tags import VirtualTagRegistry, VirtualTag
from .storage import RulesRepository


class RuleEngine:
    """
    Основной движок правил:
      - принимает входные события (обновился тег)
      - подмешивает виртуальные теги
      - проверяет условия всех ВКЛЮЧЁННЫХ правил
      - выполняет их действия
      - пишет журнал (через ActionExecutor + репозиторий)

    ВАЖНО:
    - никакого приоритета/арбитража здесь нет (по твоему требованию)
    - все подходящие правила будут выполнены
    """

    def __init__(
        self,
        *,
        rules_repo: RulesRepository,
        actions: ActionExecutor,
        virtual_tags: VirtualTagRegistry,
        evaluator: Optional[ConditionEvaluator] = None,
    ) -> None:
        self._rules_repo = rules_repo
        self._actions = actions
        self._vtags = virtual_tags
        self._evaluator = evaluator or ConditionEvaluator()

        # текущие значения «реальных» тегов (то, что пришло от опроса / MQTT)
        self._tag_values: Dict[str, TagValue] = {}

        # блокировка на обновление/чтение тегов и прогон правил
        self._lock = RLock()

        # --- важный трюк ---
        # если ActionExecutor не получил trigger_rule при создании,
        # можно «дозалить» его тут (но это полу-приватно).
        # правильный способ — передать engine.trigger_rule сразу при создании.
        if getattr(self._actions, "_trigger_rule", None) is None:
            # аккуратно: мы знаем внутреннее имя, т.к. сами писали actions.py
            self._actions._trigger_rule = self.trigger_rule  # type: ignore[attr-defined]

    # ------------------------------------------------------------------ #
    # ПУБЛИЧНЫЙ API
    # ------------------------------------------------------------------ #
    def handle_tag_update(
        self,
        tag_value: TagValue,
        *,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Это основной вход: «у нас обновился тег».
        Например, это можно вызывать из MQTT-хэндлера или из модуля опроса.
        """
        with self._lock:
            # сохраняем новое значение
            self._tag_values[tag_value.name] = tag_value

            # берём список включенных правил
            rules = self._rules_repo.list_enabled_rules()

            # построим карту значений тегов для проверок
            eval_tag_map = self._build_eval_tag_map()

        # вне lock — прогоняем правила
        for rule in rules:
            # если правило объявило виртуальные теги — убедимся, что они есть
            self._ensure_rule_virtual_tags(rule)

            # проверяем условия
            ok = self._evaluator.evaluate_group(rule.conditions, eval_tag_map)
            if not ok:
                continue

            # условия выполнены → запускаем ветку действий
            # контекст дополним источником
            rule_ctx = {"source_tag": tag_value.name}
            if context:
                rule_ctx.update(context)

            self._actions.execute_actions(
                rule,
                rule.actions,
                sequential=True,
                context=rule_ctx,
            )

    def trigger_rule(
        self,
        rule_id: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Запустить правило по запросу ИЗ другого правила (ActionType.TRIGGER_RULE)
        или из внешнего кода.

        ВНИМАНИЕ: здесь мы НЕ проверяем условия —
        это именно 'force' запуск.
        """
        rule = self._rules_repo.get_rule(rule_id)
        if rule is None:
            # если правила нет — хорошо бы это в журнал положить
            self._append_missing_rule_log(rule_id, context)
            return

        # создадим объявленные виртуальные теги
        self._ensure_rule_virtual_tags(rule)

        self._actions.execute_actions(
            rule,
            rule.actions,
            sequential=True,
            context=context or {},
        )

    def get_tag_value(self, name: str) -> Optional[TagValue]:
        """Можно будет запросить актуальное значение тега у движка."""
        with self._lock:
            return self._tag_values.get(name)

    # ------------------------------------------------------------------ #
    # ВНУТРЕННЕЕ
    # ------------------------------------------------------------------ #
    def _build_eval_tag_map(self) -> Dict[str, TagValue]:
        """
        Собирает ОБЩУЮ карту тегов для проверки условий:
          - реальные теги (что пришло снаружи)
          - виртуальные теги (из реестра)
        Если имя совпадает — реальный тег перекрывает виртуальный.
        """
        with self._lock:
            result: Dict[str, TagValue] = dict(self._tag_values)

        # добавим виртуальные теги
        vtags_dict = self._vtags.all()
        for name, vtag in vtags_dict.items():
            if name in result:
                # реальный тег важнее
                continue

            # превращаем VirtualTag в TagValue — у нас у условий интерфейс к TagValue
            tv = TagValue(
                name=name,
                value=vtag.value,
                ts=vtag.ts,
                quality=vtag.quality,
                source=vtag.source,
                meta=dict(vtag.meta),
            )
            result[name] = tv

        return result

    def _ensure_rule_virtual_tags(self, rule: Rule) -> None:
        """
        Если в правиле перечислены виртуальные теги — создадим их в реестре,
        чтобы дальше их можно было использовать в логике или публиковать.
        """
        if not rule.virtual_tags:
            return
        for tag_name in rule.virtual_tags:
            if not self._vtags.exists(tag_name):
                self._vtags.create(tag_name)

    def _append_missing_rule_log(
        self,
        rule_id: str,
        context: Optional[Dict[str, Any]],
    ) -> None:
        """
        Если какой-то action попросил запустить несуществующее правило —
        сохраним запись в журнал, чтобы было видно.
        """
        entry = ActionLogEntry(
            ts=datetime.utcnow(),
            rule_id=rule_id,
            rule_name=f"<missing:{rule_id}>",
            action_id="trigger",
            action_type=ActionType.TRIGGER_RULE,
            status=ActionStatus.FAILED,
            error="Rule not found",
            payload_preview=f"Trigger missing rule {rule_id}",
            context=context or {},
        )
        self._rules_repo.append_action_log(entry)
