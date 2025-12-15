# persay/rules/engine.py
from __future__ import annotations

from datetime import datetime
from threading import RLock
from typing import Any, Dict, Optional, List, Set  # Set уже у тебя есть

from .types import (
    TagValue,
    Rule,
    ActionLogEntry,
    ActionStatus,
    ActionType,
    ActionResult,
    RuleStats,
)
from .evaluator import ConditionEvaluator
from .actions import ActionExecutor
from .virtual_tags import VirtualTagRegistry, VirtualTag
from .storage import RulesRepository

import logging
log = logging.getLogger("automation")


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
    def _extract_condition_tags(self, rule: Rule) -> Set[str]:
        """
        Собирает множество имён тегов, которые используются в условиях правила.
        Нужно, чтобы не гонять правило на каждое изменение любого тега.
        """
        tags: Set[str] = set()
        cond_group = getattr(rule, "conditions", None)
        if not cond_group:
            return tags

        for item in getattr(cond_group, "all_of", []) or []:
            tag = getattr(item, "tag", None)
            if tag:
                tags.add(tag)

        for item in getattr(cond_group, "any_of", []) or []:
            tag = getattr(item, "tag", None)
            if tag:
                tags.add(tag)

        return tags

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

        # ЛОГ: что за тег пришёл
        try:
            log.debug(
                "tag_update: %s = %r (source=%s)",
                tag_value.name,
                tag_value.value,
                getattr(tag_value.source, "value", tag_value.source)
            )
        except Exception:
            pass

        # Для отладки: покажем, что знаем по основным реле
        for tname in ("реле1", "реле2", "реле3", "реле4", "реле5", "реле6"):
            tv = eval_tag_map.get(tname)
            if tv is None:
                log.debug("eval_map[%s]: <no value>", tname)
            else:
                log.debug(
                    "eval_map[%s]: value=%r ts=%s",
                    tname,
                    tv.value,
                    tv.ts.isoformat() if isinstance(tv.ts, datetime) else tv.ts,
                )

        # вне lock — прогоняем правила
        for rule in rules:
            # 0) смотрим, от каких тегов зависит правило
            cond_tags = self._extract_condition_tags(rule)

            # если правило вообще не зависит ни от каких тегов – оно не должно
            # срабатывать на tag_update (только через trigger_rule)
            if not cond_tags:
                log.debug(
                    "rule %s (%s): skip on tag_update (no cond_tags)",
                    rule.id,
                    rule.name,
                )
                continue

            # если правило зависит от конкретных тегов, а изменился другой – пропускаем
            if tag_value.name not in cond_tags:
                log.debug(
                    "rule %s (%s): skip for tag %s (cond_tags=%s)",
                    rule.id,
                    rule.name,
                    tag_value.name,
                    cond_tags,
                )
                continue

            # если правило объявило виртуальные теги — убедимся, что они есть
            self._ensure_rule_virtual_tags(rule)

            # проверяем условия
            ok = self._evaluator.evaluate_group(rule.conditions, eval_tag_map)

            log.debug(
                "rule %s (%s): %s",
                rule.id,
                rule.name,
                "OK" if ok else "NO"
            )

            # --- ИНИЦИАЛИЗАЦИЯ stats, если нужно ---
            if getattr(rule, "stats", None) is None:
                rule.stats = RuleStats()

            # --- Читаем опции ---
            options = getattr(rule, "options", None)
            fire_mode = getattr(options, "fire_mode", "edge") if options is not None else "edge"
            min_interval_sec = getattr(options, "min_interval_sec", None) if options is not None else None

            prev_state = getattr(rule.stats, "last_condition_state", None)

            # --- РЕЖИМ fire_mode=edge: стрелять только по фронту ---
            if fire_mode == "edge":
                if not ok:
                    # условие не выполнено → просто фиксируем состояние и идём дальше
                    rule.stats.last_condition_state = False
                    continue

                # ok == True
                if prev_state is True:
                    # условие уже было True ранее → не считаем это новым фронтом
                    rule.stats.last_condition_state = True
                    log.debug(
                        "rule %s (%s): condition still True, suppressed in edge mode",
                        rule.id,
                        rule.name,
                    )
                    continue

                # сюда попадаем, когда prev_state is not True (None/False) и ok=True
                # это как раз фронт
            else:
                # fire_mode != edge (например, "level"):
                # если условие False — просто обновим состояние и не стрелять
                if not ok:
                    rule.stats.last_condition_state = False
                    continue
                # ok=True — дальше пойдём к проверке cooldown

            # --- COOLDOWN по min_interval_sec ---
            if ok and min_interval_sec and min_interval_sec > 0:
                now = datetime.utcnow()
                last = rule.stats.last_fired_at
                if last is not None:
                    dt = (now - last).total_seconds()
                    if dt < float(min_interval_sec):
                        # слишком рано, ещё не вышел интервал
                        rule.stats.last_condition_state = True
                        log.debug(
                            "rule %s (%s): suppressed by cooldown (%.3fs elapsed, need %.3fs)",
                            rule.id,
                            rule.name,
                            dt,
                            float(min_interval_sec),
                        )
                        continue

            # если мы сюда дошли — правило реально будет стрелять
            rule.stats.last_condition_state = ok

            # контекст
            rule_ctx = {"source_tag": tag_value.name}
            if context:
                rule_ctx.update(context)

            log.info(
                "rule %s (%s) FIRED by tag %s",
                rule.id,
                rule.name,
                tag_value.name,
            )

            results = self._actions.execute_actions(
                rule,
                rule.actions,
                sequential=True,
                context=rule_ctx,
            )
            self._update_rule_stats(rule, results)


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

        results = self._actions.execute_actions(
            rule,
            rule.actions,
            sequential=True,
            context=context or {},
        )
        self._update_rule_stats(rule, results)

    def get_tag_value(self, name: str) -> Optional[TagValue]:
        """Можно будет запросить актуальное значение тега у движка."""
        with self._lock:
            return self._tag_values.get(name)

    def _update_rule_stats(self, rule: Rule, results: List[ActionResult]) -> None:
        """
        Обновить счётчик срабатываний и время/ошибку по результатам выполнения действий.
        """
        # на всякий случай: вдруг stats не инициализирован
        if getattr(rule, "stats", None) is None:
            rule.stats = RuleStats()

        rule.stats.fire_count += 1
        rule.stats.last_fired_at = datetime.utcnow()

        # если какие-то действия упали — возьмём первую ошибку
        errs = [r.error for r in results if r.error]
        if errs:
            rule.stats.last_error = errs[0]
        else:
            rule.stats.last_error = None

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
