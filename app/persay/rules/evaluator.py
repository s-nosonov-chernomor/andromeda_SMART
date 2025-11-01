# persay/rules/evaluator.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from .types import (
    Condition,
    ConditionGroup,
    ConditionOperator,
    ConditionOnBad,
    TagValue,
    TagQuality,
)


class ConditionEvaluator:
    """
    Проверяет выполнение условий правила.
    Получает:
      - карту текущих тегов (dict[name -> TagValue])
      - само условие/группу
    Возвращает: bool (выполнено/нет)
    """

    def __init__(self) -> None:
        # при желании можно добавить cache, но пока не нужно
        pass

    # ------------------------------------------------------------------
    def evaluate_group(
        self,
        group: ConditionGroup,
        tag_values: Dict[str, TagValue],
    ) -> bool:
        """
        Проверяет целую группу условий.
        """
        # если и all_of, и any_of пусты → считаем "всегда истина"
        if not group.all_of and not group.any_of:
            return True

        # если все должны быть выполнены
        if group.all_of:
            for cond in group.all_of:
                if not self.evaluate(cond, tag_values):
                    return False
            return True

        # если достаточно одного
        if group.any_of:
            for cond in group.any_of:
                if self.evaluate(cond, tag_values):
                    return True
            return False

        return True

    # ------------------------------------------------------------------
    def evaluate(self, cond: Condition, tag_values: Dict[str, TagValue]) -> bool:
        """
        Проверка одного условия.
        """
        tag = tag_values.get(cond.tag)
        if tag is None:
            # Тег не найден — по умолчанию false
            return False

        # обработка качества
        if tag.quality != TagQuality.GOOD:
            if cond.on_bad == ConditionOnBad.FAIL:
                return False
            elif cond.on_bad == ConditionOnBad.SKIP:
                return True
            elif cond.on_bad == ConditionOnBad.USE_LAST_OK:
                # предположим, что значение корректное, просто старое
                pass

        val = tag.value

        try:
            if cond.op == ConditionOperator.EQ:
                return val == cond.operand
            elif cond.op == ConditionOperator.NE:
                return val != cond.operand
            elif cond.op == ConditionOperator.GT:
                return val > cond.operand
            elif cond.op == ConditionOperator.GE:
                return val >= cond.operand
            elif cond.op == ConditionOperator.LT:
                return val < cond.operand
            elif cond.op == ConditionOperator.LE:
                return val <= cond.operand
            elif cond.op == ConditionOperator.IN_RANGE:
                return cond.range_min <= val <= cond.range_max
            elif cond.op == ConditionOperator.NOT_IN_RANGE:
                return not (cond.range_min <= val <= cond.range_max)
            elif cond.op == ConditionOperator.IS_TRUE:
                return bool(val) is True
            elif cond.op == ConditionOperator.IS_FALSE:
                return bool(val) is False
            elif cond.op == ConditionOperator.QUALITY_IS:
                return tag.quality.name.lower() == str(cond.operand).lower()
            elif cond.op == ConditionOperator.STALE:
                # значение устарело по времени
                max_age = cond.min_duration_ms or 0
                delta = (datetime.utcnow() - tag.ts).total_seconds() * 1000
                return delta > max_age
            elif cond.op == ConditionOperator.CUSTOM_EXPR:
                # если позже добавим выражения
                return self._evaluate_custom_expr(cond, tag_values)
            else:
                return False
        except Exception:
            # если сравнение не удалось — условие ложь
            return False

    # ------------------------------------------------------------------
    def _evaluate_custom_expr(
        self,
        cond: Condition,
        tag_values: Dict[str, TagValue],
    ) -> bool:
        """
        Простейшая безопасная проверка выражений (ограниченная).
        Пока можно использовать только имена тегов и стандартные операторы.
        """
        expr = str(cond.operand)
        # собираем контекст значений
        ctx: Dict[str, Any] = {k: v.value for k, v in tag_values.items()}

        # создаём безопасное пространство
        safe_locals = {
            "__builtins__": {},
            "min": min,
            "max": max,
            "abs": abs,
        }
        try:
            return bool(eval(expr, safe_locals, ctx))
        except Exception:
            return False
