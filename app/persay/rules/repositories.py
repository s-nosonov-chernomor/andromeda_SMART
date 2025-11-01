# persay/rules/repositories.py
from __future__ import annotations

from collections import deque
from datetime import datetime
from threading import RLock
from typing import Deque, Dict, List, Optional

from .types import Rule, RuleStatus, ActionLogEntry
from .storage import RuleStorage, ActionLogStorage


# ======================================================================
# 1. IN-MEMORY ХРАНИЛИЩЕ ПРАВИЛ
# ======================================================================

class InMemoryRuleStorage(RuleStorage):
    """
    Простейшее хранилище правил в памяти.
    Подходит для:
      - первых тестов,
      - unit-тестов,
      - запуска на стенде.
    """

    def __init__(self) -> None:
        self._rules: Dict[str, Rule] = {}
        self._lock = RLock()

    def get_rule(self, rule_id: str) -> Optional[Rule]:
        with self._lock:
            return self._rules.get(rule_id)

    def list_rules(self) -> List[Rule]:
        with self._lock:
            return list(self._rules.values())

    def list_enabled_rules(self) -> List[Rule]:
        with self._lock:
            return [
                r for r in self._rules.values()
                if r.status == RuleStatus.ENABLED
            ]

    def save_rule(self, rule: Rule) -> None:
        with self._lock:
            self._rules[rule.id] = rule

    def delete_rule(self, rule_id: str) -> None:
        with self._lock:
            self._rules.pop(rule_id, None)


# ======================================================================
# 2. IN-MEMORY ЖУРНАЛ ДЕЙСТВИЙ
# ======================================================================

class InMemoryActionLogStorage(ActionLogStorage):
    """
    Журнал выполнения действий в памяти.
    Хранит последние N записей (по умолчанию 1000) в deque.
    """

    def __init__(self, max_entries: int = 1000) -> None:
        self._max_entries = max_entries
        self._entries: Deque[ActionLogEntry] = deque(maxlen=max_entries)
        self._lock = RLock()

    def append(self, entry: ActionLogEntry) -> None:
        with self._lock:
            self._entries.appendleft(entry)  # новые — в начало

    def list_recent(
        self,
        limit: int = 100,
        rule_id: Optional[str] = None,
    ) -> List[ActionLogEntry]:
        with self._lock:
            if rule_id is None:
                return list(list(self._entries)[:limit])

            # фильтруем по rule_id
            filtered = [e for e in self._entries if e.rule_id == rule_id]
            return filtered[:limit]
