# persay/rules/storage.py
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, Iterable, List, Optional
from datetime import datetime

from .types import Rule, RuleStatus, ActionLogEntry


# ======================================================================
# 1. ХРАНИЛИЩЕ ПРАВИЛ
# ======================================================================

class RuleStorage(ABC):
    """
    Абстрактное хранилище правил.
    Реализации могут быть:
      - in-memory (для тестов)
      - файл YAML/JSON
      - БД (PostgreSQL, SQLite)
      - внешнее API
    """

    @abstractmethod
    def get_rule(self, rule_id: str) -> Optional[Rule]:
        """Вернёт правило по id или None."""
        raise NotImplementedError

    @abstractmethod
    def list_rules(self) -> List[Rule]:
        """Вернёт все правила (включая отключённые)."""
        raise NotImplementedError

    @abstractmethod
    def list_enabled_rules(self) -> List[Rule]:
        """Вернёт только включённые правила."""
        raise NotImplementedError

    @abstractmethod
    def save_rule(self, rule: Rule) -> None:
        """Создать или обновить правило."""
        raise NotImplementedError

    @abstractmethod
    def delete_rule(self, rule_id: str) -> None:
        """Удалить правило по id (если нет — молча)."""
        raise NotImplementedError


# ======================================================================
# 2. ХРАНИЛИЩЕ ЖУРНАЛА ДЕЙСТВИЙ
# ======================================================================

class ActionLogStorage(ABC):
    """
    Абстрактное хранилище журнала выполнения действий.
    Движок будет просто складывать туда записи, а UI/внешний код — читать.
    """

    @abstractmethod
    def append(self, entry: ActionLogEntry) -> None:
        """Добавить запись в журнал."""
        raise NotImplementedError

    @abstractmethod
    def list_recent(
        self,
        limit: int = 100,
        rule_id: Optional[str] = None,
    ) -> List[ActionLogEntry]:
        """
        Вернуть последние записи.
        Можно отфильтровать по rule_id.
        """
        raise NotImplementedError


# ======================================================================
# 3. КОМПОЗИТ ДЛЯ ДВИЖКА
# ======================================================================

class RulesRepository:
    """
    Удобная обёртка, чтобы движок получил
    и правила, и журнал в одном объекте.
    """

    def __init__(
        self,
        rules: RuleStorage,
        action_log: ActionLogStorage,
    ) -> None:
        self._rules = rules
        self._action_log = action_log

    # --- правила -------------------------------------------------------

    def get_rule(self, rule_id: str) -> Optional[Rule]:
        return self._rules.get_rule(rule_id)

    def list_rules(self) -> List[Rule]:
        return self._rules.list_rules()

    def list_enabled_rules(self) -> List[Rule]:
        return self._rules.list_enabled_rules()

    def save_rule(self, rule: Rule) -> None:
        self._rules.save_rule(rule)

    def delete_rule(self, rule_id: str) -> None:
        self._rules.delete_rule(rule_id)

    # --- журнал --------------------------------------------------------

    def append_action_log(self, entry: ActionLogEntry) -> None:
        self._action_log.append(entry)

    def list_recent_action_logs(
        self,
        limit: int = 100,
        rule_id: Optional[str] = None,
    ) -> List[ActionLogEntry]:
        return self._action_log.list_recent(limit=limit, rule_id=rule_id)
