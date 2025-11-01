# persay/rules/__init__.py
"""
Модуль автоматизации perSAY (Rules / Logic Engine).

Состав:
  - types.py        → модели тегов, условий, действий, правил
  - virtual_tags.py → реестр виртуальных тегов
  - actions.py      → исполнители действий
  - storage.py      → интерфейсы хранилищ
  - repositories.py → in-memory реализации
  - evaluator.py    → проверка условий
  - engine.py       → основной движок
"""
from .engine import RuleEngine
from .actions import ActionExecutor
from .virtual_tags import VirtualTagRegistry
from .evaluator import ConditionEvaluator
from .storage import RulesRepository

__all__ = [
    "RuleEngine",
    "ActionExecutor",
    "VirtualTagRegistry",
    "ConditionEvaluator",
    "RulesRepository",
]
