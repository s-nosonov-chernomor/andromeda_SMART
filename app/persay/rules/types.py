# persay/rules/types.py
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Union
from datetime import datetime


# === 1. БАЗОВЫЕ ENUM'Ы =======================================================

class TagQuality(Enum):
    """Качество значения тега."""
    GOOD = "good"
    BAD = "bad"
    UNCERTAIN = "uncertain"
    STALE = "stale"   # устаревшее по времени


class TagSource(Enum):
    """Откуда взялось значение тега."""
    DEVICE = "device"          # с опрашиваемого устройства/линии
    MQTT = "mqtt"              # из mqtt-сообщения
    CALCULATED = "calculated"  # вычислено движком
    VIRTUAL = "virtual"        # заведён в реестре виртуальных тегов
    OVERRIDE = "override"      # руками/через UI
    SYSTEM = "system"          # служебное


class ConditionOperator(Enum):
    """Операторы условий."""
    EQ = "=="        # равно
    NE = "!="        # не равно
    GT = ">"         # больше
    GE = ">="        # больше или равно
    LT = "<"         # меньше
    LE = "<="        # меньше или равно
    IN_RANGE = "in_range"          # в диапазоне
    NOT_IN_RANGE = "not_in_range"  # вне диапазона
    IS_TRUE = "is_true"
    IS_FALSE = "is_false"
    CHANGED = "changed"            # значение изменилось
    EDGE_RISING = "edge_rising"    # фронт
    EDGE_FALLING = "edge_falling"  # спад
    QUALITY_IS = "quality_is"      # качество = GOOD/BAD/...
    STALE = "stale"                # устарело
    CUSTOM_EXPR = "custom_expr"    # сложное выражение (позже, если нужно)


class ConditionOnBad(Enum):
    """Что делать, если значение тега плохого качества."""
    FAIL = "fail"          # считать условие невыполненным
    SKIP = "skip"          # не учитывать это условие (будто его нет)
    USE_LAST_OK = "use_last_ok"  # использовать последнее хорошее значение


class ActionType(Enum):
    """Что умеет делать движок (по твоим требованиям)."""
    MQTT_PUBLISH = "mqtt_publish"      # опубликовать событие / команду
    TRIGGER_RULE = "trigger_rule"      # запустить другое правило/сценарий
    LOG = "log"                        # написать в журнал/аудит
    # (!) остальные специально НЕ включаем — ты сказал не надо


class RuleStatus(Enum):
    """Текущий статус правила."""
    ENABLED = "enabled"
    DISABLED = "disabled"


class ActionStatus(Enum):
    """Результат исполнения конкретного действия."""
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    PENDING = "pending"  # для действий с подтверждением (если потом добавим)


class VirtualTagKind(Enum):
    """Тип виртуального тега."""
    INTERNAL = "internal"     # живёт только внутри движка, не публикуем
    PUBLISHABLE = "publishable"  # можно (и иногда надо) публиковать в MQTT
    MIRROR = "mirror"         # зеркало другого тега (для alias'ов)


# === 2. ОПИСАНИЕ ТЕГА ========================================================

@dataclass
class TagValue:
    """Текущее значение тега, которое подаётся на вход движка."""
    name: str
    value: Any
    ts: datetime
    quality: TagQuality = TagQuality.GOOD
    source: TagSource = TagSource.DEVICE
    meta: Dict[str, Any] = field(default_factory=dict)


# === 3. УСЛОВИЯ ==============================================================

@dataclass
class Condition:
    """
    Универсальное условие.
    Примеры:
      - tag=greenhouse.temp, op="<", operand=25
      - tag=pump.enabled, op="is_true"
      - tag=light.level, op="in_range", range_min=50, range_max=80
    """
    tag: str
    op: ConditionOperator
    operand: Optional[Any] = None
    range_min: Optional[float] = None
    range_max: Optional[float] = None
    hysteresis: Optional[float] = None     # гистерезис на выходе из условия
    min_duration_ms: Optional[int] = None  # условие счит. выполненным после N мс
    on_bad: ConditionOnBad = ConditionOnBad.FAIL
    description: Optional[str] = None


@dataclass
class ConditionGroup:
    """
    Группа условий: либо все должны быть выполнены (AND),
    либо достаточно одного (OR).
    """
    all_of: List[Condition] = field(default_factory=list)
    any_of: List[Condition] = field(default_factory=list)
    # можно будет расширить вложенными группами


# === 4. ДЕЙСТВИЯ =============================================================

@dataclass
class MqttPublishPayload:
    topic: str
    payload: Union[str, Dict[str, Any]]
    qos: int = 0
    retain: bool = False


@dataclass
class TriggerRulePayload:
    rule_id: str
    context: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LogPayload:
    message: str
    level: str = "INFO"
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Action:
    """Описание одного действия."""
    id: str
    type: ActionType
    mqtt: Optional[MqttPublishPayload] = None
    trigger: Optional[TriggerRulePayload] = None
    log: Optional[LogPayload] = None
    delay_ms: Optional[int] = None        # отложить выполнение
    sequential_order: Optional[int] = None  # если ветка последоват.
    description: Optional[str] = None


@dataclass
class ActionResult:
    """Результат исполнения действия — пойдёт в журнал."""
    action_id: str
    type: ActionType
    status: ActionStatus
    ts: datetime
    error: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)


# === 5. ПРАВИЛО =============================================================
@dataclass
class RuleStats:
    fire_count: int = 0
    last_fired_at: Optional[datetime] = None
    last_error: Optional[str] = None
    # новое поле: в каком состоянии было условие в прошлый раз
    last_condition_state: Optional[bool] = None

@dataclass
class RuleOptions:
    """
    Доп. опции поведения правила.

    fire_mode:
      - "edge"  — стрелять только по фронту (False/None -> True)
      - "level" — стрелять каждый раз, пока условие True

    min_interval_sec:
      - минимальный интервал между срабатываниями (cooldown).
        None или 0 → без ограничения.
    """
    fire_mode: str = "edge"
    min_interval_sec: Optional[float] = None

@dataclass
class Rule:
    """Правило автоматизации."""
    id: str
    name: str
    status: RuleStatus = RuleStatus.ENABLED
    conditions: ConditionGroup = field(default_factory=ConditionGroup)
    actions: List[Action] = field(default_factory=list)
    # Список виртуальных тегов, которые это правило может создавать/обновлять
    # (например, для публикации состояния этого правила)
    virtual_tags: List[str] = field(default_factory=list)
    description: Optional[str] = None
    stats: RuleStats = field(default_factory=RuleStats)
    # Новые опции работы правила (режим срабатывания, интервалы и т.п.)
    options: RuleOptions = field(default_factory=RuleOptions)



# === 6. ЖУРНАЛ ДЕЙСТВИЙ ======================================================

@dataclass
class ActionLogEntry:
    """
    Запись в журнал выполнения действий.
    Это то, что ты хотел: кто, когда, что сделал и чем закончилось.
    """
    ts: datetime
    rule_id: str
    rule_name: str
    action_id: str
    action_type: ActionType
    status: ActionStatus
    error: Optional[str] = None
    payload_preview: Optional[str] = None  # короткий текст, чтобы в UI показывать
    context: Dict[str, Any] = field(default_factory=dict)
