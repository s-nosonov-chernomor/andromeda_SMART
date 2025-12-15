# app/persay/__init__.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from app.persay.rules.engine import RuleEngine
from app.persay.rules.storage import RulesRepository
from app.persay.rules.repositories import (
    InMemoryRuleStorage,
    InMemoryActionLogStorage,
)
from app.persay.rules.actions import ActionExecutor
from app.persay.rules.virtual_tags import VirtualTagRegistry
from app.persay.rules.types import (
    Rule,
    RuleStatus,
    ConditionGroup,
    Condition,
    ConditionOperator,
    Action,
    ActionType,
    MqttPublishPayload,
    TriggerRulePayload,
    LogPayload,
)

from datetime import datetime
from typing import Any, Dict, Optional
from app.persay.rules.types import TagValue, TagSource  # ← у нас уже есть types.py


# сюда мы потом положим живую ссылку
_automation_ctx: "AutomationContext | None" = None


class AutomationContext:
    """
    Держим всё в одном месте:
    - движок правил
    - хранилище правил + журнал
    - реестр виртуальных тегов
    """
    def __init__(self, *, mqtt_bridge, rules: List[Rule] | None = None) -> None:
        # 1) хранилища
        rule_storage = InMemoryRuleStorage()
        action_log_storage = InMemoryActionLogStorage(max_entries=2000)
        repo = RulesRepository(rule_storage, action_log_storage)

        # 2) виртуальные теги — сразу связаны с MQTT
        vtags = VirtualTagRegistry(
            publish_func=lambda name, value, meta: mqtt_bridge.publish(
                f"persay/virt/{name}",
                value,
                code=0,
                status_details=meta,
                context={"virt": True, "name": name},
            )
        )

        # 3) исполнители действий
        actions = ActionExecutor(
            mqtt_publish=lambda topic, payload: mqtt_bridge.publish(
                topic,
                payload,
                code=0,
                status_details={"message": "rule"},
                context={"rule": True},
            ),
            # trigger_rule подставится в engine
            write_action_log=repo.append_action_log,
        )

        # 4) движок
        engine = RuleEngine(
            rules_repo=repo,
            actions=actions,
            virtual_tags=vtags,
        )

        # если нам уже принесли правила — загрузим
        if rules:
            for r in rules:
                repo.save_rule(r)

        self.mqtt = mqtt_bridge
        self.repo = repo
        self.engine = engine
        self.vtags = vtags


def init_automation(
    mqtt_bridge,
    rules_path: str = "data/rules.yaml",
) -> AutomationContext:
    """
    Вызываем ОДИН раз при старте приложения.
    Передаём уже созданный MqttBridge и путь к rules.yaml.
    """
    global _automation_ctx

    rules = load_rules_from_file(rules_path)
    ctx = AutomationContext(mqtt_bridge=mqtt_bridge, rules=rules)
    _automation_ctx = ctx
    return ctx


def get_automation() -> AutomationContext:
    global _automation_ctx
    if _automation_ctx is None:
        raise RuntimeError("AutomationContext is not initialized")
    return _automation_ctx


# -----------------------------------------------------------------------------
# Загрузка правил из отдельного YAML
# -----------------------------------------------------------------------------
def load_rules_from_file(path: str) -> List[Rule]:
    fp = Path(path)
    if not fp.exists():
        return []
    raw = fp.read_text("utf-8")
    data = yaml.safe_load(raw) or {}
    return _rules_from_dict(data)


def _rules_from_dict(data: Dict[str, Any]) -> List[Rule]:
    raw_rules = data.get("rules") or []
    out: List[Rule] = []

    for r in raw_rules:
        # --- conditions ---
        cg = ConditionGroup()
        conds = r.get("conditions") or {}
        for c in conds.get("all_of", []):
            cg.all_of.append(
                Condition(
                    tag=c["tag"],
                    op=ConditionOperator(c["op"]),
                    operand=c.get("operand"),
                    range_min=c.get("range_min"),
                    range_max=c.get("range_max"),
                    min_duration_ms=c.get("min_duration_ms"),
                    description=c.get("description"),
                )
            )
        for c in conds.get("any_of", []):
            cg.any_of.append(
                Condition(
                    tag=c["tag"],
                    op=ConditionOperator(c["op"]),
                    operand=c.get("operand"),
                )
            )

        # --- actions ---
        acts: List[Action] = []
        for a in r.get("actions", []):
            at = ActionType(a["type"])
            action = Action(
                id=a.get("id", a["type"]),
                type=at,
                delay_ms=a.get("delay_ms"),
                description=a.get("description"),
            )
            if at == ActionType.MQTT_PUBLISH:
                mp = a["mqtt"]
                action.mqtt = MqttPublishPayload(
                    topic=mp["topic"],
                    payload=mp.get("payload", {}),
                    qos=mp.get("qos", 0),
                    retain=mp.get("retain", False),
                )
            elif at == ActionType.TRIGGER_RULE:
                tr = a["trigger"]
                action.trigger = TriggerRulePayload(
                    rule_id=tr["rule_id"],
                    context=tr.get("context", {}),
                )
            elif at == ActionType.LOG:
                lp = a["log"]
                action.log = LogPayload(
                    message=lp["message"],
                    level=lp.get("level", "INFO"),
                    extra=lp.get("extra", {}),
                )
            acts.append(action)

        rule = Rule(
            id=r["id"],
            name=r.get("name", r["id"]),
            status=RuleStatus(r.get("status", "enabled")),
            conditions=cg,
            actions=acts,
            virtual_tags=r.get("virtual_tags", []),
            description=r.get("description"),
        )
        out.append(rule)

    return out

def _make_tag_name_from_ctx(ctx: Dict[str, Any]) -> str:
    """
    Придумываем единый нейминг тега из того, что у нас есть в контексте.
    Приоритет:
      1) object + param → object.param
      2) line + unit_id + param → line.<unit>.param
      3) просто param
    """
    obj = (ctx.get("object") or "").strip()
    param = (ctx.get("param") or "").strip()
    line = (ctx.get("line") or "").strip()
    unit_id = ctx.get("unit_id")

    if obj and param:
        return f"{obj}.{param}"
    if line and unit_id is not None and param:
        return f"{line}.{unit_id}.{param}"
    if param:
        return param
    # крайний случай — чтобы движок не упал
    return "unknown"


def handle_publish_from_mqtt(ctx: Dict[str, Any], payload: Dict[str, Any]) -> None:
    """
    Вызываем это из mqtt_bridge, когда у нас появилось новое опубликованное значение.
    Преобразуем в TagValue и отдаём в RuleEngine.
    """
    auto = get_automation()
    if auto is None:
        return

    name = _make_tag_name_from_ctx(ctx)

    # значение из payload
    value = payload.get("value", None)

    # время
    ts_iso = (payload.get("metadata") or {}).get("timestamp")
    if ts_iso:
        try:
            ts = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
        except Exception:
            ts = datetime.utcnow()
    else:
        ts = datetime.utcnow()

    tv = TagValue(
        name=name,
        value=value,
        ts=ts,
        source=TagSource.MQTT,
        meta=dict(ctx),
    )

    # контекст тоже передадим (там line, unit_id, trigger и т.п.)
    auto.engine.handle_tag_update(tv, context=ctx)