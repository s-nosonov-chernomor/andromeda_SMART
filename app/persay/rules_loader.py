# app/persay/rules_loader.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import yaml

from app.persay.rules.storage import RulesRepository
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
    RuleOptions,
)


def _parse_condition(d: Dict[str, Any]) -> Condition:
    op_str = str(d.get("op", "=="))
    op = ConditionOperator(op_str)

    return Condition(
        tag=str(d.get("tag", "")),
        op=op,
        operand=d.get("operand"),
        range_min=d.get("range_min"),
        range_max=d.get("range_max"),
        hysteresis=d.get("hysteresis"),
        min_duration_ms=d.get("min_duration_ms"),
        description=d.get("description"),
    )


def _parse_actions(items: List[Dict[str, Any]]) -> List[Action]:
    out: List[Action] = []
    for i, a in enumerate(items or []):
        t_str = str(a.get("type", "")).lower()
        at = ActionType(t_str)

        mqtt_payload = None
        trigger_payload = None
        log_payload = None

        if at == ActionType.MQTT_PUBLISH:
            m = a.get("mqtt") or {}
            mqtt_payload = MqttPublishPayload(
                topic=str(m.get("topic", "")),
                payload=m.get("payload", {}),
                qos=int(m.get("qos", 0)),
                retain=bool(m.get("retain", False)),
            )

        if at == ActionType.TRIGGER_RULE:
            tr = a.get("trigger") or {}
            trigger_payload = TriggerRulePayload(
                rule_id=str(tr.get("rule_id", "")),
                context=tr.get("context", {}) or {},
            )

        if at == ActionType.LOG:
            lg = a.get("log") or {}
            log_payload = LogPayload(
                message=str(lg.get("message", "")),
                level=str(lg.get("level", "INFO")),
                extra=lg.get("extra", {}) or {},
            )

        act = Action(
            id=str(a.get("id") or f"a{i+1}"),
            type=at,
            mqtt=mqtt_payload,
            trigger=trigger_payload,
            log=log_payload,
            delay_ms=a.get("delay_ms"),
            sequential_order=a.get("sequential_order"),
            description=a.get("description"),
        )
        out.append(act)
    return out


def load_rules_from_yaml(path: str, repo: RulesRepository) -> List[Rule]:
    """
    Загружает правила из YAML-файла вида:

    rules:
      - id: "overload_m1"
        name: "Отключить мощ1 при перегрузе"
        status: "enabled"   # или "disabled"
        conditions:
          all_of:
            - tag: "мощ1"
              op: ">"
              operand: 10.0
        actions:
          - id: "a1"
            type: "mqtt_publish"
            mqtt:
              topic: "andromeda/controls/relay1/on"
              payload:
                value: "0"

    Все старые правила в repo перед загрузкой очищаются.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"rules file not found: {path}")

    data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    items = data.get("rules") or []

    # очистим репозиторий
    for r in list(repo.list_rules()):
        repo.delete_rule(r.id)

    loaded: List[Rule] = []

    for idx, rd in enumerate(items):
        rid = str(rd.get("id") or f"r{idx+1}")
        name = str(rd.get("name") or rid)

        status_str = str(rd.get("status", "enabled")).lower()
        status = RuleStatus.ENABLED if status_str != "disabled" else RuleStatus.DISABLED

        # --- conditions ---
        conds = rd.get("conditions") or {}
        all_of = [_parse_condition(c) for c in (conds.get("all_of") or [])]
        any_of = [_parse_condition(c) for c in (conds.get("any_of") or [])]

        cg = ConditionGroup(
            all_of=all_of,
            any_of=any_of,
        )

        # --- actions ---
        actions = _parse_actions(rd.get("actions") or [])

        # --- options (fire_mode, min_interval_sec) ---
        opts_src: Dict[str, Any] = rd.get("options") or {}
        fire_mode = str(opts_src.get("fire_mode", "edge"))
        min_interval_raw = opts_src.get("min_interval_sec", None)
        if min_interval_raw is None:
            min_interval_sec = None
        else:
            try:
                min_interval_sec = float(min_interval_raw)
            except (TypeError, ValueError):
                min_interval_sec = None  # или можно залогировать

        options = RuleOptions(
            fire_mode=fire_mode,
            min_interval_sec=min_interval_sec,
        )

        rule = Rule(
            id=rid,
            name=name,
            status=status,
            conditions=cg,
            actions=actions,
            virtual_tags=rd.get("virtual_tags") or [],
            description=rd.get("description"),
            options=options,   # <── ВАЖНО
        )

        repo.save_rule(rule)
        loaded.append(rule)

    return loaded
