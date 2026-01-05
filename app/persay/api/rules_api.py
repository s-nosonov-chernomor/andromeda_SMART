# app/persay/api/rules_api.py
from __future__ import annotations

from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException

from app.persay.runtime import rules_repo
from app.persay.rules_loader import load_rules_from_yaml
from app.persay.rules.types import RuleStatus

router = APIRouter(prefix="/api/automation", tags=["automation"])

def _conditions_to_legacy_str(rule) -> str:
    """
    Собрать текстовое условие вроде:
      "мощ1 > 10 AND (темп1 < 25 OR темп2 < 30)"
    из rule.conditions.*
    """
    parts: list[str] = []

    cond_group = getattr(rule, "conditions", None)
    if not cond_group:
        return ""

    # all_of → AND
    if getattr(cond_group, "all_of", None):
        all_chunks = []
        for c in cond_group.all_of:
            if c.range_min is not None or c.range_max is not None:
                rng = []
                if c.range_min is not None:
                    rng.append(f">= {c.range_min}")
                if c.range_max is not None:
                    rng.append(f"<= {c.range_max}")
                rhs = " and ".join(rng)
                all_chunks.append(f"{c.tag} {rhs}")
            else:
                # простое сравнение
                if c.operand is not None:
                    all_chunks.append(f"{c.tag} {c.op.value} {c.operand}")
                else:
                    all_chunks.append(f"{c.tag} {c.op.value}")
        if all_chunks:
            parts.append(" AND ".join(all_chunks))

    # any_of → OR, в скобках
    if getattr(cond_group, "any_of", None):
        any_chunks = []
        for c in cond_group.any_of:
            if c.range_min is not None or c.range_max is not None:
                rng = []
                if c.range_min is not None:
                    rng.append(f">= {c.range_min}")
                if c.range_max is not None:
                    rng.append(f"<= {c.range_max}")
                rhs = " and ".join(rng)
                any_chunks.append(f"{c.tag} {rhs}")
            else:
                if c.operand is not None:
                    any_chunks.append(f"{c.tag} {c.op.value} {c.operand}")
                else:
                    any_chunks.append(f"{c.tag} {c.op.value}")
        if any_chunks:
            parts.append("(" + " OR ".join(any_chunks) + ")")

    return " AND ".join(parts)

def _rule_to_dict(rule) -> Dict[str, Any]:
    stats = getattr(rule, "stats", None)
    options = getattr(rule, "options", None)

    return {
        "id": rule.id,
        "name": rule.name,
        "status": rule.status.value,
        # НОВОЕ: для фронта — удобно иметь enabled как bool
        "enabled": (rule.status == RuleStatus.ENABLED),
        "description": rule.description,
        "virtual_tags": list(rule.virtual_tags or []),

        # НОВОЕ: строковое условие для редактора
        "condition": _conditions_to_legacy_str(rule),

        "conditions": {
            "all_of": [
                {
                    "tag": c.tag,
                    "op": c.op.value,
                    "operand": c.operand,
                    "range_min": c.range_min,
                    "range_max": c.range_max,
                    "hysteresis": c.hysteresis,
                    "min_duration_ms": c.min_duration_ms,
                    "on_bad": c.on_bad.value,
                    "description": c.description,
                }
                for c in (rule.conditions.all_of or [])
            ],
            "any_of": [
                {
                    "tag": c.tag,
                    "op": c.op.value,
                    "operand": c.operand,
                    "range_min": c.range_min,
                    "range_max": c.range_max,
                    "hysteresis": c.hysteresis,
                    "min_duration_ms": c.min_duration_ms,
                    "on_bad": c.on_bad.value,
                    "description": c.description,
                }
                for c in (rule.conditions.any_of or [])
            ],
        },

        "actions": [
            {
                "id": a.id,
                "type": a.type.value,
                "delay_ms": a.delay_ms,
                "sequential_order": a.sequential_order,
                "description": a.description,
                "mqtt": {
                    "topic": a.mqtt.topic,
                    "payload": a.mqtt.payload,
                    "qos": a.mqtt.qos,
                    "retain": a.mqtt.retain,
                } if a.mqtt else None,
                "trigger": {
                    "rule_id": a.trigger.rule_id,
                    "context": a.trigger.context,
                } if a.trigger else None,
                "log": {
                    "message": a.log.message,
                    "level": a.log.level,
                    "extra": a.log.extra,
                } if a.log else None,
            }
            for a in (rule.actions or [])
        ],

        "fire_count": getattr(stats, "fire_count", 0),
        "last_fired": getattr(stats, "last_fired_at", None),
        "last_error": getattr(stats, "last_error", None),
        "stats": {
            "fire_count": getattr(stats, "fire_count", 0),
            "last_fired": getattr(stats, "last_fired_at", None),
            "last_error": getattr(stats, "last_error", None),
            "last_condition_state": getattr(stats, "last_condition_state", None),
        },

        "options": {
            "fire_mode": getattr(options, "fire_mode", "edge"),
            "min_interval_sec": getattr(options, "min_interval_sec", None),
        },
    }


@router.get("/rules")
def list_rules() -> List[Dict[str, Any]]:
    repo = rules_repo()
    if repo is None:
        raise HTTPException(500, "Automation engine is not initialized")

    rules = repo.list_rules()
    return [_rule_to_dict(r) for r in rules]


@router.post("/reload")
def reload_rules():
    repo = rules_repo()
    if repo is None:
        raise HTTPException(500, "Automation engine is not initialized")

    try:
        loaded = load_rules_from_yaml("data/rules.yaml", repo)
        return {
            "ok": True,
            "rules_count": len(loaded),
        }
    except FileNotFoundError:
        raise HTTPException(404, "rules.yaml not found in ./data")
    except Exception as e:
        raise HTTPException(500, f"rules load failed: {e}")



from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Работа с rules.yaml (упрощённый CRUD)
# ---------------------------------------------------------------------------

RULES_FILE = Path("data") / "rules.yaml"


def _load_rules_file() -> Dict[str, Any]:
    """
    Читаем rules.yaml.
    Если файла нет или структура другая — аккуратно нормализуем к {rules: []}.
    """
    if not RULES_FILE.exists():
        return {"rules": []}

    with RULES_FILE.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    if not isinstance(data, dict):
        data = {}

    rules = data.get("rules")
    if not isinstance(rules, list):
        rules = []
    data["rules"] = rules
    return data


def _save_rules_file(data: Dict[str, Any]) -> None:
    RULES_FILE.parent.mkdir(parents=True, exist_ok=True)
    with RULES_FILE.open("w", encoding="utf-8") as f:
        yaml.safe_dump(
            data,
            f,
            sort_keys=False,
            allow_unicode=True,
            default_flow_style=False,
        )


# ---------- DTO для "нового" формата правил (как в движке) -----------------


class ConditionDTO(BaseModel):
    tag: str
    op: str
    operand: Optional[Any] = None
    range_min: Optional[float] = None
    range_max: Optional[float] = None
    hysteresis: Optional[float] = None
    min_duration_ms: Optional[int] = None
    on_bad: Optional[str] = None         # FAIL / SKIP / USE_LAST_OK
    description: Optional[str] = None


class ConditionGroupDTO(BaseModel):
    all_of: List[ConditionDTO] = []
    any_of: List[ConditionDTO] = []


class MqttDTO(BaseModel):
    topic: str
    payload: Any
    qos: int = 0
    retain: bool = False


class TriggerDTO(BaseModel):
    rule_id: str
    context: Dict[str, Any] = {}


class LogDTO(BaseModel):
    message: str
    level: str = "INFO"
    extra: Dict[str, Any] = {}

class RuleOptionsDTO(BaseModel):
    fire_mode: str = "edge"
    min_interval_sec: float or None = None

class ActionFullDTO(BaseModel):
    id: Optional[str] = None
    type: str
    delay_ms: Optional[int] = None
    sequential_order: Optional[int] = None
    description: Optional[str] = None

    mqtt: Optional[MqttDTO] = None
    trigger: Optional[TriggerDTO] = None
    log: Optional[LogDTO] = None


class RuleSaveDTO(BaseModel):
    """
    Модель одного правила для сохранения из UI.

    Поддерживаем ДВА варианта:

    1) "Новый" (структурный) формат:
       - id, name, enabled, description, virtual_tags, conditions{all_of/any_of}, actions[...]
    2) "Старый" упрощённый формат:
       - name, enabled, description, condition (строка), actions[ {type, topic, ...} ]

    UI может прислать и то и другое, мы сохраним максимально богато.
    """

    # общий
    id: Optional[str] = None
    name: str
    enabled: bool = True
    description: Optional[str] = ""

    # новый формат
    virtual_tags: List[str] = []
    conditions: Optional[ConditionGroupDTO] = None

    # старый формат
    condition: Optional[str] = None

    actions: List[ActionFullDTO]

    options: Optional[RuleOptionsDTO] = None


class RuleNameDTO(BaseModel):
    name: str


@router.post("/rules/save")
def save_rule(body: RuleSaveDTO):
    """
    Создать или обновить правило (upsert) в rules.yaml.

    Если пришёл структурный conditions/all_of/any_of — сохраняем в "новом"
    формате, как его понимает твой rules_loader.

    Если conditions нет, а есть только condition-строка — записываем её
    в поле "condition" (для старого интерпретатора), но actions всё равно
    сохраняем в новом виде (mqtt/trigger/log).
    """
    from fastapi import HTTPException

    name = (body.name or "").strip()
    if not name:
        raise HTTPException(400, "Имя правила не может быть пустым")

    data = _load_rules_file()
    rules = data.get("rules", [])

    # выкидываем старую версию с таким же name
    rules = [r for r in rules if str(r.get("name", "")).strip() != name]

    # --- формируем новый словарь правила ---
    rule_dict: Dict[str, Any] = {}

    # id: если не задано — берём name в виде "слага"
    rule_id = (body.id or "").strip()
    if not rule_id:
        # простая нормализация: латиница/цифры/подчёркивания
        import re
        slug = re.sub(r"\s+", "_", name.strip())
        slug = re.sub(r"[^0-9A-Za-z_]+", "", slug)
        rule_id = slug or name
    rule_dict["id"] = rule_id

    rule_dict["name"] = name
    # маппим enabled → status
    rule_dict["status"] = "enabled" if body.enabled else "disabled"
    rule_dict["description"] = body.description or ""
    rule_dict["virtual_tags"] = list(body.virtual_tags or [])

    # условия
    if body.conditions is not None:
        # структурный формат
        rule_dict["conditions"] = body.conditions.model_dump(exclude_none=True)
    else:
        # старый формат: просто условие-строка
        rule_dict["condition"] = body.condition or ""

    # НОВОЕ: опции fire_mode / min_interval_sec
    if body.options is not None:
        rule_dict["options"] = body.options.model_dump(exclude_none=True)

    # actions в "движковом" виде
    rule_dict["actions"] = [
        a.model_dump(exclude_none=True)
        for a in (body.actions or [])
    ]

    # добавляем в список и сохраняем
    rules.append(rule_dict)
    data["rules"] = rules
    _save_rules_file(data)

    return {"ok": True, "id": rule_id}


@router.post("/rules/delete")
def delete_rule(body: RuleNameDTO):
    """
    Удалить правило по имени из rules.yaml.
    Ориентируемся на поле "name" (как и при save_rule).
    """
    from fastapi import HTTPException

    name = (body.name or "").strip()
    if not name:
        raise HTTPException(400, "Имя правила не задано")

    data = _load_rules_file()
    rules = data.get("rules", [])

    new_rules = [r for r in rules if str(r.get("name", "")).strip() != name]

    if len(new_rules) == len(rules):
        raise HTTPException(404, "Правило не найдено")

    data["rules"] = new_rules
    _save_rules_file(data)
    return {"ok": True}

