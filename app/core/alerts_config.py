# app/core/alerts_config.py
from __future__ import annotations
from typing import Any, Dict
from pathlib import Path
import os, time, shutil, json
import yaml

from app.core.config import settings

ALERTS_CFG_PATH = Path(os.environ.get("ALERTS_CFG", "./alerts.yaml")).resolve()
_SKELETON: Dict[str, Any] = {"flows": []}

def _ensure_exists() -> None:
    ALERTS_CFG_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not ALERTS_CFG_PATH.exists():
        ALERTS_CFG_PATH.write_text(yaml.safe_dump(_SKELETON, allow_unicode=True, sort_keys=False), encoding="utf-8")

def _normalize(cfg: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(cfg, dict): return dict(_SKELETON)
    cfg.setdefault("flows", [])
    # переносим токены из legacy-поля telegram -> options.telegram
    for f in cfg.get("flows", []):
        opts = f.setdefault("options", {})
        if f.get("type") == "telegram":
            t = (opts.get("telegram") or {})
            legacy = (f.get("telegram") or {})
            if legacy and not t:
                opts["telegram"] = {"bot_token": legacy.get("bot_token",""), "chat_id": legacy.get("chat_id","")}
            f.pop("telegram", None)  # чистим наследие
    return cfg

def read_alerts_cfg() -> Dict[str, Any]:
    _ensure_exists()
    raw = ALERTS_CFG_PATH.read_text(encoding="utf-8")
    if not raw.strip():
        return dict(_SKELETON)
    # пробуем YAML, потом JSON
    try:
        data = yaml.safe_load(raw)
    except Exception:
        try: data = json.loads(raw)
        except Exception: data = dict(_SKELETON)
    if data is None: data = {}
    return _normalize(data)

def save_alerts_cfg(cfg: Dict[str, Any]) -> str:
    _ensure_exists()
    cfg = _normalize(cfg)

    # бэкап в общий каталог
    backup_name = ""
    bsec = (settings.get_cfg() or {}).get("backups", {})
    backups_dir = Path(bsec.get("dir", settings.backups_dir)).resolve()
    backups_keep = int(bsec.get("keep", settings.backups_keep) or 0)
    backups_dir.mkdir(parents=True, exist_ok=True)

    if ALERTS_CFG_PATH.exists():
        ts = time.strftime("%Y%m%d-%H%M%S")
        backup_name = f"{ALERTS_CFG_PATH.stem}-{ts}{ALERTS_CFG_PATH.suffix}.bak"
        try:
            shutil.copy2(ALERTS_CFG_PATH, backups_dir / backup_name)
        except Exception:
            backup_name = ""

        if backups_keep > 0:
            patt = f"{ALERTS_CFG_PATH.stem}-*{ALERTS_CFG_PATH.suffix}.bak"
            files = sorted(backups_dir.glob(patt))
            extra = len(files) - backups_keep
            for old in files[:max(0, extra)]:
                try: old.unlink()
                except Exception: pass

    # атомарная запись YAML
    tmp = ALERTS_CFG_PATH.with_suffix(ALERTS_CFG_PATH.suffix + ".tmp")
    tmp.write_text(yaml.safe_dump(cfg, allow_unicode=True, sort_keys=False), encoding="utf-8")
    tmp.replace(ALERTS_CFG_PATH)
    return backup_name
