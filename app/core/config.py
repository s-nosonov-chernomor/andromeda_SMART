# app/core/config.py
from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import yaml
from pydantic import Field, PrivateAttr
from pydantic_settings import BaseSettings
import time
from app.core.validate_cfg import validate_cfg

class Settings(BaseSettings):
    # секрет для cookie-сессий
    session_secret: str = Field(default="change-me-please")

    # путь к основному YAML (можно переопределить переменной окружения CONFIG_FILE)
    config_file: str = Field(default="config.yaml", validation_alias="CONFIG_FILE")

    # путь к файлу пользователей (можно переопределить переменной окружения ACCOUNTS_FILE)
    accounts_file: str = Field(default="data/accounts.json", validation_alias="ACCOUNTS_FILE")

    # внутреннее хранилище загруженного YAML
    _cfg: Dict[str, Any] = PrivateAttr(default_factory=dict)
    _config_path: Path | None = PrivateAttr(default=None)
    _accounts_path: Path | None = PrivateAttr(default=None)

    # куда и сколько бэкапов хранить (можно переопределить в YAML через секцию backups)
    backups_dir: str = "./data/backups"
    backups_keep: int = 10


    # ───────── пути ─────────
    @property
    def config_path(self) -> Path:
        if self._config_path is None:
            p = Path(self.config_file)
            if not p.is_absolute():
                p = Path.cwd() / p
            self._config_path = p
        return self._config_path

    # для совместимости со старым кодом
    @property
    def cfg_path(self) -> str:
        return str(self.config_path)

    @property
    def accounts_path(self) -> str:
        """Абсолютный путь к JSON с пользователями; гарантируем наличие директории."""
        if self._accounts_path is None:
            p = Path(self.accounts_file)
            if not p.is_absolute():
                p = Path.cwd() / p
            p.parent.mkdir(parents=True, exist_ok=True)
            self._accounts_path = p
        return str(self._accounts_path)

    # ───────── YAML cfg ─────────
    @property
    def cfg(self) -> Dict[str, Any]:
        return self._cfg

    def get_cfg(self) -> Dict[str, Any]:
        return self._cfg

    def set_cfg(self, data: Dict[str, Any]) -> None:
        self._cfg = data or {}

    def load_yaml_config(self) -> None:
        p = self.config_path
        if p.exists():
            with open(p, "r", encoding="utf-8") as f:
                self._cfg = yaml.safe_load(f) or {}
                validate_cfg(self._cfg)  # выбросит ValueError, если что-то не так
        else:
            self._cfg = {}

    def save_yaml_config(self, new_cfg: dict) -> str:
        """
        Сохраняет YAML на диск, предварительно кладёт бэкап текущего файла
        в backups_dir и делает ротацию (оставляем последние N).
        Возвращает только имя файла бэкапа (без пути) либо '' если бэкапа не было.
        """
        cfg_path = Path(self.cfg_path).resolve()

        # настройки бэкапов — берём из new_cfg.backups или из дефолтов Settings
        bsec = (new_cfg or {}).get("backups", {})
        backups_dir = Path(bsec.get("dir", self.backups_dir)).resolve()
        backups_keep = int(bsec.get("keep", self.backups_keep) or 0)

        backups_dir.mkdir(parents=True, exist_ok=True)

        backup_name = ""
        if cfg_path.exists():
            ts = time.strftime("%Y%m%d-%H%M%S")
            # пример: config-20250918-153012.yaml.bak
            backup_name = f"{cfg_path.stem}-{ts}{cfg_path.suffix}.bak"
            shutil.copy2(cfg_path, backups_dir / backup_name)

            # ротация: оставляем последние backups_keep
            if backups_keep > 0:
                patt = f"{cfg_path.stem}-*{cfg_path.suffix}.bak"
                files = sorted(backups_dir.glob(patt))
                extra = len(files) - backups_keep
                if extra > 0:
                    for old in files[:extra]:
                        try:
                            old.unlink()
                        except Exception:
                            pass

        # записываем новый YAML
        with open(cfg_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(new_cfg, f, allow_unicode=True, sort_keys=False)

        # обновляем кеш настроек
        self._cfg = new_cfg
        return backup_name


    # ───────── удобные секции ─────────
    @property
    def mqtt(self) -> Dict[str, Any]:
        return self._cfg.get("mqtt", {})

    @property
    def polling(self) -> Dict[str, Any]:
        return self._cfg.get("polling", {})

    @property
    def history(self) -> Dict[str, Any]:
        return self._cfg.get("history", {})

    @property
    def debug(self) -> Dict[str, Any]:
        return self._cfg.get("debug", {})

    @property
    def db_url(self) -> str:
        # дефолт «как раньше»
        return self._cfg.get("db", {}).get("url", "sqlite:///./data/data.db")


settings = Settings()
