# app/core/config.py
from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import yaml
from pydantic import Field, PrivateAttr
from pydantic_settings import BaseSettings


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
        else:
            self._cfg = {}

    def save_yaml_config(self, data: Dict[str, Any]) -> str:
        """Сохраняет YAML и делает .bak; возвращает имя бэкапа или ''."""
        self._cfg = data or {}
        p = self.config_path
        p.parent.mkdir(parents=True, exist_ok=True)

        backup_name = ""
        if p.exists():
            ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            backup = p.with_suffix(p.suffix + f".{ts}.bak")
            shutil.copy2(p, backup)
            backup_name = backup.name

        with open(p, "w", encoding="utf-8") as f:
            yaml.safe_dump(self._cfg, f, allow_unicode=True, sort_keys=False)

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
