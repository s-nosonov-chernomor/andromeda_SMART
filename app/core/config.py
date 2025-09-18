# app/core/config.py
from __future__ import annotations

import os
from typing import Any, Dict

import yaml
from pydantic import Field, PrivateAttr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Глобальные настройки приложения + «мост» к YAML-конфигу USPD.
    - Значения можно переопределять переменными окружения
      (SESSION_SECRET, ACCOUNTS_PATH, USPD_CFG, ANDROMEDA_CFG, WEB_PORT).
    - YAML конфиг читается методом load_yaml_config(); его части попадают в .debug и ._runtime_cfg.
    """

    # ---- web/auth ----
    session_secret: str = Field("CHANGE_ME_TO_RANDOM_LONG_SECRET", alias="SESSION_SECRET")
    accounts_path: str = Field("./data/users.json", alias="ACCOUNTS_PATH")
    web_port: int = Field(8080, alias="WEB_PORT")

    # ---- пути к конфигаам ----
    uspd_cfg_path: str = Field("./config.yaml", alias="USPD_CFG")
    andromeda_cfg_path: str = Field("./andromeda.yaml", alias="ANDROMEDA_CFG")

    # ---- часть из YAML (подхватываем в load_yaml_config) ----
    debug: Dict[str, Any] = Field(default_factory=lambda: {
        "enabled": False,
        "log_reads": False,
        "summary_every_s": 0,
    })

    # приватное хранилище текущего YAML
    _runtime_cfg: Dict[str, Any] = PrivateAttr(default_factory=dict)

    # pydantic v2 конфиг
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # ───────────────────────── helpers ─────────────────────────

    def load_yaml_config(self) -> Dict[str, Any]:
        """
        Читает YAML из self.uspd_cfg_path, кладёт в _runtime_cfg и подхватывает .debug, если есть.
        Возвращает словарь конфига.
        """
        path = self.uspd_cfg_path
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
        except FileNotFoundError:
            data = {}
        except Exception as e:
            # не падаем на старте — просто возвращаем пустой конфиг
            print(f"[settings] YAML read error {path}: {e}")
            data = {}

        # сохранить целиком
        self._runtime_cfg = data

        # подхватить debug из YAML (если есть)
        dbg = (data.get("debug") or {}) if isinstance(data, dict) else {}
        if isinstance(dbg, dict) and dbg:
            # не затираем целиком, а обновляем дефолт
            merged = dict(self.debug)
            merged.update(dbg)
            self.debug = merged

        return data

    def get_cfg(self) -> Dict[str, Any]:
        """Копия последнего загруженного YAML-конфига."""
        return dict(self._runtime_cfg)

    # Удобные геттеры, чтобы не падать, если в YAML нет секций
    @property
    def polling(self) -> Dict[str, Any]:
        return (self._runtime_cfg.get("polling") or {}) if isinstance(self._runtime_cfg, dict) else {}

    @property
    def mqtt(self) -> Dict[str, Any]:
        return (self._runtime_cfg.get("mqtt") or {}) if isinstance(self._runtime_cfg, dict) else {}

    @property
    def history(self) -> Dict[str, Any]:
        return (self._runtime_cfg.get("history") or {}) if isinstance(self._runtime_cfg, dict) else {}

    @property
    def lines(self) -> Any:
        return (self._runtime_cfg.get("lines") or []) if isinstance(self._runtime_cfg, dict) else []


# единый синглтон
settings = Settings()
