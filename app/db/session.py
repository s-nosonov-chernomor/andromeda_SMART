# app/db/session.py
from __future__ import annotations

import os
from pathlib import Path
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.config import settings
from app.db.models import Base  # важно, чтобы модели были импортированы


def _ensure_sqlite_dir(db_url: str) -> None:
    # sqlite:///./data/data.db  → ./data
    if db_url.startswith("sqlite"):
        # Отбрасываем префикс sqlite:///
        prefix = "sqlite:///"
        if db_url.startswith(prefix):
            fs_path = db_url[len(prefix):]
            # :memory: — ничего не делаем
            if fs_path == ":memory:":
                return
            d = Path(fs_path).resolve().parent
            d.mkdir(parents=True, exist_ok=True)


# создаём engine
db_url = settings.db_url
_ensure_sqlite_dir(db_url)
engine = create_engine(db_url, future=True)

# фабрика сессий
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)


def init_db() -> None:
    """Создать таблицы, если их ещё нет."""
    Base.metadata.create_all(bind=engine)


def get_db() -> Generator:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
