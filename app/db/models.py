# app/db/models.py
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, DateTime
from datetime import datetime

Base = declarative_base()

class TelemetryEvent(Base):
    __tablename__ = "telemetry_events"

    id            = Column(Integer, primary_key=True)
    topic         = Column(String(255), index=True, nullable=False)

    object        = Column(String(120), index=True, nullable=True)
    param         = Column(String(120), index=True, nullable=True)

    line          = Column(String(64),  index=True, nullable=True)
    unit_id       = Column(Integer,     index=True, nullable=True)
    register_type = Column(String(16),  nullable=True)
    address       = Column(Integer,     nullable=True)

    value         = Column(String(64),  nullable=True)  # null -> нет данных
    code          = Column(Integer,     index=True, default=0)
    message       = Column(String(255), nullable=True)

    silent_for_s  = Column(Integer, default=0)  # сколько секунд «молчит»
    ts            = Column(DateTime, index=True, default=datetime.utcnow)
