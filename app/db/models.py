# app/db/models.py
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Text, Float

Base = declarative_base()

class TelemetryEvent(Base):
    __tablename__ = "telemetry_events"
    id = Column(Integer, primary_key=True)
    topic = Column(String(255), index=True)
    object = Column(String(128), index=True)
    line = Column(String(64), index=True)
    unit_id = Column(Integer, index=True)
    register_type = Column(String(16))
    address = Column(Integer)
    param = Column(String(128), index=True)
    value = Column(String(64), nullable=True)      # NULL = ошибок/молчание/пульс
    code = Column(Integer, default=0)              # коды ошибок (0=OK)
    message = Column(String(128), default="OK")    # текст ошибки/состояния
    silent_for_s = Column(Integer, default=0)      # сколько секунд нет ОК
    ts = Column(DateTime(timezone=True), index=True)

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    username = Column(String(64), unique=True, index=True)
    password_hash = Column(Text)
