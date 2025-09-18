# app/api/routes/journal.py
from fastapi import APIRouter, Query
from typing import Optional, List
from datetime import datetime, timedelta
from app.db.session import SessionLocal
from app.db.models import TelemetryEvent
router = APIRouter()

@router.get("/api/events")
def events(
    limit: int = Query(200, ge=1, le=5000),
    object: Optional[str] = None,
    param: Optional[str] = None,
    topic: Optional[str] = None,
    line: Optional[str] = None,
    code_ge: Optional[int] = None,
    since_s: Optional[int] = None,
    has_value: Optional[int] = None,
):
    with SessionLocal() as s:
        q = s.query(TelemetryEvent)
        if object: q = q.filter(TelemetryEvent.object==object)
        if param:  q = q.filter(TelemetryEvent.param==param)
        if topic:  q = q.filter(TelemetryEvent.topic==topic)
        if line:   q = q.filter(TelemetryEvent.line==line)
        if code_ge is not None: q = q.filter(TelemetryEvent.code>=code_ge)
        if since_s: q = q.filter(TelemetryEvent.ts >= datetime.utcnow()-timedelta(seconds=since_s))
        if has_value == 1: q = q.filter(TelemetryEvent.value.isnot(None))
        elif has_value == 0: q = q.filter(TelemetryEvent.value.is_(None))
        rows = q.order_by(TelemetryEvent.id.desc()).limit(limit).all()
        return rows[::-1]
