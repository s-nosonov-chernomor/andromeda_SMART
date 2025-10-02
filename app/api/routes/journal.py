# app/api/routes/journal.py
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from app.db.session import get_db
from app.db.models import TelemetryEvent

router = APIRouter()

def _row_to_dict(r: TelemetryEvent):

    # ts может быть naive; считаем его UTC и добавляем Z
    ts = r.ts
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        ts_out = ts.isoformat()
    else:
        ts_out = ts
    return {
        "id": r.id,
        "ts": ts_out,
        "topic": r.topic,
        "object": r.object,
        "param": r.param,
        "value": r.value,
        "code": r.code,
        "message": r.message,
        "line": r.line,
        "unit_id": r.unit_id,
        "register_type": r.register_type,
        "address": r.address,
        "silent_for_s": r.silent_for_s,
    }

def _apply_filters(q, *, object: Optional[str], param: Optional[str], line: Optional[str],
                   topic: Optional[str], qtext: Optional[str],
                   has_value: Optional[int], code_min: Optional[int], code_max: Optional[int],
                   since_s: Optional[int]):
    if object:
        q = q.filter(TelemetryEvent.object == object)
    if param:
        q = q.filter(TelemetryEvent.param == param)
    if line:
        q = q.filter(TelemetryEvent.line == line)
    if topic:
        q = q.filter(TelemetryEvent.topic == topic)
    if has_value is not None:
        if has_value == 1:
            q = q.filter(TelemetryEvent.value.isnot(None))
        else:
            q = q.filter(TelemetryEvent.value.is_(None))
    if code_min is not None:
        q = q.filter(TelemetryEvent.code >= code_min)
    if code_max is not None:
        q = q.filter(TelemetryEvent.code <= code_max)
    if since_s and since_s > 0:
        dt = datetime.utcnow() - timedelta(seconds=since_s)
        q = q.filter(TelemetryEvent.ts >= dt)
    if qtext:
        like = f"%{qtext}%"
        q = q.filter(
            (TelemetryEvent.object.ilike(like)) |
            (TelemetryEvent.param.ilike(like))  |
            (TelemetryEvent.topic.ilike(like))  |
            (TelemetryEvent.message.ilike(like))
        )
    return q

# Основной путь, как ждёт фронт
@router.get("/events")
def events(
    limit: int = Query(200, ge=1, le=2000),
    object: Optional[str] = None,
    param: Optional[str] = None,
    line: Optional[str] = None,
    topic: Optional[str] = None,
    q: Optional[str] = Query(None, alias="q"),
    has_value: Optional[int] = Query(None),  # 1 или 0
    code_min: Optional[int] = None,
    code_max: Optional[int] = None,
    since_s: Optional[int] = None,
    db: Session = Depends(get_db)
):
    qq = db.query(TelemetryEvent)
    qq = _apply_filters(
        qq,
        object=object, param=param, line=line, topic=topic,
        qtext=q, has_value=has_value,
        code_min=code_min, code_max=code_max,
        since_s=since_s
    )
    rows: List[TelemetryEvent] = qq.order_by(TelemetryEvent.id.desc()).limit(limit).all()
    # отдаём в порядке «старые -> новые», чтобы таблица рисовалась сверху вниз ровно
    rows = rows[::-1]
    return [_row_to_dict(r) for r in rows]

# Совместимость со старым путём /api/journal/events
@router.get("/journal/events")
def events_alias(
    limit: int = Query(200, ge=1, le=2000),
    object: Optional[str] = None,
    param: Optional[str] = None,
    line: Optional[str] = None,
    topic: Optional[str] = None,
    q: Optional[str] = Query(None, alias="q"),
    has_value: Optional[int] = Query(None),
    code_min: Optional[int] = None,
    code_max: Optional[int] = None,
    since_s: Optional[int] = None,
    db: Session = Depends(get_db)
):
    return events(limit, object, param, line, topic, q, has_value, code_min, code_max, since_s, db)  # type: ignore
