# app/api/routes/current.py
from fastapi import APIRouter, Query
from app.services.current_store import current_store
import io
from fastapi.responses import StreamingResponse
from openpyxl import Workbook
from datetime import datetime, timezone

router = APIRouter()

def _apply_filters(
    items,
    q: str = "",
    line: str = "",
    object_name: str = "",
    status: str = "",
):
    q = (q or "").strip().lower()
    line = (line or "").strip()
    object_name = (object_name or "").strip()
    status = (status or "").strip()

    out = []
    for x in items:
        obj = str(x.get("object") or "")
        prm = str(x.get("param") or "")
        xline = str(x.get("line") or "")
        code = int(x.get("code") or 0)
        is_null = x.get("value") is None

        if q and q not in obj.lower() and q not in prm.lower():
            continue
        if line and xline != line:
            continue
        if object_name and obj != object_name:
            continue
        if status == "ok" and not (code == 0 and not is_null):
            continue
        if status == "err" and code == 0:
            continue
        if status == "null" and not is_null:
            continue

        out.append(x)

    return out

@router.get("/api/current")
def current(
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=500),
    q: str = Query(""),
    line: str = Query(""),
    object_name: str = Query("", alias="object"),
    status: str = Query(""),
):
    items = current_store.list()

    items.sort(key=lambda x: (
        str(x.get("object") or ""),
        str(x.get("param") or ""),
    ))

    filtered = _apply_filters(
        items,
        q=q,
        line=line,
        object_name=object_name,
        status=status,
    )

    total = len(items)
    filtered_total = len(filtered)

    start = (page - 1) * page_size
    end = start + page_size
    page_items = filtered[start:end]

    pages = max(1, (filtered_total + page_size - 1) // page_size)

    return {
        "items": page_items,
        "total": total,
        "filtered_total": filtered_total,
        "page": page,
        "page_size": page_size,
        "pages": pages,
    }

@router.get("/api/current/export")
def export_current_xlsx():
    data = current_store.list()

    headers = [
        "Объект","Параметр","Значение",
        "Время опроса","Время публикации",
        "Статус","Описание",
        "Линия","Unit","Тип","Адрес"
    ]

    wb = Workbook()
    ws = wb.active
    ws.title = "current"
    ws.append(headers)

    for x in data:
        code = int(x.get("code") or 0)
        val  = x.get("value", None)
        # как на фронте: null → «нет данных», 0 → ok, иначе err <code>
        if val is None:
            status = "нет данных"
        elif code == 0:
            status = "ok"
        else:
            status = f"err {code}"

        def _loc(ts: str) -> str:
            if not ts: return ""
            try:
                # поддержим Z и naive → считаем UTC
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00")) if isinstance(ts, str) else None
                if dt and dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                if dt:
                    return dt.astimezone().isoformat(timespec="seconds")
            except Exception:
                pass
            return str(ts)

        ws.append([
            x.get("object",""),
            x.get("param",""),
            val,

            _loc(x.get("last_ok_ts","") or ""),
            _loc(x.get("last_pub_ts", "") or ""),

            status,
            x.get("message","") or "",
            x.get("line",""),
            x.get("unit_id",""),
            x.get("register_type",""),
            x.get("address",""),
        ])

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="current.xlsx"'},
    )

@router.get("/api/current/meta")
def current_meta():
    items = current_store.list()

    lines = sorted({str(x.get("line") or "") for x in items if str(x.get("line") or "")})
    objects = sorted({str(x.get("object") or "") for x in items if str(x.get("object") or "")})

    return {
        "lines": lines,
        "objects": objects,
        "total": len(items),
    }