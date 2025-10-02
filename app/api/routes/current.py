# app/api/routes/current.py
from fastapi import APIRouter
from app.services.current_store import current_store
import io
from fastapi.responses import StreamingResponse
from openpyxl import Workbook
from datetime import datetime, timezone

router = APIRouter()
@router.get("/api/current")
def current():
    return current_store.list()

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
