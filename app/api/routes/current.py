# app/api/routes/current.py
from fastapi import APIRouter
from app.services.current_store import current_store
import io
from fastapi.responses import StreamingResponse
from openpyxl import Workbook

router = APIRouter()
@router.get("/api/current")
def current():
    return current_store.list()

@router.get("/api/current/export")
def export_current_xlsx():
    data = current_store.list()

    # заголовки (как на экране)
    headers = ["Объект","Параметр","Значение","Код","Описание","Линия","Unit","Тип","Адрес","Время"]

    wb = Workbook()
    ws = wb.active
    ws.title = "current"
    ws.append(headers)

    for x in data:
        ws.append([
            x.get("object",""),
            x.get("param",""),
            x.get("value", None),
            x.get("code",""),
            x.get("message",""),
            x.get("line",""),
            x.get("unit_id",""),
            x.get("register_type",""),
            x.get("address",""),
            x.get("ts",""),
        ])

    buf = io.BytesIO(); wb.save(buf); buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="current.xlsx"'},
    )
