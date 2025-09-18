from fastapi import APIRouter, HTTPException, Body, Request
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates
from datetime import datetime
import os, yaml

# путь к файлу конфигурации «Андромеды»
ANDROMEDA_CFG_PATH = os.environ.get("ANDROMEDA_CFG", "./andromeda.yaml")

router = APIRouter()
templates = Jinja2Templates(directory="app/web/templates")

def _ensure_exists():
    if not os.path.exists(ANDROMEDA_CFG_PATH):
        os.makedirs(os.path.dirname(ANDROMEDA_CFG_PATH) or ".", exist_ok=True)
        with open(ANDROMEDA_CFG_PATH, "w", encoding="utf-8") as f:
            f.write("# andromeda config\n")

@router.get("/api/andromeda/config", response_class=PlainTextResponse)
def get_cfg():
    _ensure_exists()
    try:
        with open(ANDROMEDA_CFG_PATH, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        raise HTTPException(500, f"read error: {e}")

@router.put("/api/andromeda/config")
def put_cfg(raw: str = Body(..., media_type="text/plain")):
    # валидация YAML
    try:
        yaml.safe_load(raw)
    except Exception as e:
        raise HTTPException(400, f"YAML parse error: {e}")

    # бэкап и запись
    _ensure_exists()
    backup = f"{ANDROMEDA_CFG_PATH}.{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.bak"
    try:
        if os.path.exists(ANDROMEDA_CFG_PATH):
            os.replace(ANDROMEDA_CFG_PATH, backup)
        with open(ANDROMEDA_CFG_PATH, "w", encoding="utf-8") as f:
            f.write(raw)
    except Exception as e:
        raise HTTPException(500, f"write error: {e}")
    return {"ok": True, "backup": os.path.basename(backup)}

@router.get("/ui/andromeda", response_class=HTMLResponse)
def ui_andromeda(request: Request):
    # Меню уже есть в base.html — просто рендерим контент страницы
    return templates.TemplateResponse("andromeda.html", {"request": request})
