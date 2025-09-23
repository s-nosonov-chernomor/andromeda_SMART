from fastapi import APIRouter, HTTPException, Body, Request
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates
from datetime import datetime
import os, yaml, shutil, time
from pathlib import Path
import subprocess, platform


from app.core.config import settings  # ← добавили

from app.core.validate_andromeda import validate_andromeda_cfg

# путь к файлу конфигурации «Андромеды»
ANDROMEDA_CFG_PATH = os.environ.get("ANDROMEDA_CFG", "./agent.yaml")

router = APIRouter()
templates = Jinja2Templates(directory="app/web/templates")

def _ensure_exists():
    if not os.path.exists(ANDROMEDA_CFG_PATH):
        os.makedirs(os.path.dirname(ANDROMEDA_CFG_PATH) or ".", exist_ok=True)
        with open(ANDROMEDA_CFG_PATH, "w", encoding="utf-8") as f:
            f.write("# andromeda config\n")

async def _read_body_text(request: Request) -> str:
    """
    Универсально читаем тело: поддерживаем как text/plain, так и JSON
    с ключами text / yaml / content.
    """
    ct = (request.headers.get("content-type") or "").split(";")[0].strip().lower()
    if ct in ("application/json", "application/*+json"):
        try:
            data = await request.json()
        except Exception:
            data = {}
        return (data.get("text") or data.get("yaml") or data.get("content") or "").strip()
    # text/plain (или другое) — читаем «сырым» текстом
    try:
        return (await request.body()).decode("utf-8", errors="replace")
    except Exception:
        return ""

KEEP_ANDROMEDA_BAKS = None  # не используем теперь, берём из settings/backups секции

def _backup_and_write_andromeda(raw: str) -> str:
    """
    Сохраняем ANDROMEDA_CFG_PATH.
    Бэкап кладём в settings.backups_dir (или backups.dir из основного YAML),
    имя: <stem>-YYYYmmdd-HHMMSS<suffix>.bak
    Ротация: оставляем последние backups_keep (или settings.backups_keep).
    Возвращает имя бэкапа (basename) или '' если исходного файла ещё не было.
    """
    # куда пишем основной файл
    target = Path(ANDROMEDA_CFG_PATH).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)

    # параметры бэкапов — как у основного YAML
    cfg = settings.get_cfg() or {}
    bsec = (cfg or {}).get("backups", {})
    backups_dir = Path(bsec.get("dir", settings.backups_dir)).resolve()
    backups_keep = int(bsec.get("keep", settings.backups_keep) or 0)
    backups_dir.mkdir(parents=True, exist_ok=True)

    # если есть старый файл — делаем бэкап
    backup_name = ""
    if target.exists():
        ts = time.strftime("%Y%m%d-%H%M%S")
        backup_name = f"{target.stem}-{ts}{target.suffix}.bak"
        try:
            shutil.copy2(target, backups_dir / backup_name)
        except Exception:
            backup_name = ""  # если копия не удалась — не роняем запись

        # ротация: оставляем только последние backups_keep
        if backups_keep > 0:
            patt = f"{target.stem}-*{target.suffix}.bak"
            files = sorted(backups_dir.glob(patt))  # по возрастанию времени в имени
            extra = len(files) - backups_keep
            if extra > 0:
                for old in files[:extra]:
                    try:
                        old.unlink()
                    except Exception:
                        pass

    # атомарная запись самого конфига
    tmp = target.with_suffix(target.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(raw)
    os.replace(tmp, target)

    return backup_name


@router.get("/api/andromeda/config", response_class=PlainTextResponse)
def get_cfg():
    _ensure_exists()
    try:
        with open(ANDROMEDA_CFG_PATH, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        raise HTTPException(500, f"read error: {e}")

@router.put("/api/andromeda/config")
async def put_cfg(request: Request):
    # 1) читаем тело как text/plain или JSON {text|yaml|content}
    raw = await _read_body_text(request)
    if not raw.strip():
        raise HTTPException(400, "Пустое тело запроса: не получен текст YAML.")

    # 2) синтаксическая проверка YAML
    try:
        doc = yaml.safe_load(raw)
    except yaml.YAMLError as e:
        raise HTTPException(400, f"YAML синтаксическая ошибка: {e}")

    # 3) нормализация и корневой тип
    if doc is None:
        doc = {}
    if isinstance(doc, list) and len(doc) == 1 and isinstance(doc[0], dict):
        doc = doc[0]
    if not isinstance(doc, dict):
        raise HTTPException(400, "корневой YAML должен быть объектом")

    # 4) предметная валидация
    try:
        validate_andromeda_cfg(doc)   # если валидатор ждёт dict
    except TypeError:
        validate_andromeda_cfg(raw)   # если валидатор ждёт сырой текст

    # 5) запись, бэкап рядом с основным YAML и ротация
    try:
        backup_name = _backup_and_write_andromeda(raw)
    except Exception as e:
        raise HTTPException(500, f"write error: {e}")

    return {"ok": True, "backup": backup_name}



@router.get("/ui/andromeda", response_class=HTMLResponse)
def ui_andromeda(request: Request):
    # Меню уже есть в base.html — просто рендерим контент страницы
    return templates.TemplateResponse("andromeda.html", {"request": request})

@router.post("/api/andromeda/restart")
def restart_andromeda_agent():
    """
    Перезапускает systemd-сервис агентa Андромеды.
    Требует Linux и права на systemctl (либо sudoers/polkit).
    """
    if platform.system() != "Linux":
        raise HTTPException(status_code=501, detail="Перезапуск доступен только в Linux (systemd)")

    cmd = ["systemctl", "restart", "agent.service"]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if res.returncode != 0:
            err = (res.stderr or res.stdout or "").strip()
            raise HTTPException(status_code=500, detail=f"systemctl exit {res.returncode}: {err}")
        return {"ok": True}
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="systemctl timeout")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"restart failed: {e}")
