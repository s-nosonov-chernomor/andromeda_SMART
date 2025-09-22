# app/api/routes/service.py
from fastapi import APIRouter
import threading, os, time

router = APIRouter(prefix="/api/service")

@router.post("/restart")
def restart_service():
    # Отдаём 200 и мягко завершаем процесс — менеджер (nssm/systemd) поднимет заново
    def _shutdown():
        time.sleep(0.7)
        os._exit(0)
    threading.Thread(target=_shutdown, daemon=True).start()
    return {"ok": True, "message": "Перезапуск…"}
