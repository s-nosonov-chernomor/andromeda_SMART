# app/api/routes/current.py
from fastapi import APIRouter
from app.core.state import all_current
router = APIRouter()
@router.get("/api/current")
def current():
    return all_current()
