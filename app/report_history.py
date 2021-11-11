from fastapi import (
    APIRouter
)
from fastapi import Depends

from app.db import ReportHistoryManager
from app.models import User, ReportRecord, ReportHistory, CreateReportRecord
from app.users import current_active_user
from config.logger import client_logger

logger = client_logger.get_logger(__name__)

router = APIRouter()


# TODO: Fix issue with response model for report history
@router.get("/")
async def get_report_history(user: User = Depends(current_active_user)):
    return await ReportHistoryManager().retrieve_records()


@router.get("/{report_id}")
async def get_record(report_id: str, user: User = Depends(current_active_user)):
    return await ReportHistoryManager().get_record(report_id)


@router.post("/")
async def create_record(record: CreateReportRecord, user: User = Depends(current_active_user)):
    record.user_id = user.id
    return await ReportHistoryManager().create_record(record)
