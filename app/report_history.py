from typing import List

from fastapi import APIRouter
from fastapi import Depends

from app.db import ReportHistoryManager
from app.models import User, ReportRecord, CreateReportRecord
from app.users import current_active_user
from config.logger import client_logger

logger = client_logger.get_logger(__name__)

router = APIRouter()


@router.get("/", response_model=List[ReportRecord])
async def get_report_history(user: User = Depends(current_active_user)):
    return await ReportHistoryManager(user).retrieve_records()


@router.get("/{report_id}", response_model=ReportRecord)
async def get_record(report_id: str, user: User = Depends(current_active_user)):
    return await ReportHistoryManager(user).get_record(report_id)


@router.post("/", response_model=ReportRecord)
async def create_record(record: CreateReportRecord, user: User = Depends(current_active_user)):
    return await ReportHistoryManager(user).create_record(record)
