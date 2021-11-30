from pathlib import Path
from typing import List

from fastapi import APIRouter, Depends, Request, status
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

from app.db import ReportHistoryManager
from app.models import User, ReportRecord, CreateReportRecord
from app.users import current_active_user, optional_current_active_user
from config.logger import client_logger

logger = client_logger.get_logger(__name__)
BASE_PATH = Path(__file__).parent.parent

TEMPLATES = Jinja2Templates(directory=str(BASE_PATH / "templates"))

router = APIRouter()


@router.get("/", response_model=List[ReportRecord])
async def get_report_history(request: Request, user: User = Depends(optional_current_active_user)):
    if user:
        return TEMPLATES.TemplateResponse(
            "report_history.html",
            {
                "request": request,
                "report_history": await ReportHistoryManager(user).retrieve_records(),
                "user_name": user.email,
                "user_id": user.id,
             }
        )
    else:
        logger.info("Unauthorized user, redirecting to sign in page")
        return RedirectResponse(url="/auth/signin", status_code=status.HTTP_302_FOUND)


@router.get("/{report_id}", response_model=ReportRecord)
async def get_record(report_id: str, user: User = Depends(current_active_user)):
    return await ReportHistoryManager(user).get_record(report_id)


@router.post("/", response_model=ReportRecord)
async def create_record(record: CreateReportRecord, user: User = Depends(current_active_user)):
    return await ReportHistoryManager(user).create_record(record)
