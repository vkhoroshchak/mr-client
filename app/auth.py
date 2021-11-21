from pathlib import Path

from fastapi import APIRouter, Depends, Request, status
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi_users import models
from fastapi_users.manager import (
    BaseUserManager,
    InvalidPasswordException,
    UserAlreadyExists,
)

from app.users import get_user_manager, RegisterForm
from config.logger import client_logger

logger = client_logger.get_logger(__name__)
BASE_PATH = Path(__file__).parent.parent

templates = Jinja2Templates(directory=str(BASE_PATH / "templates"))

router = APIRouter()


@router.get("/signup/")
def signup(request: Request):
    return templates.TemplateResponse("signup.html", {"request": request})


@router.post("/signup/")
async def signup(request: Request,
                 user_manager: BaseUserManager[models.UC, models.UD] = Depends(get_user_manager),
                 ):
    form = RegisterForm(request)
    await form.load_data()
    if await form.is_valid():
        try:
            user = models.BaseUserCreate(email=form.username, password=form.password)
            await user_manager.create(user, safe=True, request=request)
        except UserAlreadyExists:
            return templates.TemplateResponse("signup.html", form.__dict__)
        except InvalidPasswordException:
            return templates.TemplateResponse("signup.html", form.__dict__)
        else:
            return RedirectResponse(url="/report_history", status_code=status.HTTP_302_FOUND)


@router.get("/signin/")
def signin(request: Request):
    return templates.TemplateResponse("signin.html", {"request": request})
