from pathlib import Path

from fastapi import APIRouter, Request
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app import map_reduce, report_history, auth
from app.users import fastapi_users, cookie_authentication
from config.logger import client_logger

logger = client_logger.get_logger(__name__)
BASE_PATH = Path(__file__).parent

TEMPLATES = Jinja2Templates(directory=str(BASE_PATH / "templates"))

main_router = APIRouter()


@main_router.get("/")
async def get_main_page(request: Request):
    return TEMPLATES.TemplateResponse(
        "main_page.html",
        {
            "request": request,
        }
    )


main_router.include_router(
    map_reduce.router,
    prefix="/map_reduce",
    responses={404: {"description": "Not found"}},
)
main_router.include_router(
    report_history.router,
    prefix="/report_history",
    responses={404: {"description": "Not found"}},
    tags=["report_history"],
)
main_router.include_router(
    fastapi_users.get_auth_router(cookie_authentication), prefix="/auth/cookie", tags=["auth"]
)
main_router.include_router(
    fastapi_users.get_register_router(), prefix="/auth", tags=["auth"]
)
main_router.include_router(
    fastapi_users.get_reset_password_router(),
    prefix="/auth",
    tags=["auth"],
)
main_router.include_router(
    fastapi_users.get_verify_router(),
    prefix="/auth",
    tags=["auth"],
)
main_router.include_router(fastapi_users.get_users_router(), prefix="/users", tags=["users"])
main_router.include_router(auth.router,
                           prefix="/auth",
                           responses={404: {"description": "Not found"}},
                           tags=["auth"],
                           )
app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

app.include_router(
    main_router,
    responses={404: {"description": "Not found"}},
)
