from fastapi import APIRouter, FastAPI
from fastapi.staticfiles import StaticFiles

from app import map_reduce, report_history, auth
from app.users import fastapi_users, cookie_authentication

main_router = APIRouter()

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
