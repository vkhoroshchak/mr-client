import os
from typing import List
from typing import Optional

from dotenv import load_dotenv
from fastapi import Depends, Request, APIRouter, Response, status
from fastapi_users import BaseUserManager, FastAPIUsers
from fastapi_users.authentication import CookieAuthentication
from fastapi_users.db import MongoDBUserDatabase

from app.db import get_user_db
from app.models import User, UserCreate, UserDB, UserUpdate
from app.utils import EmailNotifier
from config.logger import client_logger
from templates.email_templates import RESET_PASSWORD_REQUEST

logger = client_logger.get_logger(__name__)

load_dotenv()

SECRET = os.getenv("SECRET_KEY")
router = APIRouter()


class UserManager(BaseUserManager[UserCreate, UserDB]):
    user_db_model = UserDB
    reset_password_token_secret = SECRET
    verification_token_secret = SECRET

    async def on_after_register(self, user: UserDB, request: Optional[Request] = None):
        logger.info(f"User {user.id} has registered.")

    async def on_after_forgot_password(self, user: UserDB, token: str, request: Optional[Request] = None):
        reset_password_link = f"{os.getenv('SITE_URL')}/auth/reset-password?token={token}"
        EmailNotifier(user.email).send_email(
            RESET_PASSWORD_REQUEST,
            {"reset_password_link": reset_password_link}
        )

        logger.info(f"User {user.id} has forgot their password. Reset token: {token}")

    async def on_after_request_(self, user: UserDB, token: str, request: Optional[Request] = None):
        logger.info(f"Verification requested for user {user.id}. Verification token: {token}")


def get_user_manager(user_db: MongoDBUserDatabase = Depends(get_user_db)):
    yield UserManager(user_db)


class RegisterForm:
    def __init__(self, request: Request):
        self.request: Request = request
        self.errors: List = []
        self.username: Optional[str] = None
        self.password: Optional[str] = None
        self.confirm_password: Optional[str] = None

    async def load_data(self):
        form = await self.request.form()
        self.username = form.get("username")
        self.password = form.get("password")
        self.confirm_password = form.get("confirmpassword")

    async def is_valid(self):
        if not self.username or not (self.username.__contains__("@")):
            self.errors.append("Email is required")
        if not self.password or not len(self.password) >= 4:
            self.errors.append("A valid password is required")
        if self.password != self.confirm_password:
            self.errors.append("Passwords are not same")
        if not self.errors:
            return True
        return False


class RedirectCookieAuthentication(CookieAuthentication):
    async def get_login_response(self, user: UserDB, response: Response):
        await super().get_login_response(user, response)
        response.status_code = status.HTTP_303_SEE_OTHER
        response.headers["Location"] = "/report_history"

    async def get_logout_response(self, user: UserDB, response: Response):
        await super().get_logout_response(user, response)
        response.status_code = status.HTTP_303_SEE_OTHER
        response.headers["Location"] = "/auth/signin"


cookie_authentication = RedirectCookieAuthentication(
    secret=SECRET,
    lifetime_seconds=3600,
    name="my-cookie",
)

fastapi_users = FastAPIUsers(
    get_user_manager,
    [cookie_authentication],
    User,
    UserCreate,
    UserUpdate,
    UserDB,
)

current_active_user = fastapi_users.current_user(active=True)
optional_current_active_verified_user = fastapi_users.current_user(active=True, optional=True)
