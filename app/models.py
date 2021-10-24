from datetime import datetime
from typing import List

from bson.objectid import ObjectId
from fastapi_users import models
from pydantic import BaseModel, UUID4, Field


class User(models.BaseUser):
    pass


class UserCreate(models.BaseUserCreate):
    pass


class UserUpdate(models.BaseUserUpdate):
    pass


class UserDB(User, models.BaseUserDB):
    pass


class PydanticObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not isinstance(v, ObjectId):
            raise TypeError('ObjectId required')
        return str(v)


class CreateReportRecord(BaseModel):
    user_id: UUID4 = None
    file_name: str
    created_at: datetime = datetime.now()
    updated_at: datetime = datetime.now()


class ReportRecord(CreateReportRecord):
    id: PydanticObjectId = Field(..., alias="_id")


class ReportHistory(BaseModel):
    records: List[ReportRecord] = []
