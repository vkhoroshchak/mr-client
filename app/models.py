from datetime import datetime
from typing import Optional

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


class PyObjectId(ObjectId):

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError('Invalid objectid')
        return ObjectId(v)

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(type='string')


class CreateReportRecord(BaseModel):
    user_id: UUID4 = None
    file_name: str
    created_at: datetime = datetime.now()
    updated_at: datetime = datetime.now()


class ReportRecord(CreateReportRecord):
    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias='_id')

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
