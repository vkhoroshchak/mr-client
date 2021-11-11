import os

import motor.motor_asyncio
from bson.objectid import ObjectId
from dotenv import load_dotenv
from fastapi import status
from fastapi.exceptions import HTTPException
from fastapi_users.db import MongoDBUserDatabase

from app.models import CreateReportRecord, ReportRecord
from app.models import UserDB, User

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

client = motor.motor_asyncio.AsyncIOMotorClient(
    DATABASE_URL, uuidRepresentation="standard"
)
db = client[os.getenv("MONGO_DB")]
collection = db["users"]


def get_user_db():
    yield MongoDBUserDatabase(UserDB, collection)


class ReportHistoryManager:
    report_history_collection = db["report_history"]

    def __init__(self, user: User):
        self.user = user

    async def create_record(self, new_record: CreateReportRecord):
        new_record.user_id = self.user.id
        new_record = await self.report_history_collection.insert_one(new_record.dict())
        created_record = await self.get_record(new_record.inserted_id)
        return created_record

    async def get_record(self, record_id: str):
        record = await self.report_history_collection.find_one({"_id": ObjectId(record_id)})
        record = ReportRecord(**record)

        if record.user_id != self.user.id and not self.user.is_superuser:
            raise HTTPException(
                detail="Not enough permissions",
                status_code=status.HTTP_403_FORBIDDEN,
            )
        return record

    async def retrieve_records(self):
        records = []
        async for record in self.report_history_collection.find({"user_id": self.user.id}):
            records.append(record)
        return records
