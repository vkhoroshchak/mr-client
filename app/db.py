import os

import motor.motor_asyncio
from dotenv import load_dotenv
from fastapi_users.db import MongoDBUserDatabase

from app.models import CreateReportRecord, ReportRecord
from app.models import UserDB
from bson.objectid import ObjectId

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

    async def create_record(self, new_record: CreateReportRecord):
        new_record = await self.report_history_collection.insert_one(new_record.dict())
        created_record = await self.report_history_collection.find_one({"_id": new_record.inserted_id})

        return created_record

    async def get_record(self, record_id: str):
        record = await self.report_history_collection.find_one({"_id": ObjectId(record_id)})
        return record

    async def retrieve_records(self):
        records = []
        async for record in self.report_history_collection.find():
            records.append(ReportRecord(**record))
        return records
