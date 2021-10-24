from typing import List

from fastapi import (
    APIRouter,
    File,
    UploadFile,
    Body,
)
from fastapi import Depends
from fastapi.responses import JSONResponse, FileResponse

import mapreduce.task_runner_proxy as task
from app.models import User
from app.users import current_active_user
from config.logger import client_logger

logger = client_logger.get_logger(__name__)

router = APIRouter()


@router.post("/run-map-reduce")
async def run_map_reduce(files: List[UploadFile] = File(...),
                         sql: str = Body(...),
                         user: User = Depends(current_active_user)):
    files_info = {}
    for file in files:
        logger.info(f"Pushing {file.filename} into a cluster")
        # TODO: To think about this logic
        # is_file_on_cluster = task.check_if_file_is_on_cluster(file.filename)['is_file_on_cluster']
        # logger.info(f"Is file on cluster: {is_file_on_cluster}")
        # if not is_file_on_cluster:
        #     task.push_file_on_cluster(file)
        # else:
        #     task.create_config_and_filesystem(file.filename)
        #     task.move_file_to_init_folder(file.filename)

        file_id = await task.push_file_on_cluster(file)
        files_info[file.filename] = file_id

    logger.info("File(s) uploaded, starting map_reduce phase")
    task.run_tasks(sql, files_info)
    return {"files_info": files_info}


@router.delete("/remove-file-from-cluster", response_description="The file was successfully removed from the cluster!")
async def remove_file_from_cluster(file_id: str, clear_all: bool, user: User = Depends(current_active_user)):
    task.clear_data(file_id, clear_all)
    return JSONResponse("The file was successfully removed from the cluster!")


@router.post("/push-file-on-cluster", response_description="The file was successfully uploaded to the cluster!")
async def push_file_on_cluster(file: UploadFile = File(...), user: User = Depends(current_active_user)):
    is_file_on_cluster_resp = await task.check_if_file_is_on_cluster(file)
    is_file_on_cluster = is_file_on_cluster_resp.get("is_file_on_cluster")
    file_id = is_file_on_cluster_resp.get("file_id")
    if is_file_on_cluster:
        logger.info("File already exists on the cluster! Not pushing again...")
    else:
        file_id = await task.push_file_on_cluster(file)
    return {"file_id": file_id}


# TODO: To think about the implementation
@router.get("/get-file-from-cluster")
async def get_file_from_cluster(file_id: str, user: User = Depends(current_active_user)):
    some_file_path = await task.get_file(file_id)

    return FileResponse(some_file_path, filename=some_file_path, media_type="text/csv")
