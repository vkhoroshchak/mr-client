from typing import List

# from fastapi.responses import FileResponse
from fastapi import (
    FastAPI,
    File,
    UploadFile,
    Body,
)
from fastapi.responses import JSONResponse

import mapreduce.task_runner_proxy as task
from config.logger import client_logger

logger = client_logger.get_logger(__name__)

app = FastAPI()


@app.post("/run-map-reduce")
async def run_map_reduce(files: List[UploadFile] = File(...), sql: str = Body(...)):
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
        file_id = task.push_file_on_cluster(file)
        files_info[file.filename] = file_id

    logger.info("File(s) uploaded, starting map_reduce phase")
    task.run_tasks(sql, files_info)
    return JSONResponse("Map reduce request has been successful!")


@app.delete("/remove-file-from-cluster", response_description="The file was successfully removed from the cluster!")
async def remove_file_from_cluster(file_id: str, clear_all: bool):
    task.clear_data(file_id, clear_all)
    return JSONResponse("The file was successfully removed from the cluster!")


@app.post("/push-file-on-cluster", response_description="The file was successfully uploaded to the cluster!")
async def push_file_on_cluster(file: UploadFile = File(...)):
    file_id = await task.push_file_on_cluster(file)
    return {"file_id": file_id}


# TODO: To think about the implementation
@app.post("/get-file-from-cluster")
async def get_file_from_cluster(file_name: str):
    # get_file
    pass
    # some_file_path = ""

    # return FileResponse(some_file_path, filename=file_name, media_type="text/csv")
