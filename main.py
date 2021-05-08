from typing import List

# from fastapi.responses import StreamingResponse
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
    for file in files:
        logger.info(f"Pushing {file.filename} into a cluster")
        is_file_on_cluster = task.check_if_file_is_on_cluster(file.filename)['is_file_on_cluster']
        logger.info(f"Is file on cluster: {is_file_on_cluster}")
        if not is_file_on_cluster:
            task.push_file_on_cluster(file)
        else:
            task.create_config_and_filesystem(file.filename)
            task.move_file_to_init_folder(file.filename)

    logger.info("File(s) uploaded, starting map_reduce phase")
    task.run_tasks(sql)



@app.delete("/remove-file-from-cluster", response_description="The file was successfully removed from the cluster!")
async def remove_file_from_cluster(file_name: str, clear_all: bool):
    task.clear_data(file_name, clear_all)
    return JSONResponse("The file was successfully removed from the cluster!")


@app.post("/push-file-on-cluster", response_description="The file was successfully uploaded to the cluster!")
async def push_file_on_cluster(file: UploadFile = File(...)):
    task.push_file_on_cluster(file)
    return JSONResponse("The file was successfully uploaded to the cluster!")


# TODO: To think about the implementation
@app.post("/get-file-from-cluster")
async def get_file_from_cluster(file_id):
    pass
    # get_file
    # some_file_path = ""
    # file_like = open(some_file_path, mode="rb")
    # return StreamingResponse(file_like, media_type="video/mp4")
