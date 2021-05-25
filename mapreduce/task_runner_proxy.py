import asyncio
import io
import json
import math
import os
import sys
import uuid
from itertools import cycle

import pandas as pd
import psutil
from aiohttp import ClientSession
from fastapi import UploadFile

import parsers.sql_parser as sql_parser
from config.config_provider import ConfigProvider
from config.logger import client_logger
from mapreduce import commands

logger = client_logger.get_logger(__name__)

config_provider = ConfigProvider(os.path.join('..', 'config', 'client_config.json'))


def get_file_from_cluster(file_name, dest_file_name):
    return commands.GetFileFromClusterCommand(file_name, dest_file_name).send_command()


async def create_config_and_filesystem(session, file_name):
    return await commands.CreateConfigAndFilesystem(session, file_name).send_command_async()


async def get_data_nodes_list(session):
    return await commands.GetDataNodesListCommand(session).send_command_async()


async def write(session, file_id, file_name, segment, data_node_ip):
    return await commands.WriteCommand(session, file_id, file_name, segment, data_node_ip).send_command_async()


async def refresh_table(session, file_id, ip, segment_name):
    return await commands.RefreshTableCommand(session, file_id, ip, segment_name).send_command_async()


def start_map_phase(is_mapper_in_file, mapper, file_id):
    mc = commands.MapCommand(is_mapper_in_file, mapper, file_id)
    return mc.send_command()


def start_shuffle_phase(file_id):
    return commands.ShuffleCommand(file_id).send_command()


def start_reduce_phase(is_reducer_in_file, reducer, file_id, source_file):
    rc = commands.ReduceCommand(is_reducer_in_file, reducer, file_id, source_file)
    return rc.send_command()


def send_info():
    pass


async def get_file(file_id, ip=None):
    async with ClientSession() as session:
        data_nodes = await get_data_nodes_list(session)
        file_name = await commands.GetFileNameCommand(file_id, session).send_command(ip=ip)

        mode = "w"
        header = True

        for data_node_ip in data_nodes:
            async with session.request(url=f"http://{data_node_ip}/command/get_file",  # noqa
                                       json={'file_id': file_id,
                                             'file_name': file_name},
                                       method="POST") as resp:
                res = await resp.read()

                df = pd.read_csv(io.StringIO(res.decode('utf-8')))

                df.to_csv(file_name, header=header, mode=mode, index=False)
                header = False
                mode = "a"

    return file_name


def clear_data(file_id: str, clear_all: bool):
    return commands.ClearDataCommand(file_id, clear_all).send_command()


def get_file_len(file):
    i = 0
    for i, _ in enumerate(file):
        pass

    file.seek(0)
    return i


def get_num_of_workers(file_obj, chunk_size):
    chunk_size = sys.getsizeof(json.dumps(next(read_file_by_chunks(file_obj, chunk_size))))
    free_ram = psutil.virtual_memory().free
    logger.info(f"Free RAM in MB: {free_ram / 1000000}")
    logger.info(f"Chunk size in MB: {chunk_size / 1000000}")

    num_of_workers = math.floor(free_ram / chunk_size * 0.2)
    file_obj.seek(0)
    logger.info(f"Num of workers: {num_of_workers}")
    return num_of_workers


def read_file_by_chunks(file_obj, chunk_size: int):
    chunk = []
    counter = 0
    for piece in file_obj:
        if counter == chunk_size:
            yield chunk
            chunk = []
            counter = 0
        counter += 1
        chunk.append(piece.decode("utf-8"))
    yield chunk


async def push_file_on_cluster(uploaded_file: UploadFile):
    async with ClientSession() as session:
        response = await create_config_and_filesystem(session, uploaded_file.filename)
        logger.info(f"Got a response from create_config_and_filesystem: {response}")
        data_nodes_list = await get_data_nodes_list(session)
        logger.info(f"Received data nodes list: {data_nodes_list}")
        data_nodes_list = cycle(data_nodes_list)
        row_limit = response.get("distribution")
        file_id = response.get("file_id")

        file_name, file_ext = os.path.splitext(uploaded_file.filename)
        logger.info("getting file content")

        file_obj = uploaded_file.file._file  # noqa
        num_of_workers = get_num_of_workers(file_obj, row_limit)
        file_len = get_file_len(file_obj)
        headers = next(file_obj, None)

        if headers:
            headers = headers.decode("utf-8")

        mySemaphore = asyncio.Semaphore(num_of_workers)

        async def push_chunk_on_cluster(ip):

            await mySemaphore.acquire()
            logger.info("Acquired")
            ip = f"http://{ip}"  # noqa
            chunk = next(read_file_by_chunks(file_obj, row_limit), None)

            if chunk:
                async with ClientSession() as new_session:
                    chunk_name = f"{uuid.uuid4()}{file_ext}"
                    logger.info("before write")
                    await write(new_session,
                                file_id,
                                chunk_name,
                                {"headers": headers, "items": json.dumps(chunk)},
                                ip)
                    logger.info("after write")
                    logger.info("before refresh table")
                    await refresh_table(new_session, file_id, ip, chunk_name)
                    logger.info("after refresh table")
            mySemaphore.release()
            logger.info("Released")

        tasks = []
        for i in range((file_len // row_limit) + 1):
            logger.info(f"group: {i}")
            tasks.append(asyncio.ensure_future(push_chunk_on_cluster(next(data_nodes_list, None))))

        await asyncio.gather(*tasks)

    return file_id


def move_file_to_init_folder(file_name):
    return commands.MoveFileToInitFolderCommand(file_name).send_command()


def check_if_file_is_on_cluster(file_name):
    return commands.CheckIfFileIsOnCLuster(file_name).send_command()


def run_tasks(sql, files_info):
    parsed_sql = sql if type(sql) is dict else sql_parser.SQLParser.sql_parser(sql)
    field_delimiter = config_provider.field_delimiter
    from_file = parsed_sql['from']
    if type(from_file) is dict:
        from_file = run_tasks(from_file, files_info)
    if type(from_file) is tuple:
        reducer = sql_parser.custom_reducer(parsed_sql, field_delimiter)

        for file_name in from_file:
            own_select = sql_parser.SQLParser.split_select_cols(file_name, parsed_sql['select'])
            key_col = sql_parser.SQLParser.get_key_col(parsed_sql, file_name)

            mapper = sql_parser.custom_mapper(key_col, own_select, field_delimiter)
            file_id = files_info[file_name]
            start_map_phase(
                is_mapper_in_file=False,
                mapper=mapper,
                file_id=file_id,
            )
            start_shuffle_phase(file_id=file_id)

        file_name = from_file[0]

        start_reduce_phase(
            is_reducer_in_file=False,
            reducer=reducer,
            file_id=files_info[file_name],
            source_file=list(from_file)
        )
        return file_name

    else:

        key_col = sql_parser.SQLParser.get_key_col(parsed_sql, from_file)
        reducer = sql_parser.custom_reducer(parsed_sql, field_delimiter)
        mapper = sql_parser.custom_mapper(key_col, parsed_sql['select'], field_delimiter)

        if type(from_file) is tuple:
            from_file = from_file[0]

        file_id = files_info[from_file]
        start_map_phase(
            is_mapper_in_file=False,
            mapper=mapper,
            file_id=file_id,
        )
        start_shuffle_phase(file_id=file_id)

        start_reduce_phase(
            is_reducer_in_file=False,
            reducer=reducer,
            file_id=file_id,
            source_file=from_file
        )
        return from_file
