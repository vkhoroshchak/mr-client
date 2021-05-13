import os
from itertools import groupby, count
from fastapi import UploadFile

import parsers.sql_parser as sql_parser
from config.config_provider import ConfigProvider
from config.logger import client_logger
from mapreduce import commands

logger = client_logger.get_logger(__name__)

config_provider = ConfigProvider(os.path.join('..', 'config', 'client_config.json'))


def get_file_from_cluster(file_name, dest_file_name):
    return commands.GetFileFromClusterCommand(file_name, dest_file_name).send_command()


def create_config_and_filesystem(file_name):
    return commands.CreateConfigAndFilesystem(file_name).send_command()


def append(file_id):
    return commands.AppendCommand(file_id).send_command()


def write(file_id, file_name, segment, data_node_ip):
    return commands.WriteCommand(file_id, file_name, segment, data_node_ip).send_command()


def refresh_table(file_id, ip, segment_name):
    return commands.RefreshTableCommand(file_id, ip, segment_name).send_command()


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


def get_file(file_name, ip=None):
    return commands.GetFileCommand(file_name).send_command(ip=ip)


def clear_data(file_id: str, clear_all: bool):
    return commands.ClearDataCommand(file_id, clear_all).send_command()


def push_file_on_cluster(uploaded_file: UploadFile):
    response = create_config_and_filesystem(uploaded_file.filename)

    row_limit = response.get("distribution")
    file_id = response.get("file_id")

    file_name, file_ext = os.path.splitext(uploaded_file.filename)
    output_name_template = f"{file_name}_%s{file_ext}"
    file_obj = uploaded_file.file._file
    headers = next(file_obj, None)

    if headers:
        headers = headers.decode("utf-8")

    groups = groupby(file_obj, key=lambda _, line=count(): next(line, None) // row_limit)

    for counter, group in groups:
        logger.info(f"Send chunk: {output_name_template % counter} to data node")
        push_file_chunk_on_cluster(file_id=file_id,
                                   chunk_name=output_name_template % counter,
                                   chunk={"headers": headers, "items": [i.decode("utf-8") for i in group]},
                                   file_name=uploaded_file.filename)
    return file_id


def push_file_chunk_on_cluster(file_id, chunk_name, chunk, file_name):
    ip = append(file_id).get("data_node_ip")
    logger.info(f"Sending chunk {chunk_name} to data node with ip: {ip}")
    write(file_id, chunk_name, chunk, ip)
    refresh_table(file_id, ip, chunk_name)


def move_file_to_init_folder(file_name):
    return commands.MoveFileToInitFolderCommand(file_name).send_command()


def check_if_file_is_on_cluster(file_name):
    return commands.CheckIfFileIsOnCLuster(file_name).send_command()


# TODO: Refactor
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
