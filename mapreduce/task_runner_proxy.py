import os

from mapreduce import commands


def get_file_from_cluster(file_name, dest_file_name):
    return commands.GetFileFromClusterCommand(file_name, dest_file_name).send_command()


def create_config_and_filesystem(dest_file):
    return commands.CreateConfigAndFilesystem(dest_file).send_command()


# TODO: refactor
def main_func(file, row_limit, dest, keep_headers=True):
    file_name, ext = os.path.splitext(os.path.basename(file))
    dest_name, dest_ext = os.path.splitext(dest)
    output_name_template = dest_name + "_%s"
    output_name_template = output_name_template + ext
    with open(file, 'r', encoding='utf-8') as f:
        current_piece = 1
        headers = f.readline()
        current_limit = row_limit

        dict_item = {"file_name": None, "content": {"headers": None, "items": []}}

        for i, row in enumerate(f):

            if i + 1 > current_limit:
                current_piece += 1
                current_limit = row_limit * current_piece
                if keep_headers:
                    dict_item["content"]["headers"] = headers

                dict_item["file_name"] = output_name_template % (current_piece - 1)
                ip = append(dest)['data_node_ip']
                write(dict_item["file_name"], dict_item["content"], ip)
                refresh_table(dest, ip, dict_item["file_name"])
                dict_item = {"file_name": None, "content": {"headers": None, "items": []}}

            dict_item["content"]["items"].append(row)

        dict_item["file_name"] = output_name_template % current_piece

        if keep_headers:
            dict_item["content"]["headers"] = headers

        ip = append(dest)['data_node_ip']
        write(dict_item["file_name"], dict_item["content"], ip)
        refresh_table(dest, ip, output_name_template % current_piece)


def append(file_name):
    return commands.AppendCommand(file_name).send_command()


def write(file_name, segment, data_node_ip):
    return commands.WriteCommand(file_name, segment, data_node_ip).send_command()


def refresh_table(file_name, ip, segment_name):
    return commands.RefreshTableCommand(file_name, ip, segment_name).send_command()


def map(is_mapper_in_file, mapper, is_server_source_file, source_file, destination_file):
    mc = commands.MapCommand(is_mapper_in_file, mapper, is_server_source_file, source_file, destination_file)
    return mc.send_command()


def shuffle(source_file):
    return commands.ShuffleCommand(source_file).send_command()


def reduce(is_reducer_in_file, reducer, is_server_source_file, source_file, destination_file):
    rc = commands.ReduceCommand(is_reducer_in_file, reducer, is_server_source_file, source_file, destination_file)
    return rc.send_command()


def send_info():
    pass


def get_file(file_name, ip=None):
    return commands.GetFileCommand(file_name).send_command(ip=ip)


def clear_data(folder_name):
    folder_name, remove_all = folder_name.split(',')
    remove_all = bool(int(remove_all))
    return commands.ClearDataCommand(folder_name, remove_all).send_command()


def push_file_on_cluster(src_file):
    dest_file = os.path.basename(src_file)
    dist = create_config_and_filesystem(dest_file)

    main_func(src_file, dist['distribution'], dest_file)


def move_file_to_init_folder(file_name):
    return commands.MoveFileToInitFolderCommand(file_name).send_command()


def check_if_file_is_on_cluster(file_name):
    return commands.CheckIfFileIsOnCLuster(file_name).send_command()
