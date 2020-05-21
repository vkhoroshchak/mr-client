import os

from config import config_provider
from mapreduce.commands import (
    append_command,
    clear_data_command,
    get_file_command,
    create_config_and_filesystem,
    map_command,
    shuffle_command,
    reduce_command,
    refresh_table_command,
    write_command,
    move_file_to_init_folder_command,
    check_if_file_is_on_cluster_command
)

field_delimiter = config_provider.ConfigProvider.get_field_delimiter(
    os.path.join('..', 'config', 'json', 'client_config.json'))


class TaskRunner:

    @staticmethod
    def create_config_and_filesystem(dest_file):
        mfc = create_config_and_filesystem.CreateConfigAndFilesystem()
        mfc.set_destination_file(dest_file)
        return mfc.send()

    @staticmethod
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
                    ip = TaskRunner.append(dest)['data_node_ip']
                    TaskRunner.write(dict_item["file_name"], dict_item["content"], ip)
                    TaskRunner.refresh_table(dest, ip, dict_item["file_name"])
                    dict_item = {"file_name": None, "content": {"headers": None, "items": []}}

                dict_item["content"]["items"].append(row)

            dict_item["file_name"] = output_name_template % current_piece

            if keep_headers:
                dict_item["content"]["headers"] = headers

            ip = TaskRunner.append(dest)['data_node_ip']
            TaskRunner.write(dict_item["file_name"], dict_item["content"], ip)
            TaskRunner.refresh_table(dest, ip, output_name_template % current_piece)

    @staticmethod
    def append(file_name):
        app = append_command.AppendCommand()
        app.set_file_name(file_name)
        return app.send()

    @staticmethod
    def write(file_name, segment, data_node_ip):
        wc = write_command.WriteCommand()

        wc.set_segment(segment)
        wc.set_file_name(file_name)
        wc.set_data_node_ip(data_node_ip)

        return wc.send()

    @staticmethod
    def refresh_table(file_name, ip, segment_name):
        rtc = refresh_table_command.RefreshTableCommand()
        rtc.set_file_name(file_name)
        rtc.set_ip(ip)
        rtc.set_segment_name(segment_name)

        return rtc.send()

    @staticmethod
    def map(is_mapper_in_file, mapper, is_server_source_file, source_file, destination_file):
        mc = map_command.MapCommand()
        if is_mapper_in_file is False:
            mc.set_mapper(mapper)
        else:
            mc.set_mapper_from_file(mapper)

        if is_server_source_file is True:
            mc.set_server_source_file(source_file)
        else:
            mc.set_source_file(source_file)

        mc.set_field_delimiter(field_delimiter)
        mc.set_destination_file(destination_file)

        return mc.send()

    @staticmethod
    def shuffle(source_file, key):
        sc = shuffle_command.ShuffleCommand()
        sc.set_source_file(source_file)
        sc.set_field_delimiter(field_delimiter)
        sc.set_key(key)
        return sc.send()

    @staticmethod
    def reduce(is_reducer_in_file, reducer, is_server_source_file, source_file, destination_file):
        rc = reduce_command.ReduceCommand()

        if is_reducer_in_file is False:
            rc.set_reducer(reducer)
        else:
            rc.set_reducer_from_file(reducer)

        if is_server_source_file is True:
            rc.set_server_source_file(source_file)
        else:
            rc.set_source_file(source_file)

        rc.set_field_delimiter(field_delimiter)
        print("DESTDESTDEST")
        print(destination_file)
        print("DESTDESTDEST")
        rc.set_destination_file(destination_file)
        return rc.send()

    @staticmethod
    def send_info():
        pass

    @staticmethod
    def get_file(file_name, ip=None):
        get_file = get_file_command.GetFileCommand()
        get_file.set_file_name(file_name)
        if not ip:
            return get_file.send()
        else:
            return get_file.send(ip, )

    @staticmethod
    def clear_data(folder_name):
        clear_data = clear_data_command.ClearDataCommand()
        folder_name_arr = folder_name.split(',')
        print(folder_name_arr)
        clear_data.set_folder_name(folder_name_arr[0])
        clear_data.set_remove_all_data(bool(int(folder_name_arr[1])))

        return clear_data.send()

    @staticmethod
    def push_file_on_cluster(src_file, dest_file):
        dest_file = os.path.basename(src_file)
        dist = TaskRunner.create_config_and_filesystem(dest_file)
        TaskRunner.main_func(src_file, dist['distribution'], dest_file)

    @staticmethod
    def move_file_to_init_folder(file_name):
        mftifc = move_file_to_init_folder_command.MoveFileToInitFolderCommand(file_name)
        mftifc.send()

    @staticmethod
    def check_if_file_is_on_cluster(file_name):
        cifioc = check_if_file_is_on_cluster_command.CheckIfFileIsOnCLuster()
        cifioc.set_file_name(file_name)
        print("CHECKING FILE NAME " + file_name)
        return cifioc.send()
