import os
import glob

from config import config_provider
from filesystem import service
from mapreduce.commands import (
    append_command,
    clear_data_command,
    get_file_command,
    get_result_of_key_command,
    make_file_command,
    map_command,
    shuffle_command,
    reduce_command,
    refresh_table_command,
    write_command,
)


# TODO: refactor
class TaskRunner:

    @staticmethod
    def make_file(dest_file):
        mfc = make_file_command.MakeFileCommand()
        mfc.set_destination_file(dest_file)
        return mfc.send()

    @staticmethod
    def main_func(file, distribution, dest):
        file_name = file.split(os.sep)[-1]
        file_name, ext = file_name.split(os.extsep)
        output_path = 'temp_data'
        output_name_template = dest + "_%s."

        service.split_file(file, row_limit=distribution, output_name_template=output_name_template + ext,
                           output_path=output_path)
        files = [os.path.join(r, file) for r, d, f in os.walk(output_path) for file in f]

        for i in files:
            ip = TaskRunner.append(dest)['data_node_ip']
            with open(i, 'r', encoding='utf-8') as f:
                TaskRunner.write(i, f.read(), ip)
            TaskRunner.refresh_table(dest, ip, i)

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
    def map(is_mapper_in_file, mapper, key_delimiter, is_server_source_file, source_file, destination_file, sql_query):
        mc = map_command.MapCommand()
        if is_mapper_in_file is False:
            mc.set_mapper(mapper)
        else:
            mc.set_mapper_from_file(mapper)

        if is_server_source_file is True:
            mc.set_server_source_file(source_file)
        else:
            mc.set_source_file(source_file)

        mc.set_key_delimiter(key_delimiter)
        field_delimiter = config_provider.ConfigProvider.get_field_delimiter(
            os.path.join('..', 'config', 'json', 'client_config.json'))

        mc.set_field_delimiter(field_delimiter)
        mc.set_destination_file(destination_file)
        mc.set_sql_query(sql_query)

        return mc.send()

    @staticmethod
    def shuffle(source_file, sql_query):
        sc = shuffle_command.ShuffleCommand()
        sc.set_source_file(source_file)
        sc.set_sql_query(sql_query)
        return sc.send()

    @staticmethod
    def reduce(is_reducer_in_file, reducer, key_delimiter, is_server_source_file, source_file, destination_file,
               sql_query):
        rc = reduce_command.ReduceCommand()

        if is_reducer_in_file is False:
            rc.set_reducer(reducer)
        else:
            rc.set_reducer_from_file(reducer)

        if is_server_source_file is True:
            rc.set_server_source_file(source_file)
        else:
            rc.set_source_file(source_file)

        rc.set_key_delimiter(key_delimiter)

        rc.set_destination_file(destination_file)
        rc.set_sql_query(sql_query)
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
            return get_file.send(ip)

    @staticmethod
    def clear_data(folder_name):
        clear_data = clear_data_command.ClearDataCommand()
        folder_name_arr = folder_name.split(',')
        clear_data.set_folder_name(folder_name_arr[0])
        clear_data.set_remove_all_data(bool(int(folder_name_arr[1])))

        return clear_data.send()

    @staticmethod
    def run_map_reduce(is_mapper_in_file, mapper, is_reducer_in_file, reducer, key_delimiter, is_server_source_file,
                       source_file, destination_file, sql_query):
        if not is_server_source_file:
            print("MAKE_FILE_ON_CLUSTER_FINISHED")
            distribution = TaskRunner.make_file(os.path.join(destination_file))['distribution']
            print("MAKING_FILE_ON_CLUSTER_FINISHED")
            print("APPEND_AND_WRITE_PHASE")
            print(distribution)
            TaskRunner.main_func(source_file, distribution, destination_file)
            print("APPEND_AND_WRITE_PHASE_FINISHED")

        print("SHUFFLE_STARTED")
        TaskRunner.shuffle(destination_file,sql_query)
        print("SHUFFLE_FINISHED")

        print("REDUCE_STARTED")
        TaskRunner.reduce(is_reducer_in_file, reducer, key_delimiter, is_server_source_file, source_file,
                          destination_file, sql_query)
        print("REDUCE_FINISHED")
        print("MAP_STARTED")
        TaskRunner.map(is_mapper_in_file, mapper, key_delimiter, is_server_source_file,
                       source_file, destination_file, sql_query)

        print("MAP_FINISHED")
        print("COMPLETED!")

    @staticmethod
    def push_file_on_cluster(pfc):
        arr = pfc.split(",")
        dist = TaskRunner.make_file(arr[1])
        TaskRunner.main_func(arr[0], dist['distribution'], arr[1])

    @staticmethod
    def get_result_of_key(key, file_name):
        field_delimiter = config_provider.ConfigProvider.get_field_delimiter(
            os.path.join('..', 'config', 'json', 'client_config.json'))

        grk = get_result_of_key_command.GetResultOfKeyCommand()
        grk.set_key(key)
        grk.set_file_name(file_name)
        grk.set_field_delimiter(field_delimiter)
        json_response = grk.send()
        key_hash = json_response['hash_key']['key_hash']
        print(json_response)
        for item in json_response['key_ranges']:
            if item['hash_keys_range'][0] <= key_hash < item['hash_keys_range'][1]:
                data_node_ip = item['data_node_ip']
                break
            elif item['hash_keys_range'][0] < key_hash <= item['hash_keys_range'][1]:
                data_node_ip = item['data_node_ip']
                break
        result = grk.send('http://' + data_node_ip)
        service.write_to_file(result['result'], file_name)
