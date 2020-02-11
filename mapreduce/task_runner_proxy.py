import os

from config import config_provider
from filesystem import service
from mapreduce.commands import (
    append_command,
    clear_data_command,
    get_file_command,
    get_result_of_key_command,
    create_config_and_filesystem,
    map_command,
    shuffle_command,
    reduce_command,
    refresh_table_command,
    write_command,
    move_file_to_init_folder_command,
    check_if_file_is_on_cluster_command
)
import json
import moz_sql_parser as msp
import re

field_delimiter = config_provider.ConfigProvider.get_field_delimiter(
            os.path.join('..', 'config', 'json', 'client_config.json'))
# TODO: refactor
class TaskRunner:

    @staticmethod
    def create_config_and_filesystem(dest_file):
        mfc = create_config_and_filesystem.CreateConfigAndFilesystem()
        mfc.set_destination_file(dest_file)
        return mfc.send()

    @staticmethod
    def main_func(file, row_limit, dest, delimiter=',', keep_headers=True):
        file_name, ext = os.path.splitext(os.path.basename(file))
        output_name_template = dest + "_%s"
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
    def map(is_mapper_in_file, mapper, is_server_source_file, source_file, destination_file,
            parsed_select):
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
        mc.set_parsed_select(parsed_select)

        return mc.send()

    @staticmethod
    def shuffle(source_file, parsed_group_by):
        sc = shuffle_command.ShuffleCommand()
        sc.set_source_file(source_file)
        sc.set_parsed_group_by(parsed_group_by)
        sc.set_field_delimiter(field_delimiter)
        return sc.send()

    @staticmethod
    def reduce(is_reducer_in_file, reducer, is_server_source_file, source_file, destination_file,
               **kwargs):
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
        rc.set_parsed_sql(kwargs)
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
        clear_data.set_folder_name(folder_name_arr[0])
        clear_data.set_remove_all_data(bool(int(folder_name_arr[1])))

        return clear_data.send()

    @staticmethod
    def push_file_on_cluster(src_file, dest_file):
        dest_file = os.path.basename(src_file)
        dist = TaskRunner.create_config_and_filesystem(dest_file)
        TaskRunner.main_func(src_file, dist['distribution'], dest_file)

    @staticmethod
    def group_by_parser(data):
        select_data = data['groupby']
        res = []
        if type(select_data) is list:
            for item in select_data:
                item_dict = {}

                if 'literal' in item['value'].keys():
                    item_dict['key_name'] = item['value']['literal']
                else:
                    item_dict['key_name'] = item['value']

                res.append(item_dict)
        else:
            item_dict = {}
            if 'literal' in select_data['value'].keys():
                item_dict['key_name'] = select_data['value']['literal']
            else:
                item_dict['key_name'] = select_data['value']

            res.append(item_dict)
        return res

    @staticmethod
    def process_dict_item(diction):
        item_dict = {}
        if type(diction['value']) is not dict:
            item_dict['old_name'] = diction['value']
            if 'name' in diction.keys():
                item_dict['new_name'] = diction['name']
            else:
                item_dict['new_name'] = diction['value']
        elif 'literal' in diction['value'].keys():
            item_dict['old_name'] = diction['value']['literal']
            if 'name' in diction.keys():
                item_dict['new_name'] = diction['name']
            else:
                item_dict['new_name'] = diction['value']['literal']
        elif 'sum' in diction['value'].keys():
            item_dict = TaskRunner.parse_aggregation_value('sum', diction)

        elif 'min' in diction['value'].keys():
            item_dict = TaskRunner.parse_aggregation_value('min', diction)
        elif 'max' in diction['value'].keys():
            item_dict = TaskRunner.parse_aggregation_value('max', diction)
        elif 'avg' in diction['value'].keys():
            item_dict = TaskRunner.parse_aggregation_value('avg', diction)
        elif 'count' in diction['value'].keys():
            item_dict = TaskRunner.parse_aggregation_value('count', diction)

        return item_dict

    @staticmethod
    def select_parser(data):
        select_data = data['select']
        res = []
        item_dict = {}
        if select_data == '*':
            item_dict['old_name'] = select_data
            item_dict['new_name'] = select_data
            res.append(item_dict)
        else:
            if type(select_data) is list:
                for i in select_data:
                    res.append(TaskRunner.process_dict_item(i))
            else:
                res.append(TaskRunner.process_dict_item(select_data))
        return res

    @staticmethod
    def from_parser(data):
        res = {}
        if type(data['from']) is not dict:
            res['file_name'] = data['from']
        return res

    @staticmethod
    def parse_aggregation_value(name, data):
        res = {'old_name': data['value'][name]}
        if 'name' in data.keys():
            res['new_name'] = f"{data['name']}"
        else:
            res['new_name'] = f"{name.upper()}_{data['value'][name]}"
        res['aggregate_f_name'] = name
        return res

    @staticmethod
    def move_file_to_init_folder():
        mftifc = move_file_to_init_folder_command.MoveFileToInitFolderCommand()
        mftifc.send()

    @staticmethod
    def prepare_for_sql_query(dest_file):
        # print("create_config_and_filesystem".upper())
        TaskRunner.create_config_and_filesystem(dest_file)
        # print("create_config_and_filesystem finished".upper())
        # TaskRunner.push_file_on_cluster(dest_file, dest_file)
        print("move_file_to_init_folder".upper())
        TaskRunner.move_file_to_init_folder()
        print("move_file_to_init_folder finished".upper())

    @staticmethod
    def check_if_file_is_on_cluster(file_name):
        cifioc = check_if_file_is_on_cluster_command.CheckIfFileIsOnCLuster()
        cifioc.set_file_name(file_name)
        print("CHECKING FILE NAME " + file_name)
        return cifioc.send()


    @staticmethod
    def run_sql_command(is_mapper_in_file, mapper, is_reducer_in_file, reducer, sql_command, is_server_source_file):
        # initial_sql_command = sql_command
        pattern = re.compile(r"(?<=FROM )(.+? )", flags=re.IGNORECASE)
        print("SQL COMMAND")
        print(sql_command)
        search = re.search(pattern, sql_command)
        src_file = search.group(1)
        print("SOURCE FILE")
        print(src_file)
        file_name = os.path.basename(src_file)
        sql_command = re.sub(pattern, file_name, sql_command)
        src_file = src_file.strip()
        file_name = file_name.strip()
        print("UPDATED SQL COMMAND")
        print(sql_command)
        parsed_sql = json.dumps(msp.parse(sql_command))
        json_res = json.loads(parsed_sql)
        parsed_select = TaskRunner.select_parser(json_res)
        parsed_group_by = TaskRunner.group_by_parser(json_res)
        print(json_res)
        print("STARTED TO CHECK IF FILE IS ON CLUSTER")
        is_file_on_cluster = TaskRunner.check_if_file_is_on_cluster(file_name)['is_file_on_cluster']
        print(is_file_on_cluster)
        print("FINISHED TO CHECK IF FILE IS ON CLUSTER")
        if not is_file_on_cluster:
            print("PUSHING FILE ON CLUSTER")
            TaskRunner.push_file_on_cluster(src_file, file_name)
        # src_file = TaskRunner.from_parser(json_res)['file_name']

        # Never enters if statement
        # if not is_server_source_file:
        #     dest_file = os.path.dirname(src_file)
        #     TaskRunner.push_file_on_cluster(src_file, dest_file)
        # else:

        dest_file = os.path.basename(src_file)
        TaskRunner.prepare_for_sql_query(dest_file)

        TaskRunner.shuffle(src_file, parsed_group_by[0])
        TaskRunner.reduce(is_reducer_in_file, reducer, is_server_source_file, src_file, dest_file,
                          parsed_select=parsed_select, parsed_group_by=parsed_group_by)
        TaskRunner.map(is_mapper_in_file, mapper, is_server_source_file, src_file, dest_file, parsed_select)
