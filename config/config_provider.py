import json
import os.path


class ConfigProvider:
    @staticmethod
    def get_data(file_path):
        with open(os.path.join(os.path.dirname(__file__), file_path)) as json_data:
            data = json.load(json_data)
            return data

    @staticmethod
    def get_arbiter_address(file_path):
        return ConfigProvider.get_data(file_path)['arbiter_address']

    @staticmethod
    def get_access_token(file_path):
        return ConfigProvider.get_data(file_path)['access_token']

    @staticmethod
    def get_map_key_delimiter(file_path):
        return ConfigProvider.get_data(file_path)['map_reduce']['map_key_delimiter']

    @staticmethod
    def get_field_delimiter(file_path):
        return ConfigProvider.get_data(file_path)['map_reduce']['field_delimiter']
