import json
import os.path


class ConfigProvider:

    @staticmethod
    def get_arbiter_address(file_path):
        json_data = open(os.path.join(os.path.dirname(__file__), file_path))
        data = json.load(json_data)
        json_data.close()
        return data["arbiter_address"]

    @staticmethod
    def get_access_token(file_path):
        json_data = open(os.path.join(os.path.dirname(__file__), file_path))
        data = json.load(json_data)
        json_data.close()
        return data["access_token"]

    @staticmethod
    def get_map_key_delimiter(file_path):
        json_data = open(os.path.join(os.path.dirname(__file__), file_path))
        data = json.load(json_data)
        json_data.close()
        return data['map_reduce']['map_key_delimiter']

    @staticmethod
    def get_field_delimiter(file_path):
        json_data = open(os.path.join(os.path.dirname(__file__), file_path))
        data = json.load(json_data)
        json_data.close()
        return data['map_reduce']['field_delimiter']
