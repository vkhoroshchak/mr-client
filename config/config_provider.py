import json
import os


class ConfigProvider:
    def __init__(self, file_path):
        with open(os.path.join(os.path.dirname(__file__), file_path)) as json_data:
            data = json.load(json_data)
            self.config: dict = data

    @property
    def arbiter_address(self):
        return self.config.get('arbiter_address')

    @property
    def access_token(self):
        return os.environ.get("ACCESS_TOKEN")

    @property
    def map_key_delimiter(self):
        return self.config.get('map_reduce')['map_key_delimiter']

    @property
    def field_delimiter(self):
        return self.config.get('map_reduce')['field_delimiter']
