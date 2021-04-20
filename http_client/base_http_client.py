import os

import requests

from config.config_provider import ConfigProvider

config_provider = ConfigProvider(os.path.join('json', 'cluster_access.json'))

address = f"http://{config_provider.arbiter_address}"


def post(data, command, ip=address):
    url = f"{ip}/command/{command}"
    response = requests.post(url, json=data)

    return response.json()
