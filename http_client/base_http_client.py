import os

import requests

from config import config_provider

address = "http://" + config_provider.ConfigProvider.get_arbiter_address(os.path.join('json', 'cluster_access.json'))
access_token = config_provider.ConfigProvider.get_access_token(os.path.join('json', 'cluster_access.json'))


def post(data, command, ip=address, ):
    url = f"{ip}/command/{command}"
    response = requests.post(url, json=data)
    response.raise_for_status()

    return response.json()
