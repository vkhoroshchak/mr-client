import os

import requests
from aiohttp import ClientSession
from config.config_provider import ConfigProvider

config_provider = ConfigProvider(os.path.join('cluster_access.json'))

address = f"http://{config_provider.arbiter_address}"


async def send_request(session: ClientSession, data, command, ip=address, method: str = "POST"):
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    async with session.request(url=f"{ip}/command/{command}", headers=headers, json=data, method=method) as resp:
        return await resp.json(content_type=None)


def post(data, command, ip=address):
    url = f"{ip}/command/{command}"
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    response = requests.post(url, json=data, headers=headers)

    return response.json()
