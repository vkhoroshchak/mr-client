from http_client import base_http_client


# TODO: add validation
class BaseCommand(object):

    def __init__(self, data):
        self._data = data

    def validate(self):
        pass

    def send(self, ip=None):
        if not ip:
            return base_http_client.post(self._data)
        else:
            return base_http_client.post(self._data, ip)