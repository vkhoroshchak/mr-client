from mapreduce.commands import base_command


class GetResultOfKeyCommand(base_command.BaseCommand):

    def __init__(self):
        self._data = {}
        super().__init__(self._data)

    def set_key(self, key):
        self._data['key'] = key

    def set_file_name(self, file_name):
        self._data['file_name'] = file_name

    def set_field_delimiter(self, field_delimiter):
        self._data['field_delimiter'] = field_delimiter

    def validate(self):
        if not self._data['key']:
            raise AttributeError('Key is not specified!')
        if not self._data['file_name']:
            raise AttributeError('File name is not specified!')
        if not self._data['field_delimiter']:
            raise AttributeError('Field delimiter is not specified!')

    def send(self, ip=None, **kwargs):
        self.validate()
        if not ip:
            return super().send()
        else:
            return super().send(ip)
