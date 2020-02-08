from mapreduce.commands import base_command


# TODO: add validation
class RefreshTableCommand(base_command.BaseCommand):

    def __init__(self):
        self._data = {}
        super().__init__(self._data)

    def set_file_name(self, file_name):
        self._data['file_name'] = file_name

    def set_ip(self, ip):
        self._data['ip'] = ip

    def set_segment_name(self, segment_name):
        self._data['segment_name'] = segment_name

    def validate(self):
        if not self._data['file_name']:
            raise AttributeError('File name is not specified!')
        if not self._data['segment_name']:
            raise AttributeError('Segment name is not specified!')

    def send(self, **kwargs):
        self.validate()
        return super().send('refresh_table')
