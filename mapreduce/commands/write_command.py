from mapreduce.commands import base_command


# TODO: add validation
class WriteCommand(base_command.BaseCommand):

    def __init__(self):
        self._data = {}
        super().__init__(self._data)

    def set_segment(self, segment):
        self._data['segment'] = segment

    def set_file_name(self, file_name):
        self._data["file_name"] = file_name

    def set_data_node_ip(self, data_node_ip):
        self._data['data_node_ip'] = data_node_ip

    def validate(self):
        if not self._data['segment']:
            raise AttributeError('Segment is not specified!')
        if not self._data['file_name']:
            raise AttributeError('File name is not specified!')
        if not self._data['data_node_ip']:
            raise AttributeError('Data node ip is not specified!')

    def send(self, **kwargs):
        self.validate()
        return super().send('write', self._data['data_node_ip'])
