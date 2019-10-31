from mapreduce.commands import base_command


# TODO: add validation
class AppendCommand(base_command.BaseCommand):

    def __init__(self):
        self._data = {}

    def set_segment(self, segment):
        self._data['segment'] = segment

    def set_file_name(self, file_name):
        self._data["file_name"] = file_name

    def validate(self):
        if not self._data['segment']:
            raise AttributeError('Segment is not specified!')
        if not self._data['file_name']:
            raise AttributeError('Destination file is not specified!')

    def send(self):
        self.validate()
        data = {'append': self._data}
        super(AppendCommand, self).__init__(data)
        return super(AppendCommand, self).send()
