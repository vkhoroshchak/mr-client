from mapreduce.commands import base_command


class CheckIfFileIsOnCLuster(base_command.BaseCommand):

    def __init__(self):
        self._data = {}
        super().__init__(self._data)

    def set_file_name(self, file_name):
        self._data['file_name'] = file_name

    def validate(self):
        if not self._data['file_name']:
            raise AttributeError('File name is not specified!')

    def send(self, **kwargs):
        self.validate()
        return super().send('check_if_file_is_on_cluster')
