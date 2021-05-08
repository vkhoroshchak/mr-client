from mapreduce.commands import base_command


class CreateConfigAndFilesystem(base_command.BaseCommand):

    def __init__(self):
        self._data = {}
        super().__init__(self._data)

    def set_destination_file(self, destination_file):
        self._data['file_name'] = destination_file

    def validate(self):
        if not self._data['file_name']:
            raise AttributeError('File_name is not specified!')

    def send(self, **kwargs):
        self.validate()
        return super().send('create_config_and_filesystem')
