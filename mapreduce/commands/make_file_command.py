from mapreduce.commands import base_command


# TODO: add validation
class MakeFileCommand(base_command.BaseCommand):

    def __init__(self):
        self._data = {}

    def set_destination_file(self, destination_file):
        self._data['file_name'] = destination_file

    def validate(self):
        if not self._data['file_name']:
            raise AttributeError('File_name is not specified!')

    def send(self):
        self.validate()
        super(MakeFileCommand, self).__init__(self._data)
        return super(MakeFileCommand, self).send('make_file')
