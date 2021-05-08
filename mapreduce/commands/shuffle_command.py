from mapreduce.commands import base_command


class ShuffleCommand(base_command.BaseCommand):

    def __init__(self):
        self._data = {}
        super().__init__(self._data)

    def set_source_file(self, src_file):
        encoded = src_file
        self._data['source_file'] = encoded

    def set_field_delimiter(self, field_delimiter):
        encoded = field_delimiter
        self._data['field_delimiter'] = encoded

    def validate(self):
        pass

    def send(self, **kwargs):
        self.validate()
        super().send('shuffle')

        return True
