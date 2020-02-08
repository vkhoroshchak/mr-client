from mapreduce.commands import base_command


class ShuffleCommand(base_command.BaseCommand):

    def __init__(self):
        self._data = {}
        super().__init__(self._data)

    def set_source_file(self, src_file):
        encoded = src_file
        self._data['source_file'] = encoded

    def validate(self):
        pass

    def set_parsed_group_by(self, parsed_group_by):
        encoded = parsed_group_by
        self._data['parsed_group_by'] = encoded

    def send(self, **kwargs):
        self.validate()
        super().send('shuffle')

        return True
