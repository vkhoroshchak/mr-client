from mapreduce.commands import base_command


class ShuffleCommand(base_command.BaseCommand):

    def __init__(self):
        self._data = {}

    def set_source_file(self, src_file):
        encoded = src_file
        self._data['source_file'] = encoded

    def validate(self):
        pass

    def send(self):
        self.validate()
        super(ShuffleCommand, self).__init__(self._data)

        super(ShuffleCommand, self).send('shuffle')

        return True
