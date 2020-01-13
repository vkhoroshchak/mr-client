from mapreduce.commands import base_command


# TODO: add validation
class GetFileCommand(base_command.BaseCommand):

    def __init__(self):
        self._data = {}

    def set_file_name(self, file_name):
        self._data["file_name"] = file_name

    def validate(self):
        pass

    def send(self, ip=None):
        self.validate()
        super(GetFileCommand, self).__init__(self._data)
        return super(GetFileCommand, self).send(ip) if ip else super(GetFileCommand, self).send()
