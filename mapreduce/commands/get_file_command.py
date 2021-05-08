from mapreduce.commands import base_command


class GetFileCommand(base_command.BaseCommand):

    def __init__(self):
        self._data = {}
        super().__init__(self._data)

    def set_file_name(self, file_name):
        self._data["file_name"] = file_name

    def validate(self):
        pass

    def send(self, ip=None, **kwargs):
        self.validate()
        return super().send(ip) if ip else super().send()
