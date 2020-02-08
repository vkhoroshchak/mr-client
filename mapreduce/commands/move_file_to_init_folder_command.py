import base64
from mapreduce.commands import base_command


class MoveFileToInitFolderCommand(base_command.BaseCommand):
    def __init__(self):
        self._data = {}

    def send(self):
        super(MoveFileToInitFolderCommand, self).__init__(self._data)
        return super(MoveFileToInitFolderCommand, self).send('move_file_to_init_folder')
