import base64

from mapreduce.commands import base_command


class MapCommand(base_command.BaseCommand):

    def __init__(self):
        self._data = {}
        super().__init__(self._data)

    def set_mapper_from_file(self, path):
        with open(path, 'rb') as file:
            file_content = file.read()
            encoded = base64.b64encode(file_content)
            decoded = encoded.decode('utf-8')
            self._data['mapper'] = decoded

    def set_mapper(self, content):
        encoded = base64.b64encode(bytes(content, 'utf-8'))
        decoded = encoded.decode('utf-8')
        self._data['mapper'] = decoded

    def set_field_delimiter(self, field_delimiter):
        encoded = field_delimiter
        self._data['field_delimiter'] = encoded

    def set_server_source_file(self, src_file):
        encoded = src_file
        self._data['server_source_file'] = encoded

    def set_source_file(self, src_file):
        encoded = src_file
        self._data['source_file'] = encoded

    def set_destination_file(self, dest_file):
        encoded = dest_file
        self._data['destination_file'] = encoded

    def set_parsed_select(self, parsed_select):
        encoded = parsed_select
        self._data['parsed_select'] = encoded

    def validate(self):
        if not self._data['mapper']:
            raise AttributeError('Mapper is empty!')
        # if 'source_file' and 'server_source_file' not in self._data:
        #     raise AttributeError('Source file in not mentioned!')
        if not self._data['destination_file']:
            raise AttributeError('Destination file in not mentioned!')

    def send(self, **kwargs):
        self.validate()
        return super().send('map')
