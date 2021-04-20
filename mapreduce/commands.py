import base64
import os

from config.config_provider import ConfigProvider
from config.logger import client_logger
from http_client import base_http_client

logger = client_logger.get_logger(__name__)

field_delimiter = ConfigProvider(os.path.join('..', 'config', 'json', 'client_config.json')).field_delimiter


class BaseCommand(object):
    def __init__(self, command_body):
        self.command_body = command_body

    def validate(self):
        raise NotImplementedError()

    def send_command(self, command_name, ip=None):
        if not ip:
            return base_http_client.post(self.command_body, command_name)
        else:
            return base_http_client.post(self.command_body, command_name, ip)


class CheckIfFileIsOnCLuster(BaseCommand):

    def __init__(self, file_name):
        self.command_body = {'file_name': file_name}
        super().__init__(self.command_body)

    def validate(self):
        if not self.command_body.get('file_name'):
            raise AttributeError('File name is not specified!')

    def send_command(self, **kwargs):
        self.validate()
        return super().send_command(command_name='check_if_file_is_on_cluster')


class AppendCommand(BaseCommand):

    def __init__(self, file_name):
        self.command_body = {'file_name': file_name}
        super().__init__(self.command_body)

    def validate(self):
        if not self.command_body['file_name']:
            raise AttributeError('Destination file is not specified!')

    def send_command(self, **kwargs):
        self.validate()
        return super().send_command(command_name='append')


class ClearDataCommand(BaseCommand):

    def __init__(self, folder_name, remove_all: bool):
        self.command_body = {'folder_name': folder_name,
                             'remove_all_data': remove_all}

        super().__init__(self.command_body)

    def validate(self):
        if not self.command_body.get('folder_name'):
            raise AttributeError('Folder name is not specified!')

    def send_command(self, **kwargs):
        self.validate()
        return super().send_command(command_name='clear_data')


class CreateConfigAndFilesystem(BaseCommand):

    def __init__(self, file_name):
        self.command_body = {'file_name': file_name}
        super().__init__(self.command_body)

    def validate(self):
        if not self.command_body.get('file_name'):
            raise AttributeError('File_name is not specified!')

    def send_command(self, **kwargs):
        self.validate()
        return super().send_command(command_name='create_config_and_filesystem')


class GetFileCommand(BaseCommand):

    def __init__(self, file_name):
        self.command_body = {"file_name": file_name}
        super().__init__(self.command_body)

    def validate(self):
        pass

    def send_command(self, ip=None, **kwargs):
        self.validate()
        return super().send_command(ip)


class GetFileFromClusterCommand(BaseCommand):

    def __init__(self, file_name, dest_file_name):
        self.command_body = {"file_name": file_name,
                             "dest_file_name": dest_file_name}
        super().__init__(self.command_body)

    def validate(self):
        if not self.command_body.get('file_name'):
            raise AttributeError('File name is not specified!')
        if not self.command_body.get('dest_file_name'):
            raise AttributeError('Dest file name is not specified!')

    def send_command(self, **kwargs):
        self.validate()
        return super().send_command(command_name='get_file_from_cluster')


class GetResultOfKeyCommand(BaseCommand):

    def __init__(self, file_name, key):
        self.command_body = {"file_name": file_name,
                             "key": key,
                             "field_delimiter": field_delimiter}
        super().__init__(self.command_body)

    def validate(self):
        if not self.command_body.get('key'):
            raise AttributeError('Key is not specified!')
        if not self.command_body.get('file_name'):
            raise AttributeError('File name is not specified!')
        if not self.command_body.get('field_delimiter'):
            raise AttributeError('Field delimiter is not specified!')

    def send_command(self, ip=None, **kwargs):
        self.validate()

        return super().send_command(ip)


# TODO: To finish Refactoring
class MapCommand(BaseCommand):

    def __init__(self, is_mapper_in_file, mapper, is_server_source_file, source_file, destination_file):
        self.command_body = {"field_delimiter": field_delimiter}

        self._set_mapper_from_file(mapper) if is_mapper_in_file else self._set_mapper(mapper)
        self._set_server_source_file(source_file) if is_server_source_file else self._set_source_file(source_file)
        self._set_destination_file(destination_file)

        super().__init__(self.command_body)

    def _set_mapper_from_file(self, path):
        with open(path, 'rb') as file:
            file_content = file.read()
            encoded = base64.b64encode(file_content)
            decoded = encoded.decode('utf-8')
            self.command_body['mapper'] = decoded

    def _set_mapper(self, content):
        encoded = base64.b64encode(bytes(content, 'utf-8'))
        decoded = encoded.decode('utf-8')
        self.command_body['mapper'] = decoded

    def _set_server_source_file(self, src_file):
        encoded = src_file
        self.command_body['server_source_file'] = encoded

    def _set_source_file(self, src_file):
        encoded = src_file
        self.command_body['source_file'] = encoded

    def _set_destination_file(self, dest_file):
        encoded = dest_file
        self.command_body['destination_file'] = encoded

    def validate(self):
        if not self.command_body.get('mapper'):
            raise AttributeError('Mapper is empty!')
        if not self.command_body.get('destination_file'):
            raise AttributeError('Destination file in not mentioned!')

    def send_command(self, **kwargs):
        self.validate()
        return super().send_command(command_name='map')


class ReduceCommand(BaseCommand):

    def __init__(self, is_reducer_in_file, reducer, is_server_source_file, source_file, destination_file):
        self.command_body = {"field_delimiter": field_delimiter}
        self._set_reducer_from_file(reducer) if is_reducer_in_file else self._set_reducer(reducer)
        self._set_server_source_file(source_file) if is_server_source_file else self._set_source_file(source_file)
        self._set_destination_file(destination_file)
        super().__init__(self.command_body)

    def _set_reducer_from_file(self, path):
        with open(path, 'rb') as file:
            file_content = file.read()
            encoded = base64.b64encode(file_content)
            decoded = encoded.decode('utf-8')
            self.command_body['reducer'] = decoded

    def _set_reducer(self, content):
        encoded = base64.b64encode(bytes(content, 'utf-8'))
        decoded = encoded.decode('utf-8')
        self.command_body['reducer'] = decoded

    def _set_server_source_file(self, src_file):
        encoded = src_file
        self.command_body['server_source_file'] = encoded

    def _set_source_file(self, src_file):
        encoded = src_file
        self.command_body['source_file'] = encoded

    def _set_destination_file(self, dest_file):
        encoded = dest_file
        self.command_body['destination_file'] = encoded

    def validate(self):
        if not self.command_body.get('reducer'):
            raise AttributeError('Reducer is empty!')
        if not self.command_body.get('destination_file'):
            raise AttributeError('Destination file in not mentioned!')

    def send_command(self, **kwargs):
        self.validate()
        return super(ReduceCommand, self).send_command(command_name='reduce')


class MoveFileToInitFolderCommand(BaseCommand):
    def __init__(self, file_name):
        self.command_body = {'file_name': file_name}
        super().__init__(self.command_body)

    def validate(self):
        pass

    def send_command(self, **kwargs):
        return super().send_command(command_name='move_file_to_init_folder')


class RefreshTableCommand(BaseCommand):

    def __init__(self, file_name, ip, segment_name):
        self.command_body = {
            "file_name": file_name,
            "ip": ip,
            "segment_name": segment_name,
        }
        super().__init__(self.command_body)

    def validate(self):
        if not self.command_body.get('file_name'):
            raise AttributeError('File name is not specified!')
        if not self.command_body.get('segment_name'):
            raise AttributeError('Segment name is not specified!')

    def send_command(self, **kwargs):
        self.validate()
        return super().send_command('refresh_table')


class ShuffleCommand(BaseCommand):

    def __init__(self, src_file):
        self.command_body = {
            "field_delimiter": field_delimiter,
            "source_file": src_file,
        }
        super().__init__(self.command_body)

    def validate(self):
        pass

    def send_command(self, **kwargs):
        self.validate()
        return super().send_command(command_name='shuffle')


class WriteCommand(BaseCommand):

    def __init__(self, file_name, segment, data_node_ip):
        self.command_body = {
            "file_name": file_name,
            "segment": segment,
            "data_node_ip": data_node_ip,
        }
        super().__init__(self.command_body)

    def validate(self):
        if not self.command_body.get('segment'):
            raise AttributeError('Segment is not specified!')
        if not self.command_body.get('file_name'):
            raise AttributeError('File name is not specified!')
        if not self.command_body.get('data_node_ip'):
            raise AttributeError('Data node ip is not specified!')

    def send_command(self, **kwargs):
        self.validate()
        return super().send_command(command_name='write', ip=self.command_body['data_node_ip'])
