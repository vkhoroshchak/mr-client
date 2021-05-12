import base64
import os

from fastapi import status, HTTPException

from config.config_provider import ConfigProvider
from config.logger import client_logger
from http_client import base_http_client

logger = client_logger.get_logger(__name__)

field_delimiter = ConfigProvider(os.path.join('..', 'config', 'client_config.json')).field_delimiter


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
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail='File name is not specified!')

    def send_command(self, **kwargs):
        self.validate()
        return super().send_command(command_name='check_if_file_is_on_cluster')


class AppendCommand(BaseCommand):

    def __init__(self, file_id):
        self.command_body = {'file_id': file_id}
        super().__init__(self.command_body)

    def validate(self):
        if not self.command_body['file_id']:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail='Destination file is not specified!')

    def send_command(self, **kwargs):
        self.validate()
        return super().send_command(command_name='append')


class ClearDataCommand(BaseCommand):

    def __init__(self, file_id, remove_all: bool):
        self.command_body = {'file_id': file_id,
                             'remove_all_data': remove_all}

        super().__init__(self.command_body)

    def validate(self):
        if not self.command_body.get('file_id'):
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail='File id is not specified!')

    def send_command(self, **kwargs):
        self.validate()
        return super().send_command(command_name='clear_data')


class CreateConfigAndFilesystem(BaseCommand):

    def __init__(self, file_name):
        self.command_body = {'file_name': file_name, 'field_delimiter': field_delimiter}
        super().__init__(self.command_body)

    def validate(self):
        if not self.command_body.get('file_name'):
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail='File name is not specified!')

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
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail='File name is not specified!')
        if not self.command_body.get('dest_file_name'):
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail='Dest file name is not specified!')

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
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail='Key is not specified!')

        if not self.command_body.get('file_name'):
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail='File name is not specified!')

        if not self.command_body.get('field_delimiter'):
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail='Field delimiter is not specified!')

    def send_command(self, ip=None, **kwargs):
        self.validate()

        return super().send_command(ip)


class MapCommand(BaseCommand):

    def __init__(self, is_mapper_in_file, mapper, file_id, source_file):
        self.command_body = {"field_delimiter": field_delimiter, "file_id": file_id, "source_file": source_file}
        self._set_mapper_from_file(mapper) if is_mapper_in_file else self._set_mapper(mapper)

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

    def validate(self):
        if not self.command_body.get('mapper'):
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail='Mapper is empty!')

    def send_command(self, **kwargs):
        self.validate()
        return super().send_command(command_name='map')


class ReduceCommand(BaseCommand):

    def __init__(self, is_reducer_in_file, reducer, file_id, source_file):
        if isinstance(source_file, list):
            source_file = ",".join(source_file)
        self.command_body = {"field_delimiter": field_delimiter, "file_id": file_id, "source_file": source_file}
        self._set_reducer_from_file(reducer) if is_reducer_in_file else self._set_reducer(reducer)

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

    def validate(self):
        if not self.command_body.get('reducer'):
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail='Reducer is empty!')

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
            "file_id": file_name,
            "ip": ip,
            "segment_name": segment_name,
        }
        super().__init__(self.command_body)

    def validate(self):
        if not self.command_body.get('file_id'):
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail='File id is not specified!')

        if not self.command_body.get('segment_name'):
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail='Segment name is not specified!')

    def send_command(self, **kwargs):
        self.validate()
        return super().send_command('refresh_table')


class ShuffleCommand(BaseCommand):

    def __init__(self, file_id, source_file):
        self.command_body = {
            "field_delimiter": field_delimiter,
            "file_id": file_id,
            "source_file": source_file,
        }
        super().__init__(self.command_body)

    def validate(self):
        pass

    def send_command(self, **kwargs):
        self.validate()
        return super().send_command(command_name='shuffle')


class WriteCommand(BaseCommand):

    def __init__(self, file_name, segment, data_node_ip, src_file_name):
        self.command_body = {
            "file_name": file_name,
            "segment": segment,
            "data_node_ip": data_node_ip,
            "src_file_name": src_file_name
        }
        super().__init__(self.command_body)

    def validate(self):
        if not self.command_body.get('segment'):
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail='Segment is not specified!')

        if not self.command_body.get('file_name'):
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail='File name is not specified!')

        if not self.command_body.get('data_node_ip'):
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail='Data node ip is not specified!')

    def send_command(self, **kwargs):
        self.validate()
        return super().send_command(command_name='write', ip=self.command_body['data_node_ip'])
