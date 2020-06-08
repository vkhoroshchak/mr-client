from mapreduce.commands import base_command


# TODO: add validation
class GetFileFromClusterCommand(base_command.BaseCommand):

    def __init__(self):
        self._data = {}
        super().__init__(self._data)

    def set_file_name(self, file_name):
        self._data["file_name"] = file_name

    def set_dest_file_name(self, dest_file_name):
        self._data['dest_file_name'] = dest_file_name

    # def set_data_node_ip(self, data_node_ip):
    #     self._data['data_node_ip'] = data_node_ip

    def validate(self):
        if not self._data['file_name']:
            raise AttributeError('File name is not specified!')
        if not self._data['dest_file_name']:
            raise AttributeError('Dest file name is not specified!')
        # if not self._data['data_node_ip']:
        #     raise AttributeError('Data node ip is not specified!')

    def send(self, **kwargs):
        self.validate()
        return super().send('get_file_from_cluster')
