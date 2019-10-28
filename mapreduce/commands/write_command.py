from mapreduce.commands import base_command


class WriteCommand(base_command.BaseCommand):

	def __init__(self):
		self._data = {}

	def set_segment(self, segment):
		self._data['segment'] = segment

	def set_file_name(self, file_name):
		self._data["file_name"] = file_name

	def set_data_node_ip(self, data_node_ip):
		self._data['data_node_ip'] = data_node_ip

	def validate(self):
		pass

	def send(self):
		self.validate()
		data = dict()
		data['write'] = self._data
		super(WriteCommand, self).__init__(data)
		return super(WriteCommand, self).send(self._data['write']['data_node_ip'])
