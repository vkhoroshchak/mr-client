from mapreduce.commands import base_command


class GetResultOfKeyCommand(base_command.BaseCommand):

	def __init__(self):
		self._data = {}

	def set_key(self, key):
		self._data['key'] = key

	def set_file_name(self, file_name):
		self._data['file_name'] = file_name

	def set_field_delimiter(self, field_delimiter):
		self._data['field_delimiter'] = field_delimiter

	def validate(self):
		pass

	def send(self, ip=None):
		self.validate()
		data = dict()
		data['get_result_of_key'] = self._data
		super(GetResultOfKeyCommand, self).__init__(data)
		if ip is None:
			return super(GetResultOfKeyCommand, self).send()
		else:
			return super(GetResultOfKeyCommand, self).send(ip)
