from mapreduce.commands import base_command


class GetFileCommand(base_command.BaseCommand):

	def __init__(self):
		self._data = {}

	def set_file_name(self, file_name):
		self._data["file_name"] = file_name

	def validate(self):
		pass

	def send(self, ip=None):
		self.validate()
		data = dict()
		data['get_file'] = self._data
		super(GetFileCommand, self).__init__(data)
		if ip is None:
			return super(GetFileCommand, self).send()
		else:
			return super(GetFileCommand, self).send(ip)