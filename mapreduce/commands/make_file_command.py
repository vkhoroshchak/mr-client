from mapreduce.commands import base_command


class MakeFileCommand(base_command.BaseCommand):

	def __init__(self):
		self._data = {}

	def set_destination_file(self, destination_file):
		self._data['destination_file'] = destination_file

	def validate(self):
		pass

	def send(self):
		self.validate()
		data = dict()
		data['make_file'] = self._data
		super(MakeFileCommand, self).__init__(data)
		return super(MakeFileCommand, self).send()

