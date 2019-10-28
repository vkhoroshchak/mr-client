from mapreduce.commands import base_command


class ClearDataCommand(base_command.BaseCommand):

	def __init__(self):
		self._data = {}

	def set_folder_name(self, folder_name):
		self._data['folder_name'] = folder_name

	def set_remove_all_data(self,remove_all_data):
		self._data['remove_all_data']=bool(int(remove_all_data))

	def validate(self):
		pass

	def send(self):
		self.validate()
		data = dict()
		data['clear_data'] = self._data
		super(ClearDataCommand, self).__init__(data)
		return super(ClearDataCommand, self).send()
