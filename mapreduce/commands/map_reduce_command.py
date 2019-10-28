from mapreduce.commands import base_command
import base64
import os


class MapReduceCommand(base_command.BaseCommand):

	def __init__(self):
		self._data = dict()

	def set_mapper_from_file(self, path):
		file = open(path, 'rb')
		file_content = file.read()
		encoded = base64.b64encode(file_content)
		decoded = encoded.decode('utf-8')
		self._data["mapper"] = decoded

	def set_mapper(self, content):
		encoded = base64.b64encode(bytes(content, 'utf-8'))
		decoded = encoded.decode('utf-8')
		self._data["mapper"] = decoded

	def set_reducer_from_file(self, path):
		file = open(path, 'rb')
		file_content = file.read()
		encoded = base64.b64encode(file_content)
		decoded = encoded.decode('utf-8')
		self._data["reducer"] = decoded

	def set_reducer(self, content):
		encoded = base64.b64encode(bytes(content, 'utf-8'))
		decoded = encoded.decode('utf-8')
		self._data["reducer"] = decoded

	# check if method to get (map)_key_delimiter from file is necessary
	def set_key_delimiter(self, key_delimiter):
		# encoded = base64.b64encode(key_delimiter.encode())
		encoded = key_delimiter
		self._data["key_delimiter"] = encoded

	def set_field_delimiter(self, field_delimiter):
		encoded = field_delimiter
		self._data['field_delimiter'] = encoded

	def set_server_source_file(self, src_file):
		encoded = src_file
		self._data["server_source_file"] = encoded

	def set_source_file(self, src_file):
		encoded = src_file
		self._data["source_file"] = encoded

	def set_destination_file(self, dest_file):
		encoded = dest_file
		self._data["destination_file"] = encoded

	def validate(self):
		if self._data['mapper'] is None:
			raise AttributeError("Mapper is empty!")
		if self._data['reducer'] is None:
			raise AttributeError("Reducer is empty!")
		if not 'source_file' in self._data and not'server_source_file' in self._data:
			raise AttributeError("Source file in not mentioned!")
		if self._data['destination_file'] is None:
			raise AttributeError("Destination file in not mentioned!")

	def send(self):
		self.validate()
		data = dict()
		data['map_reduce'] = self._data
		super(MapReduceCommand, self).__init__(data)
		return super(MapReduceCommand, self).send()
