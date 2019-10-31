import os

def custom_mapper(content, field_delimiter, key):
	right_part = key.split(':')[0]
	key_list = right_part.split(',')
	res = list()
	for field in content:
		field = field[:-1]
		line = field.split(field_delimiter)
		res_line = str()
		for item in key_list:
			res_line += line[int(item)]
			res_line += field_delimiter
		k = res_line[:-1]
		res_line = k + '^' + field
		res.append(res_line + '\n')
	return res
