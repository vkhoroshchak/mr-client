import os
def custom_reducer(content,key):
	import collections
	left_part = key.split(':')[1]
	key_list = left_part.split(',')
	result_dict = collections.Counter()
	for field in content:
		key = field.split('^')[0]
		if key in result_dict.keys():
			pass
		else:
			sum = 0
			for i in content:
				line = i.split('^')[1].split('|')
				if i.split('^')[0] == key:
					for item in key_list:
						sum += int(line[int(item)])
				result_dict[key] = sum
	result = list()
	for item in result_dict.keys():
		result.append(item+'^'+ item + '|' + str(result_dict[item]) + '\n')
	return result
