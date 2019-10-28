# Different service ops... 
# like reducing the file segments count on one data node,
# removing failed segments etc...
import os

from config import config_provider


def split_file(file, m):
    with open(file) as data:
        content = [i for i in data]
        res = [content[x:x + m] for x in range(0, len(content), m)]
        return res


def write_to_file(content, file_name):
    with open(file_name, 'a+') as file:
        file.write(content)
