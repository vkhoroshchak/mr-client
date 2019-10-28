# Different service ops... 
# like reducing the file segments count on one data node,
# removing failed segments etc...
from config import config_provider
import os


def split_file(file, m):
    f_d = config_provider.ConfigProvider.get_field_delimiter(os.path.join('..', 'config', 'json', 'client_config.json'))
    # m_k_d = config_provider.ConfigProvider.get_map_key_delimiter('\\..\\config\\json\\client_config.json')
    data = open(file)
    content = list()
    for i in data.readlines():
        # content.append(i.strip())
        content.append(i)
    res = [content[x:x + m] for x in range(0, len(content), m)]

    # print(res)
    return res

def write_to_file(content, file_name):
    print(content)
    file = open(file_name, 'a+')
    file.write(content)
    file.close()
