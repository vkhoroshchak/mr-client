import pandas as pd


def custom_mapper(data_frame, col_names):
    old_names = []
    new_names = []
    for i in col_names:
        if i['old_name'] == '*':
            res = data_frame.copy()
            return res

        old_names.append(i['old_name'])
        new_names.append(i['new_name'])

    res = data_frame[old_names].copy()
    res.columns = new_names

    return res
