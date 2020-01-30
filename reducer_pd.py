import pandas as pd


def custom_reducer(data_frame, col_names, groupby_cols):
    for i in col_names:
        if 'aggregate_f_name' in i.keys():
            data_frame[i['new_name']] = data_frame.groupby(groupby_cols)[i['new_name']].transform(
                i['aggregate_f_name'])
    res = data_frame.drop_duplicates(groupby_cols)

    return res
