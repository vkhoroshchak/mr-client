# Different service ops... 
# like reducing the file segments count on one data node,
# removing failed segments etc...
import os

from config import config_provider


def split_file(filehandler, delimiter=',', row_limit=1000,
               output_name_template='data_%s.csv', output_path='temp_data', keep_headers=True):
    import csv
    reader = csv.reader(open(filehandler, 'r', encoding='utf-8'), delimiter=delimiter)
    current_piece = 1
    if not os.path.exists(output_path):
        os.mkdir(output_path)
    current_out_path = os.path.join(
        output_path,
        output_name_template % current_piece
    )
    current_out_writer = csv.writer(open(current_out_path, 'w', encoding='utf-8'), delimiter=delimiter)
    current_limit = row_limit
    if keep_headers:
        headers = next(reader)
        current_out_writer.writerow(headers)
    for i, row in enumerate(reader):

        if i + 1 > current_limit:
            current_piece += 1
            current_limit = row_limit * current_piece
            current_out_path = os.path.join(
                output_path,
                output_name_template % current_piece
            )
            current_out_writer = csv.writer(open(current_out_path, 'w', encoding='utf-8'), delimiter=delimiter)
            if keep_headers:
                current_out_writer.writerow(headers)
        current_out_writer.writerow(row)
