import json
import os

import moz_sql_parser as msp

from config.logger import client_logger

logger = client_logger.get_logger(__name__)


class SQLParser:
    @staticmethod
    def parse_aggregation_value(name, data):
        res = {'old_name': data['value'][name]}
        if 'name' in data.keys():
            res['new_name'] = f"{data['name']}"
        else:
            res['new_name'] = f"{name.upper()}_{data['value'][name]}"
        res['aggregate_f_name'] = name
        return res

    @staticmethod
    def from_parser(sql_from):
        if isinstance(sql_from, list):
            first, second = sql_from
            if isinstance(first, dict):
                if first.get("name", "").upper() == "OUTER":
                    return first.get("value"), second.get("outer join")
            else:
                for join_type in (
                        'join',
                        'inner join',
                        'right join',
                ):
                    if second.get(join_type):
                        return first, second.get(join_type)
                if second.get('full join'):
                    return first, second.get('outer join')
        elif isinstance(sql_from, dict):
            return sql_from.get('value')
        else:
            return sql_from

    @staticmethod
    def join_parser(data):
        join_info = data['from'][1]
        join_type = next(iter(join_info), None)

        if join_type:
            join_type = join_type.split(' ')[0]
        if join_type == "join":
            join_type = "inner"

        on = join_info['on']['eq']
        res = {
            'join_type': join_type,
            'on': on
        }
        return res

    @staticmethod
    def select_parser(select_data):
        res = []
        item_dict = {}
        if select_data == '*':
            item_dict['old_name'] = select_data
            item_dict['new_name'] = select_data
            res.append(item_dict)
        else:
            if type(select_data) is list:
                for i in select_data:
                    res.append(SQLParser.process_dict_item(i))
            else:
                res.append(SQLParser.process_dict_item(select_data))
        return res

    @staticmethod
    def split_select_cols(file_name, parsed_select):
        file_name, ext = os.path.splitext(file_name)
        res = []
        for i in parsed_select:
            col_file_name, col_name = i['old_name'].split('.')
            if col_file_name == file_name:
                if i['old_name'] != i['new_name']:
                    res.append(
                        {'old_name': col_name,
                         'new_name': i['new_name']
                         }
                    )
                else:
                    res.append(
                        {'old_name': col_name,
                         'new_name': col_name
                         }
                    )

        return res

    @staticmethod
    def process_dict_item(diction):
        item_dict = {}
        if type(diction['value']) is not dict:
            item_dict['old_name'] = diction['value']
            if 'name' in diction.keys():
                item_dict['new_name'] = diction['name']
            else:
                item_dict['new_name'] = diction['value']
        elif 'literal' in diction['value'].keys():
            item_dict['old_name'] = diction['value']['literal']
            if 'name' in diction.keys():
                item_dict['new_name'] = diction['name']
            else:
                item_dict['new_name'] = diction['value']['literal']
        elif 'sum' in diction['value'].keys():
            item_dict = SQLParser.parse_aggregation_value('sum', diction)

        elif 'min' in diction['value'].keys():
            item_dict = SQLParser.parse_aggregation_value('min', diction)
        elif 'max' in diction['value'].keys():
            item_dict = SQLParser.parse_aggregation_value('max', diction)
        elif 'avg' in diction['value'].keys():
            item_dict = SQLParser.parse_aggregation_value('avg', diction)
        elif 'count' in diction['value'].keys():
            item_dict = SQLParser.parse_aggregation_value('count', diction)

        return item_dict

    @staticmethod
    def group_by_parser(sql_group_by):
        res = []
        if type(sql_group_by) is list:
            for item in sql_group_by:
                item_dict = {}
                item_dict['key_name'] = item['value']
                if type(item_dict['key_name']) is dict:
                    item_dict['key_name'] = item_dict['key_name']['literal']
                res.append(item_dict)
        else:
            item_dict = {}
            gb_val = sql_group_by['value']
            if type(gb_val) is dict:
                item_dict['key_name'] = gb_val['literal']
            else:
                item_dict['key_name'] = gb_val

            res.append(item_dict)
        return res

    @staticmethod
    def where_parser(sql_where):  # noqa: C901
        def process_condition_dict(condition_dict):
            oper = list(condition_dict.keys())[0]
            res = {
                oper: {}
            }

            oper_to_operator = {
                "eq": "==",
                "neq": "!=",
                "gt": ">",
                "lt": "<",
                "gte": ">=",
                "lte": "<=",
            }

            operator = oper_to_operator.get(oper)

            if operator:
                left, right = condition_dict[oper].get("literal") if isinstance(condition_dict[oper], dict) \
                    else condition_dict[oper]

                res[oper]["operator"] = operator
                res[oper]["left"] = left
                res[oper]["right"] = right.get("literal") if isinstance(right, dict) else right
            elif oper.endswith("between"):
                literal = condition_dict[oper]
                col = literal[0]
                left = literal[1].get("literal") if isinstance(literal[1], dict) else literal[1]
                right = literal[2].get("literal") if isinstance(literal[2]) else literal[2]

                if oper == "not_between":
                    first_oper = "<"
                    second_oper = ">"
                else:
                    first_oper = ">="
                    second_oper = "<="

                res[oper]["operator"] = "&" if oper == "between" else "|"
                res[oper]["col"] = col
                res[oper]["first_oper"] = first_oper
                res[oper]["left"] = left
                res[oper]["second_oper"] = second_oper
                res[oper]["right"] = right

            elif oper.endswith("like"):
                literal = condition_dict[oper]
                column = literal[0]
                pattern = literal[1]["literal"]
                escaped_chars = [".", "^", "$", "*", "+", "?", "{", "}", "\\", "[", "]", "|", "(", ")"]
                escaped_pattern = pattern
                for esc in escaped_chars:
                    if esc in escaped_pattern:
                        escaped_pattern = escaped_pattern.replace(esc, "\\" + esc)
                re_pattern = escaped_pattern.replace("%", ".*").replace("_", ".") + "$"
                not_keyword = "~" if oper == "nlike" else ""

                res[oper]["not_keyword"] = not_keyword
                res[oper]["column"] = column
                res[oper]["re_pattern"] = re_pattern
            elif oper.endswith("in"):
                literal = condition_dict[oper]
                column = literal[0]
                list_of_literals = literal[1].get("literal") if isinstance(literal[1], dict) else literal[1]
                not_keyword = "~" if oper == "nin" else ""

                res[oper]["not_keyword"] = not_keyword
                res[oper]["column"] = column
                res[oper]["list_of_literals"] = list_of_literals
            else:
                raise ValueError("SQL operator not parser properly!")
            return res

        res = {}
        main_oper = list(sql_where.keys())[0]

        main_oper_to_concat_oper = {
            "or": " | ",
            "and": " & ",
        }

        concat_oper = main_oper_to_concat_oper.get(main_oper)

        if concat_oper:
            res[concat_oper] = []
            for d in sql_where[main_oper]:
                res[concat_oper].append(process_condition_dict(d))
        else:
            res[concat_oper] = process_condition_dict(sql_where)

        return res

    @staticmethod
    def orderby_parser(sql_orderby):
        val = sql_orderby['value']
        sort_asc = True
        col = val if not type(val) is dict else val['literal']
        if 'sort' in sql_orderby:
            if sql_orderby['sort'] == 'desc':
                sort_asc = False
        return col, sort_asc

    @staticmethod
    def sql_parser(sql_query):
        if type(sql_query) is dict:
            while 'value' in sql_query:
                sql_query = sql_query['value']
            json_res = sql_query
        else:
            parsed_sql = json.dumps(msp.parse(sql_query))
            json_res = json.loads(parsed_sql)
        res = {}
        if 'where' in json_res:
            res['where'] = SQLParser.where_parser(json_res['where'])
        if 'select' in json_res:
            res['select'] = SQLParser.select_parser(json_res['select'])
        if 'groupby' in json_res:
            res['groupby'] = SQLParser.group_by_parser(json_res['groupby'])
        if 'orderby' in json_res:
            res['orderby'] = SQLParser.orderby_parser(json_res['orderby'])
        if 'from' in json_res:
            if type(json_res['from']) is list:
                res['from'] = SQLParser.from_parser(json_res['from'])
                res['join'] = SQLParser.join_parser(json_res)
            elif type(json_res['from']) is dict:
                res['from'] = SQLParser.sql_parser(json_res['from'])
            else:
                res['from'] = SQLParser.from_parser(json_res['from'])
        return res

    @staticmethod
    def get_key_col(parsed_sql, file_name=None):
        if 'join' in parsed_sql:
            if file_name:
                file_name, ext = os.path.splitext(file_name)
                col_file_name, col_name = parsed_sql['join']['on'][0].split('.')
                if col_file_name == file_name:
                    return col_name
                else:
                    col_file_name, col_name = parsed_sql['join']['on'][1].split('.')
                    return col_name

        if 'groupby' in parsed_sql:
            return parsed_sql['groupby'][0]['key_name']
        if 'select' in parsed_sql:
            col = parsed_sql['select'][0]
            if 'new_name' in col:
                return col['new_name']
            else:
                return col['value']


def custom_reducer(parsed_sql, field_delimiter):  # noqa: C901
    def where_dict_to_command(where_dict):
        oper = list(where_dict.keys())[0]
        results = where_dict[oper]
        if oper.endswith("in"):
            comm = f"{results['not_keyword']}data_frame.{results['column'].title()}.isin(" \
                       f"{results['list_of_literals']})"
        elif oper.endswith("like"):
            comm = f"{results['not_keyword']}data_frame.{results['column'].title()}.apply(str).str.match" \
                       f"('{results['re_pattern']}')"
        elif oper.endswith("between"):
            comm = f"(data_frame.{results['col'].title()} {results['first_oper']} " \
                       f"{results['left']}) {results['operator']} (data_frame.{results['col'].title()} " \
                       f"{results['second_oper']} {results['right']})"
        else:
            comm = f"data_frame.{results['left'].title()} {results['operator']} {results['right']}"
        return comm

    def parse_from(from_file):
        if isinstance(from_file, tuple):
            parsed_join = parsed_sql["join"]
            left_df_col_name = parsed_join['on'][0].split('.')[1]
            right_df_col_name = parsed_join['on'][1].split('.')[1]
            return f"""
    l_file_name, r_file_name = file_name
    # left_df = dd.read_csv(l_file_name)
    # right_df = dd.read_csv(r_file_name)
    left_df = dd.read_parquet(l_file_name)
    right_df = dd.read_parquet(r_file_name)
    left_df = left_df.drop(columns=['key_column'])
    right_df = right_df.drop(columns=['key_column'])
    left_df_col_name = '{left_df_col_name}'
    right_df_col_name = '{right_df_col_name}'
    data_frame = dd.merge(left=left_df,
                          how='{parsed_join['join_type']}',
                          right=right_df,
                          left_on=left_df_col_name,
                          right_on=right_df_col_name)
    """
        else:
            return f"""
    # data_frame = dd.read_csv(file_name, sep='{field_delimiter}')
    data_frame = dd.read_parquet(file_name, sep='{field_delimiter}')
    """

    def parse_where(parsed_where):
        if parsed_where:
            main_oper = list(parsed_where.keys())[0]
            if main_oper in ("none", None):
                dict_for_process = parsed_where[main_oper]
                command = where_dict_to_command(dict_for_process)
            else:
                commands = []
                for d in parsed_where[main_oper]:
                    commands.append(where_dict_to_command(d))
                command = main_oper.join([f"({x})" for x in commands])
            return f"""
    data_frame = data_frame[{command}]
    """
        else:
            return ""

    def parse_groupby(parsed_groupby, select_cols):
        if parsed_groupby:
            groupby_col = parsed_groupby[0]
            return f"""
    for i in {select_cols}:
        if 'aggregate_f_name' in i.keys():
            if {groupby_col}['key_name']:
                data_frame[i['new_name']] = data_frame.groupby({groupby_col}['key_name'])[i['new_name']].transform(
                    i['aggregate_f_name'])
            else:
                data_frame[i['new_name']] = data_frame.groupby(i['new_name'])[i['new_name']].transform(
                    i['aggregate_f_name'])
    data_frame = data_frame.drop_duplicates({groupby_col}['key_name'])
    """
        else:
            return ""

    def parse_select(select_cols):
        if select_cols == ["*"]:
            return """
    data_frame = data_frame.drop(columns='key_column')
    """
        else:
            return f"""
    data_frame = data_frame[{select_cols}]
    """

    def parse_orderby(parsed_orderby):
        if parsed_orderby:
            col, asc = parsed_orderby
            return f"""
    data_frame = data_frame.sort_values(by='{col}', ascending={asc})
    """
        else:
            return ""

    res = """
def custom_reducer(file_name, dest):
    import dask.dataframe as dd
    """

    res += parse_from(parsed_sql.get("from"))

    res += parse_where(parsed_sql.get("where"))

    res += parse_groupby(parsed_sql.get("groupby"), parsed_sql.get('select'))

    res += parse_select([i['new_name'].split(".")[-1] for i in parsed_sql.get('select')])

    res += parse_orderby(parsed_sql.get("orderby"))

    res += f"""
    data_frame.to_csv(dest, index=False, sep='{field_delimiter}')
    """

    return res


def custom_mapper(key_column, col_names, field_delimiter):
    return f"""
def custom_mapper(file_name):
    import dask.dataframe as dd

    def update_col_names():
        df_col_names = list(data_frame)
        if {col_names}[0]['old_name'] == '*':
            res = data_frame.copy()
            return res

        for i in range(len({col_names})):
            for j in range(len(df_col_names)):
                if df_col_names[j] == {col_names}[i]['old_name']:
                    df_col_names[j] = {col_names}[i]['new_name']

        res = data_frame.copy()
        res.columns = df_col_names
        return res

    data_frame = dd.read_csv(file_name, sep='{field_delimiter}')

    data_frame = update_col_names()

    data_frame['key_column'] = data_frame['{key_column}'] if '{key_column}' != '*' else data_frame[data_frame.columns[0]]

    return data_frame
"""
