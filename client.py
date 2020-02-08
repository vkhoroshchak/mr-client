import argparse

from mapreduce import task_runner_proxy

parser = argparse.ArgumentParser()
parser.add_argument("--m", "--mapper", action="store", help="Set mapper as a content")
parser.add_argument("--mf", "--mapper_from_file", action="store", help="Set mapper as a file path where it is located")
parser.add_argument("--r", "--reducer", action="store", help="Set reducer as a content")
parser.add_argument("--rf", "--reducer_from_file", action="store",
                    help="Set reducer as a file path where it is located")
parser.add_argument("--kd", "--key_delimiter", action="store", help="Set key delimiter")
parser.add_argument("--src", "--source_file", action="store", help="Source file path")
parser.add_argument("--ssrc", "--server_source_file", action="store", help="Source file path on server")
parser.add_argument("--dest", "--destination_file", action="store", help="Destination file path")
parser.add_argument("--rk", "--result_key", action="store", help="get result of a specified key")
parser.add_argument("--sql", "--sql_query", action="store", help="SQL query")
parser.add_argument("--rem", "--remove_files", action="store", help="clear all data")
parser.add_argument("--pfc", "--push file on cluster", action="store", help="Pushes and stores file on cluster")

args = parser.parse_args()


# TODO: refactor
def cli_parser(tr):
    if args.pfc:
        src_file, dest_file = args.pfc.split(",")
        return tr.push_file_on_cluster(src_file, dest_file)
    if args.rem:
        print("CLEAR_DATA_STARTED")
        print("CLEAR_DATA_FINISHED!")
        return tr.clear_data(args.rem)

    if args.rk and args.dest:
        print("GET_KEY_FROM_CLUSTER")
        return tr.get_result_of_key(args.rk, args.dest)



    if not args.mf:
        is_mapper_in_file = False
        mapper = args.m
    else:
        is_mapper_in_file = True
        mapper = args.mf

    if not args.rf:
        is_reducer_in_file = False
        reducer = args.r
    else:
        is_reducer_in_file = True
        reducer = args.rf

    if not args.src:
        source_file = args.ssrc
        is_server_source_file = True
    else:
        source_file = args.src
        is_server_source_file = False



    choice = input("Map Reduce (1) or SQL (2)?")
    if choice == "1":
        return tr.run_map_reduce(is_mapper_in_file, mapper, is_reducer_in_file, reducer, args.kd, is_server_source_file,
                             source_file, args.dest, args.sql)
    else:
        tr.run_sql_command(is_mapper_in_file, mapper, is_reducer_in_file, reducer, is_server_source_file, args.sql)


if __name__ == '__main__':
    tr = task_runner_proxy.TaskRunner()
    cli_parser(tr)
