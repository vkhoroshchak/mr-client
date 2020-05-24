import argparse
import os

from mapreduce import task_runner_proxy

parser = argparse.ArgumentParser()
parser.add_argument("--m", "--mapper", action="store", help="Set mapper as a content")
parser.add_argument("--mf", "--mapper_from_file", action="store", help="Set mapper as a file path where it is located")
parser.add_argument("--r", "--reducer", action="store", help="Set reducer as a content")
parser.add_argument("--rf", "--reducer_from_file", action="store",
                    help="Set reducer as a file path where it is located")
parser.add_argument("--src", "--source_file", action="store", help="Source file path")
parser.add_argument("--is_src", "--is_server_source_file", action="store", help="If source file is on server")
parser.add_argument("--dest", "--destination_file", action="store", help="Destination file path")
parser.add_argument("--rk", "--result_key", action="store", help="get result of a specified key")
parser.add_argument("--rem", "--remove_files", action="store", help="clear all data")
parser.add_argument("--pfc", "--push file on cluster", action="store", help="Pushes and stores file on cluster")
parser.add_argument("--key", action="store")
parser.add_argument("--map", action="store", help="Run map")
parser.add_argument("--shuffle", action="store", help="Run shuffle")
parser.add_argument("--reduce", action="store", help="Run reduce")

args = parser.parse_args()


# TODO: refactor
def cli_parser(tr):
    # TaskRunner.shuffle(src_file, key)
    # TaskRunner.reduce(is_reducer_in_file, reducer, is_server_source_file, src_file, dest_file)
    # tr.map(is_mapper_in_file, mapper, is_server_source_file, src_file, dest_file)
    is_server_source_file = args.is_src is not None

    if args.map:
        if not args.mf:
            is_mapper_in_file = False
            mapper = args.m
        else:
            is_mapper_in_file = True
            mapper = args.mf
        tr.map(is_mapper_in_file, mapper, is_server_source_file, args.src, args.dest)
    elif args.shuffle:
        tr.shuffle(args.src)
    elif args.reduce:
        if not args.rf:
            is_reducer_in_file = False
            reducer = args.r
        else:
            is_reducer_in_file = True
            reducer = args.rf
        tr.reduce(is_reducer_in_file, reducer, is_server_source_file, args.src, args.dest)
    elif args.pfc:
        print("STARTED TO CHECK IF FILE IS ON CLUSTER")
        is_file_on_cluster = tr.check_if_file_is_on_cluster(args.dest)['is_file_on_cluster']
        print(is_file_on_cluster)
        print("FINISHED TO CHECK IF FILE IS ON CLUSTER")
        if not is_file_on_cluster:
            print("PUSHING FILE ON CLUSTER")
            tr.push_file_on_cluster(args.src, args.dest)
        else:
            print("move_file_to_init_folder".upper())
            tr.move_file_to_init_folder(args.src)
            print("move_file_to_init_folder finished".upper())
    elif args.rem:
        print("CLEAR_DATA_STARTED")
        print("CLEAR_DATA_FINISHED!")
        return tr.clear_data(args.rem)

    # if args.pfc:
    #     src_file, dest_file = args.pfc.split(",")
    #     return tr.push_file_on_cluster(src_file, dest_file)
    # if args.rem:
    #     print("CLEAR_DATA_STARTED")
    #     print("CLEAR_DATA_FINISHED!")
    #     return tr.clear_data(args.rem)
    #
    # if args.rk and args.dest:
    #     print("GET_KEY_FROM_CLUSTER")
    #     return tr.get_result_of_key(args.rk, args.dest)
    #
    # if not args.mf:
    #     is_mapper_in_file = False
    #     mapper = args.m
    # else:
    #     is_mapper_in_file = True
    #     mapper = args.mf
    #
    # if not args.rf:
    #     is_reducer_in_file = False
    #     reducer = args.r
    # else:
    #     is_reducer_in_file = True
    #     reducer = args.rf

    # tr.run_map_reduce_command(is_mapper_in_file, mapper, is_reducer_in_file, reducer, args.src, args.dest, args.key)


if __name__ == '__main__':
    tr = task_runner_proxy.TaskRunner()
    cli_parser(tr)
