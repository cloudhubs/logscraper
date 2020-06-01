import aggregatorscript
import sys


def search_by_offset(path_to_log, offset):
    log_list = aggregatorscript.get_groups(path_to_log)

    groups = aggregatorscript.get_groups(log_list)

    return [group for group in groups if group.offset == offset]




if __name__ == "__main__":
    print(sys.argv[1])
    logs = aggregatorscript.get_log_list("logs/aggregator.log")
    results = search_by_offset(sys.argv[1], 1055)
    print(results)
