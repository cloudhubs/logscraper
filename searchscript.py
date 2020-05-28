import aggregatorScript
import sys


def searchByOffset(path_to_log, offset):
    log_list = aggregatorScript.get_log_list(path_to_log)

    groups = aggregatorScript.get_groups(log_list)

    return [group for group in groups if group.offset == offset]


if __name__ == "__main__":
    print(sys.argv[1])
    logs = aggregatorScript.get_log_list("logs/aggregator.log")
    results = searchByOffset(sys.argv[1], 1055)
    print(results)
