import aggregatorscript
import sys


def search_by_offset(path_to_log, offset):
    groups = aggregatorscript.get_groups(path_to_log)
    return [group for group in groups if group.offset == offset]


if __name__ == "__main__":
    results = search_by_offset("logs/aggregator.log", 1055)
    for result in results:
        print(result.offset)
