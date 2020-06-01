import fnmatch
import os
from src import aggregatorscript, pipelinescript


class OffsetSearchResult:
    def __init__(self):
        self.offset = None
        self.consumedTime = None
        self.sentTime = None
        self.consumed = False  # True if it was consumed, false if failed
        self.pipelineMessages = []
        self.aggregatorMessages = []


def search_by_offset(log_dir, offset):
    aggregator_logs = []
    pipeline_logs = []
    results = []
    for file in os.listdir(log_dir):
        full_path = os.path.join(log_dir, file)
        if fnmatch.fnmatch(full_path, 'aggregator*.log'):
            aggregator_logs.extend(get_results_by_offset(full_path, offset, False))
        elif fnmatch.fnmatch(full_path, 'pipeline*.log'):
            pipeline_logs.extend(get_results_by_offset(full_path, offset, True))

    for pipe_log in pipeline_logs:
        match_flag = False
        for agg_log in aggregator_logs:
            if pipe_log.offset == agg_log.offset and pipe_log == offset:
                match_flag = True
                new_result = OffsetSearchResult()
                new_result.offset = pipe_log.offset
                new_result.consumeTime = agg_log.timestamp
                new_result.sentTime = pipe_log.timestamp
                new_result.consumed = False if agg_log.error else True
                new_result.aggregatorMessages = agg_log.messages
                new_result.pipelineMessages = pipe_log.messages
                results.append(new_result)
                break

        if not match_flag:
            new_result = OffsetSearchResult
            new_result.offset = pipe_log.offset
            new_result.sentTime = pipe_log.timestamp
            new_result.pipelineMessages = pipe_log.messages
            new_result.consumed = False
            results.append(new_result)

    return results


# @param path_to_log: log file to read the logs from
# @param offset: the offset to search by
# @return value: list of ConsumedGroups found in log file that match the given offset
def get_results_by_offset(path_to_log, offset, pipeline=True):
    if pipeline:
        groups = pipelinescript.get_log_items(path_to_log)
    else:
        groups = aggregatorscript.get_groups(path_to_log)

    matches = [group for group in groups if group.offset == offset]
    return matches


# @param matches: list of matches from the other search methods
# @return value: returns the matches in SearchResult format
def get_offset_search_objects(matches: list):
    # create a search result Object for each match
    search_results = []
    for match in matches:
        new_result = OffsetSearchResult()
        new_result.timestamp = match.timestamp  # location of timestamp in ConsumedGrouping
        new_result.description = [[message[0], message[1]] for message in match.messages]
        new_result.offset = match.offset

        # status = found if there's no error, status = error if there's an error
        new_result.status = "found" if 'error' not in (message[0] for message in match.messages) else "error"
        search_results.append(new_result)
    return search_results


if __name__ == "__main__":

    test_results = search_by_offset("../logs/", 28)
    print(test_results)
