import fnmatch
import os
import aggregatorscript
import pipelinescript

class SearchResult:
    def __init__(self):
        self.timestamp = None
        self.status = None
        self.description = []
        
class SearchResult:
    def __init__(self):
        self.offset = None
        self.consumedTime = None
        self.sentTime = None
        self.consumed = False  # True if it was consumed, false if failed
        self.pipelineMessages = []
        self.aggregatorMessages = []


def search_by_offset(log_dir, offset):
    holder = []
    aggregator_logs = []
    pipeline_logs = []
    results = []
    for file in os.listdir(log_dir):
        full_path = os.path.join(log_dir, file)
        if fnmatch.fnmatch(full_path, '*/aggregator*.log'):
            holder = get_results_by_offset(full_path, offset, False)
            if holder is not None:
                aggregator_logs.append(holder)
        elif fnmatch.fnmatch(full_path, '*/pipeline*.log'):
            holder = get_results_by_offset(full_path, offset, True)
            if holder is not None:
                pipeline_logs.append(holder)

    if pipeline_logs is not None and aggregator_logs is not None:
        for pipe_log in pipeline_logs:
            i = 0
            for agg_log in aggregator_logs:
                j = 0
                if pipe_log[i].offset == agg_log[j].offset and pipe_log[i].offset == offset:
                    new_result = SearchResult()
                    new_result.offset = pipe_log[i].offset
                    new_result.consumeTime = agg_log[j].timestamp
                    new_result.sentTime = pipe_log[i].timestamp
                    new_result.consumed = False if agg_log[j].error else True
                    new_result.aggregatorMessages = agg_log[j].messages
                    new_result.pipelineMessages = pipe_log[i].messages
                    results.append(new_result.__dict__)
                    break
                if j < len(agg_log) -1:
                    j += 1

            if i < len(pipe_log) -1:
                i += 1


    # for pipe_log in pipeline_logs:
    #     match_flag = False
    #     for agg_log in aggregator_logs:
    #         if len(pipe_log) > 0 and len(agg_log) > 0:
    #             match_flag = True
    #             new_result = SearchResult()
    #             new_result.offset = pipe_log.offset
    #             new_result.consumeTime = agg_log.timestamp
    #             new_result.sentTime = pipe_log.timestamp
    #             new_result.consumed = False if agg_log.error else True
    #             new_result.aggregatorMessages = agg_log.messages
    #             new_result.pipelineMessages = pipe_log.messages
    #             results.append(new_result.__dict__)
    #             break
    #
    #     if not match_flag:
    #         new_result = SearchResult
    #         new_result.offset = pipe_log.offset
    #         new_result.sentTime = pipe_log.timestamp
    #         new_result.pipelineMessages = pipe_log.messages
    #         new_result.consumed = False
    #         results.append(new_result)

    return results


# @param path_to_log: log file to read the
# logs from
# @param offset: the offset to search by
# @return value: list of ConsumedGroups found in log file that match the given offset
def get_results_by_offset(path_to_log, offset, pipeline=True):
    matches = []
    if pipeline:
        groups = pipelinescript.get_log_items(path_to_log)
    else:
        groups = aggregatorscript.get_groups(path_to_log)

    for g in groups:
        if g.offset == offset:
            matches.append(g)

    if len(matches) > 0:
        return matches
    else:
        return


# @param matches: list of matches from the other search methods
# @return value: returns the matches in SearchResult format
def get_offset_search_objects(matches: list):
    # create a search result Object for each match
    search_results = []
    for match in matches:
        new_result = SearchResult()
        new_result.timestamp = match.timestamp  # location of timestamp in ConsumedGrouping
        new_result.description = [[message[0], message[1]] for message in match.messages]
        new_result.offset = match.offset

        # status = found if there's no error, status = error if there's an error
        new_result.status = "found" if 'error' not in (message[0] for message in match.messages) else "error"
        search_results.append(new_result)
    return search_results


if __name__ == "__main__":
    test_results = search_by_offset("../logs/", 28)
    print(test_results[0])
