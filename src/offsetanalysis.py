import fnmatch
import os
import aggregatorscript
import pipelinescript


# This script will return the resulting offsets found in a given directory of logs
# All results found in either the pipeline or aggregate logs will be returned to the caller

class SearchResult:
    def __init__(self):
        self.offset = None
        self.timestamp = None
        self.status = None
        self.description = []
        self.filetype = None

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
            while i < len(pipe_log):
                if pipe_log[i].offset == offset:
                    new_result = SearchResult()
                    new_result.offset = pipe_log[i].offset
                    new_result.timestamp = pipe_log[i].timestamp
                    new_result.description = pipe_log[i].messages
                    if pipe_log[i].warning is True:
                        new_result.status = "Warning"
                    elif pipe_log[i].error is True:
                        new_result.status = "Error"
                    else:
                        new_result.status = "No Error Detected"
                    new_result.filetype = "Pipeline"
                    results.append(new_result.__dict__)
                if i < len(pipe_log):
                    i += 1
                    print(i)
        for agg_log in aggregator_logs:
            j = 0
            while j < len(agg_log):
                if agg_log[j].offset == offset:
                    new_result = SearchResult()
                    new_result.offset = agg_log[j].offset
                    new_result.timestamp = agg_log[j].timestamp
                    new_result.description = agg_log[j].messages
                    new_result.filetype = "Aggregate"
                    if agg_log[j].error is True:
                        new_result.status = "Error"
                    else:
                        new_result.status = "No Error Detected"
                    results.append(new_result.__dict__)

                if j < len(agg_log):
                    j += 1

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
