import aggregatorscript, pipelinescript


class SearchResult:
    def __init__(self):
        self.timestamp = None
        self.status = None
        self.description = []


# @param path_to_log: log file to read the logs from
# @param offset: the offset to search by
# @return value: list of ConsumedGroups found in log file that match the given offset
def search_by_offset(cls, path_to_log, offset):
    groups = aggregatorscript.get_groups(path_to_log)
    groups.extend(pipelinescript.get_log_items(path_to_log))
    matches = [group for group in groups if group.offset == offset]

    return get_search_objects(matches)


# @param path_to_log: log file to read the logs from
# @param organization: the organization id to search by
# @param cluster_id: the id of the cluster to search by
# @return value: list of ConsumedGroups found in log file that match the given organization and cluster_id
def search_by_org_cluster(cls, path_to_log, organization, cluster_id):
    groups = aggregatorscript.get_groups(path_to_log)
    groups.extend(pipelinescript.get_log_items(path_to_log))
    cluster_matches = [group for group in groups if group.cluster_id == cluster_id]
    matches = [matches for matches in cluster_matches if matches.organization == organization]

    return get_search_objects(matches)


# @param matches: list of matches from the other search methods
# @return value: returns the matches in SearchResult format
def get_search_objects(matches: list):
    # create a search result Object for each match
    search_results = []
    for match in matches:
        new_result = SearchResult()
        new_result.timestamp = match.timestamp  # location of timestamp in ConsumedGrouping
        new_result.description = [[message[0], message[1]] for message in match.messages]

        # status = found if there's no error, status = error if there's an error
        new_result.status = "found" if 'error' not in (message[0] for message in match.messages) else "error"
        search_results.append(new_result)
    return search_results


if __name__ == "__main__":
    results = search_by_offset("logs/aggregator.log", 1056)
    for result in results:
        print(result.status)

    results = search_by_org_cluster("logs/aggregator.log", 12769894, "11de562e-76f6-477d-92a6-817a1b256bee")
    for result in results:
        print(result.status)
