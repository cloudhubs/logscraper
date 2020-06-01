import fnmatch
import os
import aggregatorscript
import pipelinescript
from offsetanalysis import SearchResult


def search_by_org_cluster(log_dir, organization, cluster_id):
    aggregator_logs = []
    pipeline_logs = []
    results = []
    for file in os.listdir(log_dir):
        full_path = os.path.join(log_dir, file)
        if fnmatch.fnmatch(full_path, '*/aggregator*.log'):
            aggregator_logs.extend(get_results_by_org_cluster(full_path, organization, cluster_id, False))
        elif fnmatch.fnmatch(full_path, '*/pipeline*.log'):
            pipeline_logs.extend(get_results_by_org_cluster(full_path, organization, cluster_id, True))

    for pipe_log in pipeline_logs:
        match_flag = False
        for agg_log in aggregator_logs:
            if (int(pipe_log.organization) == int(agg_log.organization) and int(pipe_log.organization) == organization)\
                    and (pipe_log.cluster_id == agg_log.cluster_id and pipe_log.cluster_id == cluster_id):
                match_flag = True
                new_result = SearchResult()
                new_result.offset = pipe_log.offset
                new_result.consumeTime = agg_log.timestamp
                new_result.sentTime = pipe_log.timestamp
                new_result.consumed = False if agg_log.error else True
                new_result.aggregatorMessages = agg_log.messages
                new_result.pipelineMessages = pipe_log.messages
                results.append(new_result)
                break

        if not match_flag:
            new_result = SearchResult
            new_result.offset = pipe_log.offset
            new_result.sentTime = pipe_log.timestamp
            new_result.pipelineMessages = pipe_log.messages
            new_result.consumed = False
            results.append(new_result)

    return results


# @param path_to_log: log file to read the logs from
# @param organization: the organization id to search by
# @param cluster_id: the id of the cluster to search by
# @return value: list of ConsumedGroups found in log file that match the given organization and cluster_id
def get_results_by_org_cluster(path_to_log, organization, cluster_id, pipeline=True):
    if pipeline:
        groups = pipelinescript.get_log_items(path_to_log)
    else:
        groups = aggregatorscript.get_groups(path_to_log)
    cluster_matches = [group for group in groups if
                       group.cluster_id is not None and group.cluster_id]
    matches = [matches for matches in cluster_matches if
               matches.organization is not None and int(matches.organization) == organization]
    return matches


if __name__ == "__main__":

    test_results = search_by_org_cluster("../logs/", 11789772, "0382553e-815b-4bc6-b452-dd129c9610c3")
    print(test_results[0])
