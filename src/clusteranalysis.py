import fnmatch
import os
import aggregatorscript
import pipelinescript

# This script will return the resulting cluster and org id's found in a given directory of logs
# All results found in either the pipeline or aggregate logs will be returned to the caller


class SearchResult:
    def __init__(self):
        self.clusterid = None
        self.orgid = None
        self.timestamp = None
        self.status = None
        self.description = []
        self.filetype = None



def search_by_org_cluster(log_dir, organization, cluster_id):
    aggregator_logs = []
    pipeline_logs = []
    results = []
    for file in os.listdir(log_dir):
        full_path = os.path.join(log_dir, file)

        if fnmatch.fnmatch(full_path, '*/aggregator*.log'):
            holder = get_results_by_org_cluster(full_path, organization, cluster_id, False)
            if holder is not None:
                aggregator_logs.append(holder)
        elif fnmatch.fnmatch(full_path, '*/pipeline*.log'):
            holder = get_results_by_org_cluster(full_path, organization, cluster_id, True)
            if holder is not None:
                pipeline_logs.append(holder)


    if pipeline_logs is not None and aggregator_logs is not None:
        for pipe_log in pipeline_logs:
            i = 0
            while i < len(pipe_log):
                if pipe_log[i].organization == int(organization) and pipe_log[i].cluster_id == cluster_id:
                    new_result = SearchResult()
                    new_result.clusterid = pipe_log[i].cluster_id
                    new_result.orgid = pipe_log[i].organization
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
        for agg_log in aggregator_logs:
            j = 0
            while j < len(agg_log):
                if agg_log[j].organization == int(organization) and agg_log[j].cluster_id == cluster_id:
                    new_result = SearchResult()
                    new_result.clusterid = agg_log[j].cluster_id
                    new_result.orgid = agg_log[j].organization
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

# @param path_to_log: log file to read the logs from
# @param organization: the organization id to search by
# @param cluster_id: the id of the cluster to search by
# @return value: list of ConsumedGroups found in log file that match the given organization and cluster_id
def get_results_by_org_cluster(path_to_log, organization, cluster_id, pipeline=True):
    matches = []
    if pipeline:
        groups = pipelinescript.get_log_items(path_to_log)
    else:
        groups = aggregatorscript.get_groups(path_to_log)

    for g in groups:
        if g.cluster_id == cluster_id:
            matches.append(g)


    if len(matches) > 0:
        return matches
    else:
        return


if __name__ == "__main__":

    test_results = search_by_org_cluster("../logs/", 11789772, "0382553e-815b-4bc6-b452-dd129c9610c3")
    print(test_results[0])
