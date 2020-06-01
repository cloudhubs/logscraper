from src import aggregatorscript, pipelinescript
import sys



# @param path_to_log: log file to read the logs from
# @param organization: the organization id to search by
# @param cluster_id: the id of the cluster to search by
# @return value: list of ConsumedGroups found in log file that match the given organization and cluster_id
def get_results_by_org_cluster(path_to_log, organization, cluster_id):
    groups = aggregatorscript.get_groups(path_to_log)
    groups.extend(pipelinescript.get_log_items(path_to_log))
    cluster_matches = [group for group in groups if group.cluster_id == cluster_id]
    matches = [matches for matches in cluster_matches if matches.organization == organization]
