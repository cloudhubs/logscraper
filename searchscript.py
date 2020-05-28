import aggregatorscript


# @param path_to_log: log file to read the logs from
# @param offset: the offset to search by
# @return value: list of ConsumedGroups found in log file that match the given offset
def search_by_offset(path_to_log, offset):
    groups = aggregatorscript.get_groups(path_to_log)
    return [group for group in groups if group.offset == offset]


# @param path_to_log: log file to read the logs from
# @param organization: the organization id to search by
# @param cluster_id: the id of the cluster to search by
# @return value: list of ConsumedGroups found in log file that match the given organization and cluster_id
def search_by_org_cluster(path_to_log, organization, cluster_id):
    groups = aggregatorscript.get_groups(path_to_log)
    cluster_matches = [group for group in groups if group.cluster_id == cluster_id]
    return [matches for matches in cluster_matches if matches.organization == organization]


if __name__ == "__main__":
    results = search_by_offset("logs/aggregator.log", 1055)
    for result in results:
        print(result.offset)

    results = search_by_org_cluster("logs/aggregator.log", 11789772, "92c04d4a-9f4c-441d-9df9-1e50c426df11")
    for result in results:
        print(result.cluster_id)
