import json

file_name = "logs/aggregator.log"


class ConsumedGrouping:

    def __init__(self):
        self.messages = []
        self.organization = None
        self.cluster_id = None
        self.error = False


def get_log_list(file):
    log_list = [{}]

    for line in open(file, encoding='utf-8'):
        log_list.append(json.loads(line))

    return log_list[1:]

# Problems: Can't get Organization/Clustering ID for some errors
def group_consumed_logs(logs):
    groupings = []
    processing_group = False
    current_offset = 0
    current_group = None

    for i in range(len(logs)):
        # make sure each JSON has an associated message
        if 'message' not in logs[i]:
            raise Exception("Error: log with no message")

        if not processing_group and logs[i]['message'] == "Consumed":
            # new group
            processing_group = True
            current_offset = logs[i]['offset']
            current_group = ConsumedGrouping()
        elif not processing_group:
            # message not apart of a group, move on
            continue
        else:
            # in the middle of grouping, determine if offset is same
            if 'offset' in logs[i] and logs[i]['offset'] == current_offset:
                # add organization and cluster if not found yet
                if logs[i]['level'] == "info":
                    current_group.cluster_id = logs[i]['cluster']
                    current_group.organization = logs[i]['organization']
                    current_group.messages.append("info: " + logs[i]['message'])
                elif logs[i]['level'] == "error":
                    # add error
                    current_group.error = True
                    current_group.messages.append("error: " + logs[i]['message'])
            elif 'offset' not in logs[i] and not logs[i]['message'].startswith("Request URI"):
                if logs[i]['level'] == "error":
                    current_group.error = True
                    current_group.messages.append("error: " + logs[i]['error'])
                    current_group.messages.append(logs[i]['message'])
                else:
                    current_group.messages.append(logs[i]['level'] + ": " + logs[i]['message'])
            else:
                # end of group
                processing_group = False
                groupings.append(current_group)
                if logs[i]['message'] == "Consumed":
                    # new group
                    current_offset = logs[i]['offset']
                    current_group = ConsumedGrouping()
                    processing_group = True


    return groupings




if __name__ == "__main__":
    logs = get_log_list(file_name)

    groups = group_consumed_logs(logs)

    for group in groups:
        print(group.organization, group.messages)
