import json


# Class to hold desired information about each log
class LogItem:
    organization = -1
    cluster_id = ""
    partition = -1
    offset = -1
    timestamp = ""
    error = False
    warning = False
    messages = []

    def __init__(self):
        self.organization = -1
        self.cluster_id = ""
        self.partition = -1
        self.offset = -1
        self.timestamp = ""
        self.error = False
        self.warning = False
        self.messages = []


# Verify if a line is in JSON format
# @params myjson: the object to verify json form of
# @return boolean indicating whether the object can be parsed into json
def is_json(myjson):
    try:
        json_object = json.loads(myjson)
    except ValueError as e:
        return False
    return True


# Extracts lines from log file in json format
# @params pipeline_path: the file to extract the logs from
# @return array of all the logs as json objects
def get_log_list(pipeline_path):
    log_list = [{}]

    for line in open(pipeline_path, encoding='utf-8'):
        if is_json(line):
            log_list.append(json.loads(line))

    return log_list[2:]


# Reads through logs and assembles an array of LogItem objects
# @params pipeline_path: the path to the pipeline log
# @return an array of LogItem objects as JSON objects
def get_log_items(pipeline_path):
    logItems = []
    logs = get_log_list(pipeline_path)
    is_error = False
    is_warning = False
    msg_arr = []
    partition = -1
    offset = -1
    # Loop through each line of the logs and grab desired information
    for i in range(len(logs)):
        # Beginning of a chunk of data, so save its message
        if logs[i]['levelname'] == "INFO":
            # If the previous block had messages associated with it
            if len(msg_arr) > 1:
                item = LogItem()
                # Check for orgId and clusterName
                if "OrgId" in logs[i - 1]['message'] or "ClusterName" in logs[i - 1]['message']:
                    message = logs[i - 1]['message'].split(',')
                    for h in range(len(message)):
                        if "OrgId" in message[h]:
                            item.organization = int(message[h][message[h].find("=") + 1:])
                        elif "ClusterName" in message[h]:
                            item.cluster_id = message[h][message[h].find("=") + 2:-1]

                # check for partition and offset
                if "Partition" in msg_arr[0] or "Offset" in msg_arr[0]:
                    message = msg_arr[0].split(';')
                    for k in range(len(message)):
                        if "Partition" in message[k]:
                            partition = int(message[k][12:])
                        elif "Offset" in message[k]:
                            offset = message[k][9:]
                item.partition = partition
                item.offset = offset

                # Assign message array and error/warning members
                item.messages = [None] * len(msg_arr)
                for j in range(len(msg_arr)):
                    item.messages[j] = msg_arr[j]
                item.error = is_error
                item.warning = is_warning
                item.timestamp = logs[i - 1]['asctime']
                logItems.append(item)
                del item
                is_error = False
                is_warning = False
                partition = -1
                offset = -1
            del msg_arr[:]
        else:
            if "ERROR" in logs[i]['levelname']:
                is_error = True
            elif "WARNING" in logs[i]['levelname']:
                is_warning = True
        msg_arr.append(logs[i]['message'])

    # Append final log item
    if len(msg_arr) > 1:
        item = LogItem()
        # Check for orgId and clusterName
        if "OrgId" in logs[len(logs) - 1]['message'] or "ClusterName" in logs[len(logs) - 1]['message']:
            message = logs[len(logs) - 1]['message'].split(',')
            for h in range(len(message)):
                if "OrgId" in message[i]:
                    item.organization = int(message[h][message[h].find("=") + 1:])
                elif "ClusterName" in message[h]:
                    item.cluster_id = message[h][message[h].find("=") + 2:-1]
        # check for partition and offset
        if "Partition" in msg_arr[0] or "Offset" in msg_arr[0]:
            message = msg_arr[0].split(';')
            print(message)
            for j in range(len(message)):
                if "Partition" in message[j]:
                    item.partition = int(message[j][11:])
                elif "Offset" in message[i]:
                    item.offset = message[j][9:]

        # Assign message array and error/warning members
        item.messages = [None] * len(msg_arr)
        for i in range(len(msg_arr)):
            item.messages[i] = msg_arr[i]
        item.error = is_error
        item.warning = is_warning
        item.timestamp = logs[i - 1]['asctime']
        logItems.append(item)
        del msg_arr[:]
        del item
    return logItems


# Gets the key for a sorting algorithm
# @params the LogItem to return the offset from
# @return the offset of the logItem
def get_offset(log_item):
    return log_item.offset


# Groups the logItem chunks together based on offset
# @param the path to the pipeline log file
# @return a list of the new merged chunks
def get_chunks(path):
    log_items = get_log_items(path)
    log_items.sort(key=get_offset)

    chunks = [log_items[0]]
    temp_cluster = log_items[0].cluster_id
    chunks[0].cluster_id = []
    chunks[0].cluster_id.append(temp_cluster)
    index = 0
    # Iterates through each LogItem and creates chunks based on matching offsets
    for i in range(1, len(log_items)):
        if chunks[index].offset == log_items[i].offset:
            temp_cluster
            if len(log_items[i].cluster_id) > 0:
                chunks[index].cluster_id.append(log_items[i].cluster_id)

            for j in range(len(log_items[i].offset)):
                chunks[index].messages.append(log_items[i].messages[j])

            if log_items[i].error:
                chunks[index].error = True
            if log_items[i].warning:
                chunks[index].warning = True
        else:
            index += 1
            chunks.append(log_items[i])
            temp_cluster = log_items[i].cluster_id
            chunks[index].cluster_id = []
            chunks[index].cluster_id.append(temp_cluster)
    json_logs = []
    for h in range(len(chunks)):
        json_logs.append(chunks[h].__dict__)
    return json_logs
