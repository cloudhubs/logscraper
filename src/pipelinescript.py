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


# Convert anonymized pipeline logs to compatible JSON format
# @params path is the path to the pipeline log file
# @return a list of logs in a compatible JSON format
def plain_to_json(path):
    """Turns file of plain text pipeline logs to a list of logs in JSON format"""
    log_statements = []
    next_line_check = False  # for determining whether to check for multi-line errors
    json_dict = {}
    for line in open(path, encoding='utf-8'):
        split_line = line.split(" ")
        if next_line_check:
            if split_line[0].startswith("2020"):
                # end of message, continue with new log statement
                next_line_check = False
                log_statements.append(json_dict)
            else:
                json_dict['message'] += (" " + line)
                continue
        json_dict = {}
        json_dict['asctime'] = split_line[0] + split_line[1]
        json_dict['levelname'] = split_line[2]
        json_dict['filename'] = split_line[4]
        json_dict['message'] = ""
        for message_text in split_line[6:]:
            json_dict['message'] += (message_text + " ")

        if json_dict['levelname'] == 'WARNING' or json_dict['levelname'] == 'ERROR':
            next_line_check = True
        else:
            log_statements.append(json_dict)

    return log_statements


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
    plain_text = False

    # Determine if only the first line of the file was not json
    index = 0
    for line in open(pipeline_path, encoding='utf-8'):
        if is_json(line):
            log_list.append(json.loads(line))
            index = 1
        else:
            # If more than one line is not json, then it is anonymized
            if index > 0:
                plain_text = True
                break
            index = 1
        if plain_text:
            break

    if plain_text:
        return plain_to_json(pipeline_path)

    return log_list[2:]


# Reads through logs and assembles an array of LogItem objects
# @params pipeline_path: the path to the pipeline log
# @return an array of LogItem objects as JSON objects
def get_log_items(pipeline_path):
    log_items = []
    # Returns a list of logs from either file type
    logs = get_log_list(pipeline_path)
    is_error = False
    is_warning = False
    msg_arr = []
    partition = -1
    offset = -1

    for i in range(len(logs)):
        # Beginning of the log chunk
        if "Offset" in logs[i]['message'] or "Partition" in logs[i]['message']:
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
                # Assign message array and error/warning members
                item.messages = [None] * len(msg_arr)
                for j in range(len(msg_arr)):
                    item.messages[j] = msg_arr[j]

                item.timestamp = logs[i - 1]['asctime']
                item.error = is_error
                item.warning = is_warning
                item.partition = partition
                item.offset = offset
                log_items.append(item)
                del item
                is_error = False
                is_warning = False
                partition = -1
                offset = -1
            del msg_arr[:]

            msg_arr.append(logs[i]['message'])
            if "Partition" in msg_arr[0] or "Offset" in msg_arr[0]:
                message = msg_arr[0].split(';')
                for k in range(len(message)):
                    if "Partition:" in message[k]:
                        partition = int(message[k][12:])
                    elif "Offset:" in message[k]:
                        offset = message[k][9:]
                        print(offset)

        else:
            if "ERROR" in logs[i]['levelname']:
                is_error = True
            elif "WARNING" in logs[i]['levelname']:
                is_warning = True
        msg_arr.append(logs[i]['message'])

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
        # Assign message array and error/warning members
        item.messages = [None] * len(msg_arr)
        for j in range(len(msg_arr)):
            item.messages[j] = msg_arr[j]

        item.timestamp = logs[i - 1]['asctime']
        item.error = is_error
        item.warning = is_warning
        item.partition = partition
        item.offset = offset
        log_items.append(item)
        del item
        is_error = False
        is_warning = False
        partition = -1
        offset = -1

    return log_items


# Gets the key for a sorting algorithm
# @params the LogItem to return the offset from
# @return the offset of the logItem
def get_offset(log_item):
    return int(log_item.offset)


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
        # If the offsets match, combine the two log items
        if chunks[index].offset == log_items[i].offset:
            temp_cluster
            if len(log_items[i].cluster_id) > 0:
                chunks[index].cluster_id.append(log_items[i].cluster_id)

            for j in range(len(log_items[i].messages)):
                chunks[index].messages.append(log_items[i].messages[j])

            if log_items[i].error:
                chunks[index].error = True
            if log_items[i].warning:
                chunks[index].warning = True
        # Add the log item to the list since it had no duplicates
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


if __name__ == "__main__":
    groups = get_chunks("../logs/from_prod_anonymized/ccx_data_pipeline_1_anonymized.log")
    print(len(groups))
