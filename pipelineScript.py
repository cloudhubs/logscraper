import json


# Class to hold desired information about each log
class LogItem:
<<<<<<< HEAD
    orgId = ""
    clusterName = []
    partition = ""
    offset = ""
    timestamp = ""
    error = False
    warning = False
    messages = []
    def _init_(self):
        self.orgId = ""
        self.clusterName = []
        self.partition = ""
        self.offset = ""
        self.timestamp = ""
        self.error = False
        self.warning = False
        self.messages = []
=======

    def _init_(self):
        orgId = -1
        clusterName = ""
        partition = -1
        offset = -1
        timestamp = ""
        error = False
        warning = False
        messages = []

>>>>>>> origin/poc1


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
    isError = False
    isWarning = False
    msgArr = []
<<<<<<< HEAD
    partition = ""
    offset = ""
=======
    partition = -1
    offset = -1
>>>>>>> origin/poc1
    info_message = ""
    # Loop through each line of the logs and grab desired information
    for i in range(len(logs)):
        # Beginning of a chunk of data, so save its message
        if logs[i]['levelname'] == "INFO":
            # If the previous block had messages associated with it
            if len(msgArr) > 1:
                item = LogItem()
                # Check for orgId and clusterName
                if "OrgId" in logs[i - 1]['message'] or "ClusterName" in logs[i - 1]['message']:
                    message = logs[i - 1]['message'].split(',')
                    for h in range(len(message)):
                        if "OrgId" in message[h]:
                            item.orgId = message[h][message[h].find("=") + 1:]
                        elif "ClusterName" in message[h]:
<<<<<<< HEAD
                            item.clusterName.append(message[h][message[h].find("=") + 2:-1])
=======
                            item.clusterName = message[h][message[h].find("=") + 2:-1]
>>>>>>> origin/poc1

                # check for partition and offset
                if "Partition" in msgArr[0] or "Offset" in msgArr[0]:
                    message = msgArr[0].split(';')
                    for k in range(len(message)):
                        if "Partition" in message[k]:
                            partition = message[k][12:]
                        elif "Offset" in message[k]:
                            offset = message[k][9:]
                item.partition = partition
                item.offset = offset

                # Assign message array and error/warning members
                item.messages = [None] * len(msgArr)
                for j in range(len(msgArr)):
                    item.messages[j] = msgArr[j]
                item.error = isError
                item.warning = isWarning
                item.timestamp = logs[i - 1]['asctime']
                logItems.append(item)
                del item
                isError = False
                isWarning = False
<<<<<<< HEAD
                partition = ""
                offset = ""
=======
                partition = -1
                offset = -1
>>>>>>> origin/poc1
            del msgArr[:]
        else:
            if "ERROR" in logs[i]['levelname']:
                isError = True
            elif "WARNING" in logs[i]['levelname']:
                isWarning = True
        msgArr.append(logs[i]['message'])

    # Append final log item
    if len(msgArr) > 1:
        item = LogItem()
        # Check for orgId and clusterName
        if "OrgId" in logs[len(logs) - 1]['message'] or "ClusterName" in logs[len(logs) - 1]['message']:
            message = logs[len(logs) - 1]['message'].split(',')
            for h in range(len(message)):
                if "OrgId" in message[i]:
                    item.orgId = message[h][message[h].find("=") + 1:]
                elif "ClusterName" in message[h]:
                    item.clusterName = message[h][message[h].find("=") + 2:-1]
        # check for partition and offset
        if "Partition" in msgArr[0] or "Offset" in msgArr[0]:
            message = msgArr[0].split(';')
            print(message)
            for j in range(len(message)):
                if "Partition" in message[j]:
                    item.partition = message[j][11:]
                elif "Offset" in message[i]:
                    item.offset = message[j][9:]

        # Assign message array and error/warning members
        item.messages = [None] * len(msgArr)
        for i in range(len(msgArr)):
            item.messages[i] = msgArr[i]
        item.error = isError
        item.warning = isWarning
        item.timestamp = logs[i - 1]['asctime']
        logItems.append(item)
        del msgArr[:]
        del item
        isError = False
        isWarning = False
        partition = -1
        offset = -1

    # Convert objects to json objects
<<<<<<< HEAD
    #json_logs = []
    #for i in range(len(logItems)):
     #   json_logs.append(logItems[i].__dict__)
    return logItems

def get_offset(logItem):
    return logItem.offset

def get_chunks(path):
    logItems = get_log_items(path)
    logItems.sort(key=get_offset)

    chunks = []
    chunks.append(logItems[0])
    index = 0
    for i in range(1, len(logItems)):
        if chunks[index].offset == logItems[i].offset:
            chunks[index].clusterName.append(logItems[i].clusterName[0])
            #chunks[index].orgId.append(logItems[i].orgId[0])

            for j in range(len(logItems[i].offset)):
                chunks[index].messages.append(logItems[i].messages[j])

            if logItems[i].error:
                chunks[index].error = True
            if logItems[i].warning:
                chunks[index].warning = True
        else:
            index += 1
            chunks.append(logItems[i])
    json_logs = []
    for h in range(len(chunks)):
        json_logs.append(chunks[h].__dict__)
    return json_logs

# If you want to test with command line, input file name here
logs = get_chunks("logs/pipeline.log")

for i in range(len(logs)):
     print(logs[i]['offset'], logs[i]['messages'])
=======
    json_logs = []
    for i in range(len(logItems)):
        json_logs.append(logItems[i].__dict__)
    return json_logs


# If you want to test with command line, input file name here
# logs = get_log_items("logs/pipeline.log")
# for i in range(len(logs)):
#     print(logs[i])
>>>>>>> origin/poc1
