import json

# Class to hold desired information about each log
class LogItem:
    def _init_(self):
        self.orgId = -1
        self.clusterName = ''
        self.partition = -1
        self.offset = -1
        self.isError = False
        self.isWarning = False
        self.messages = {}

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
# @return an array of LogItem objects
def get_log_items(pipeline_path):
    logItems = []
    logs = get_log_list(pipeline_path)
    isError = False
    isWarning = False
    msgArr = []
    # Loop through each line of the logs and grab desired information
    for i in range(len(logs)):
        # Beginning of a chunk of data, so save its message
        if logs[i]['levelname'] == "INFO":
            if len(msgArr) == 0:
                msgArr.append(logs[i]['message'])
            else:
                del msgArr[:]
        else:
            # If at the end of the chunk, save orgId, cluster name, partition, and offset
            if logs[i]['message'].count("OrgId") == 1:
                message = logs[i]['message'].split(',')
                item = LogItem()
                item.orgId = message[0][message[0].find("=")+1:]
                item.clusterName = message[0][message[0].find("=")+2:-1]
                item.partition = msgArr[0][msgArr[0].find("Partition")+11:msgArr[0].find("Offset")-3]
                item.offset = msgArr[0][msgArr[0].find("Offset")+8:msgArr[0].find("LastChecked")-3]
                item.messages = msgArr
                item.isError = isError
                item.isWarning = isWarning
                logItems.append(item)
                del msgArr[:]
                del item
                isError = False
                isWarning = False
            # Check if the log is a warning or an error
            else:
                if logs[i]['levelname'] == "ERROR":
                    isError = True
                elif logs[i]['levelname'] == "WARNING":
                    isWarning = True
                msgArr.append(logs[i]['message'])
    return logItems


logs = get_log_items("logs/pipeline.log")
for i in range(len(logs)):
    print(logs[i].orgId, logs[i].clusterName, logs[i].messages)
        
