import json

# Class to hold desired information about each log
class LogItem:

    def _init_(self):
        orgId = -1
        clusterName = ""
        partition = -1
        offset = -1
        timestamp = ""
        isError = False
        isWarning = False
        messages = []


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
    # Loop through each line of the logs and grab desired information
    for i in range(len(logs)):
        # Beginning of a chunk of data, so save its message
        if logs[i]['levelname'] == "INFO":
            del msgArr[:]
            msgArr.append(logs[i]['message'])
        else:
            # If at the end of the chunk, save orgId, cluster name, partition, and offset
            if "OrgId" in logs[i]['message']:
                message = logs[i]['message'].split(',')
                item = LogItem()
                item.orgId = message[0][message[0].find("=")+1:]
                item.clusterName = message[1][message[1].find("=")+2:-1]

                message = msgArr[0].split(';')
                if "Partition" in msgArr[0]:
                    item.partition = message[2][11:]
                if "Offset" in msgArr[0]:
                    item.offset = message[3][9:]

                item.messages = [None]*len(msgArr)
                for i in range(len(msgArr)):
                    item.messages[i]= msgArr[i]
                item.isError = isError
                item.isWarning = isWarning
                item.timestamp = logs[i]['asctime']
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
    #Convert objects to json objects
    json_logs = []
    for i in range(len(logItems)):
        json_logs.append(json.dumps(logItems[i].__dict__, indent = 4))
    return json_logs


# If you want to test with command line, input file name here
#logs = get_log_items(input())
#for i in range(len(logs)):
#    print(logs[i])
        
