import json

# Object which holds all information about each log with its messages
class LogItem:
    def _init_(self):
        self.orgId = -1
        self.clusterName = ''
        self.partition = -1
        self.offset = -1
        self.isError = False
        self.isWarning = False
        self.messages = {}

# Converts each line of log file to json format
def get_log_list(file):
    log_list = [{}]

    for line in open(file, encoding='utf-8'):
        log_list.append(json.loads(line))

    return log_list[2:]


logItems = []
logs = get_log_list("logs/pipeline.log")
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
            msgArr.append(p['message'])

print(logItems)
        
