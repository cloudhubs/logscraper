import json

class LogItem:
    orgId = -1
    clusterName = ''
    partition = -1
    offset = -1
    messages = {}
    

logItems = []

with open(input(), encoding='utf-8') as json_file:
    json_data = json.load(json_file)

msgArr = []
for p in json_data:
    if p['levelname'] == 'INFO':
        if len(msgArr) == 0:
            msgArr.append(p['message'])
        else:
            del msgArr[:]
    else:
        if p['message'].count('OrgId') == 1:
            message = p['message'].split(',')
            item = LogItem()
            item.orgId = message[0][message[0].find('=')+1:]
            item.clusterName = message[0][message[0].find('=')+2:-1]
            item.partition = msgArr[0][msgArr[0].find('Partition')+11:msgArr[0].find('Offset')-3]
            item.offset = msgArr[0][msgArr[0].find('Offset')+8:msgArr[0].find('LastChecked')-3]
            item.messages = msgArr
            logItems.append(item)
            del msgArr[:]
            del item
        else:
            msgArr.append(p['message'])

print(logItems)
        
