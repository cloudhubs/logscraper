import itertools
import json
import sys
from json import JSONDecodeError


class ConsumedGrouping:

    def __init__(self):
        # messages is a list of list, in the format [level, timestamp, message]
        self.messages = []
        self.organization = None
        self.cluster_id = None
        self.error = False  # flag set to true if an error occured in this group


# @param file: path to the log file to decode and return
# @return value: list of decoded log records
def get_log_list(file):
    """Open a log file, decode the JSON, and export the log records"""
    log_list = [{}]

    with open(file, encoding='utf-8') as f:
        for line in itertools.islice(f, 10, None):
            try:
                log_list.append(json.loads(line))
            except JSONDecodeError as e:
                print("Error: Log record contains illegal, unescaped quotation mark ", e.msg, str(e))

    return log_list[1:]


# @param logs: list of log records decoded from the JSON
# @return value: list of a ConsumedGroupings, which contain the messages, timestamps, and other info for a group
def group_consumed_logs(logs):
    """Group logs based on the format of 'Consumed'"""
    groupings = []
    processing_group = False
    current_offset = 0
    current_group = None

    for record in logs:
        # make sure each JSON has an associated message
        if 'message' not in record:
            raise Exception("Error: log with no message")

        if not processing_group and record['message'] == "Consumed":
            # new group
            processing_group = True
            current_offset = record['offset']
            current_group = ConsumedGrouping()
            current_group.messages.append([record['level'], record['time'], record['message']])
        elif not processing_group:
            # message not apart of a group, move on
            continue
        else:
            # in the middle of grouping a ConsumedGrouping, determine if offset is same
            if 'offset' in record and record['offset'] == current_offset:
                # add organization and cluster if not found yet
                if record['level'] == "info":
                    current_group.cluster_id = record['cluster']
                    current_group.organization = record['organization']
                    current_group.messages.append([record['level'], record['time'], record['message']])
                elif record['level'] == "error":
                    # add error
                    current_group.error = True
                    current_group.messages.append([record['level'], record['time'], record['message']])
            elif 'offset' not in record and not record['message'].startswith("Request URI"):
                if record['level'] == "error":
                    current_group.error = True
                    current_group.messages.append([record['level'], record['time'], record['error']])
                    current_group.messages.append([record['level'], record['time'], record['message']])
                else:
                    current_group.messages.append([record['level'], record['time'], record['message']])
            else:
                # end of group
                processing_group = False
                groupings.append(current_group)
                current_group = None
                if record['message'] == "Consumed":
                    # new group
                    current_offset = record['offset']
                    current_group = ConsumedGrouping()
                    processing_group = True

    # add last group to groupings
    if current_group is not None:
        groupings.append(current_group)

    return groupings


# @param logs: list of log records decoded from the JSON
# @return value: list of a ConsumedGroupings, which contain the messages, timestamps, and other info for a group
def group_consumed_offset_logs(logs):
    """Group logs based on the format of 'Consumed Log Offset'"""
    groupings = []
    processing_group = False
    current_group = None
    local_record_index = 0  # for keeping track of how many records since first in a group

    for record in logs:
        # make sure each JSON has an associated message
        if 'message' not in record:
            raise Exception("Error: log with no message")

        if not processing_group and record['message'].startswith("Consumed message offset"):
            current_group = ConsumedGrouping()
            processing_group = True
            local_record_index = 1
            current_group.messages.append([record['level'], record['time'], record['message']])
        elif not processing_group:
            # record not apart of a group, move on
            continue
        else:
            # in the middle of grouping a ConsumedMessageOffset
            if local_record_index == 1 and record['level'] == "debug":
                # extract organization and cluster from message
                message_details = record['message'].split(" ")
                current_group.organization = message_details[3]
                current_group.cluster_id = message_details[6]
                current_group.messages.append([record['level'], record['time'], record['message']])
                local_record_index += 1
            elif local_record_index >= 1 and record['level'] == "error":
                # add error to the grouping
                current_group.error = True
                current_group.messages.append([record['level'], record['time'], record['error']])
                current_group.messages.append([record['level'], record['time'], record['message']])
                local_record_index += 1
            else:
                # either regular log or end of group
                if record['message'].startswith("Request URI:") or record['message'].startswith("Consumed"):
                    # end of group
                    groupings.append(current_group)
                    if record['message'].startswith("Consumed"):
                        local_record_index = 1
                        current_group = ConsumedGrouping()
                        current_group.messages.append([record['level'], record['time'], record['message']])
                    else:
                        current_group = None
                        processing_group = False
                        local_record_index = 0
                else:
                    # regular message of group
                    if 'time' in record:
                        current_group.messages.append([record['level'], record['time'], record['message']])
                    else:
                        current_group.messages.append([record['level'], [], record['message']])
                    local_record_index += 1

    # add last group to groupings
    if current_group is not None:
        groupings.append(current_group)

    return groupings


# @return value: returns a list of 1-2 ConsumedGroups. One group is based on Consumed, and the other Consumed Offset
def get_groups():
    """Take an aggregator log file as input, produce groupings, and print/return"""

    if len(sys.argv) < 2:
        print("Error: Please provide a path to a file")
        sys.exit(1)

    log_file = sys.argv[1]
    logs = get_log_list(log_file)

    # get log groupings for the two types
    groups = [group_consumed_logs(logs), group_consumed_offset_logs(logs)]

    for group_category in groups:
        if len(group_category) == 0:
            groups.remove(group_category)

    for group_category in groups:
        for group in group_category:
            print(group.messages)

    return groups


if __name__ == "__main__":
    get_groups()
