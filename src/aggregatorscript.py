import itertools
import json
import os
import sys
from json import JSONDecodeError


class ConsumedGrouping:

    def __init__(self):
        # messages is a list of list, in the format [level, message]
        self.messages = []
        self.timestamp = None
        self.organization = None
        self.cluster_id = None
        self.offset = None
        self.error = False  # flag set to true if an error occured in this group


# @param file: path to the log file to decode and return
# @return value: list of decoded log records
def get_log_list(file):
    """Open a log file, decode the JSON, and export the log records"""
    log_list = [{}]

    if not os.path.exists(file) or not os.path.isfile(file):
        err = Exception("Could not open file")
        raise err

    with open(file, encoding='utf-8') as f:
        for line in itertools.islice(f, 10, None):
            try:
                line = line.replace("\\\\", "")
                line = line.replace("\\t", "")
                line = line.replace("\\n", "")
                line = line.replace("\\\"", "")
                log_list.append(json.loads(line))

            except JSONDecodeError as err:
                print("Error: Log record contains illegal, unescaped quotation mark ", err.msg, str(err))

    return log_list[1:]


# @param logs: list of log records decoded from the JSON
# @return value: list of a ConsumedGroupings, which contain the messages, timestamps, and other info for a group
def group_consumed_logs_old(logs):
    """Outdated"""
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
            current_group.offset = current_offset
            current_group.timestamp = record['time']
            current_group.messages.append([record['level'], record['message']])
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
                    current_group.messages.append([record['level'], record['message']])
                elif record['level'] == "error":
                    # add error
                    current_group.error = True
                    current_group.messages.append([record['level'], record['message']])
            elif 'offset' not in record and not record['message'].startswith("Request URI"):
                if record['level'] == "error":
                    current_group.error = True
                    current_group.messages.append([record['level'], record['error']])
                    current_group.messages.append([record['level'], record['message']])
                else:
                    current_group.messages.append([record['level'], record['message']])
            else:
                # end of group
                processing_group = False
                groupings.append(current_group)
                current_group = None
                if record['message'] == "Consumed":
                    # new group
                    processing_group = True
                    current_offset = record['offset']
                    current_group = ConsumedGrouping()
                    current_group.offset = current_offset
                    current_group.timestamp = record['time']
                    current_group.messages.append([record['level'], record['message']])

    # add last group to groupings
    if current_group is not None:
        groupings.append(current_group)

    return groupings


# @param logs: list of log records decoded from the JSON
# @return value: list of a ConsumedGroupings, which contain the messages, timestamps, and other info for a group
def group_consumed_offset_logs_old(logs):
    """Outdated"""
    groupings = []
    processing_group = False
    current_group = None
    local_record_index = 0  # for keeping track of how many records since first in a group

    for record in logs:
        record['message'] = record['message'].replace("\\n", "")
        record['message'] = record['message'].replace("\\t", "")
        # make sure each JSON has an associated message
        if 'message' not in record:
            raise Exception("Error: log with no message")

        if not processing_group and record['message'].startswith("Consumed message offset"):
            offset = record['message'].split(" ")[3]
            current_group = ConsumedGrouping()
            current_group.current_offset = offset
            processing_group = True
            local_record_index = 1
            current_group.timestamp = record['time']
            current_group.messages.append([record['level'], record['message']])

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
                current_group.messages.append([record['level'], record['message']])
                local_record_index += 1
            elif local_record_index >= 1 and record['level'] == "error":
                # add error to the grouping
                current_group.error = True
                current_group.messages.append([record['level'], record['error']])
                current_group.messages.append([record['level'], record['message']])
                local_record_index += 1
            else:
                # either regular log or end of group
                if record['message'].startswith("Request URI:") or record['message'].startswith("Consumed"):
                    # end of group
                    groupings.append(current_group)
                    if record['message'].startswith("Consumed"):
                        offset = record['message'].split(" ")[3]
                        local_record_index = 1
                        current_group = ConsumedGrouping()
                        current_group.offset = offset
                        current_group.timestamp = record['time']
                        current_group.messages.append([record['level'], record['message']])
                    else:
                        current_group = None
                        processing_group = False
                        local_record_index = 0
                else:
                    current_group.messages.append([record['level'], record['message']])
                    local_record_index += 1

    # add last group to groupings
    if current_group is not None:
        groupings.append(current_group)

    return groupings


# @param logs: list of log records decoded from the JSON
# @return value: list of a ConsumedGroupings, which contain the messages, timestamps, and other info for a group
def get_consumed_groups(logs):
    groupings = []
    processing_group = False
    current_group = None
    current_offset = None

    for record in logs:
        # ensure no dirty JSON formatting
        record['message'] = record['message'].replace("\\n", "")
        record['message'] = record['message'].replace("\\t", "")

        # make sure each JSON has an associated message
        if 'message' not in record:
            raise Exception("Error: log with no message")

        # get cluster logs, not URI Requests
        if record['message'].startswith("Request received - URI"):
            # skip URI requests
            continue
        if not processing_group:
            processing_group = True
            current_offset = record['offset']
            current_group = ConsumedGrouping()
            current_group.offset = current_offset
            current_group.timestamp = record['time']
            current_group.messages.append([record['level'], record['message']])

        # log is part of group, know by offset
        elif processing_group and 'offset' in record and record['offset'] == current_offset:
            if record['level'] != "error":
                # get organization and cluster if not found yet
                current_group.messages.append([record['level'], record['message']])
                if 'organization' in record and current_group.organization is None:
                    current_group.organization = record['organization']
                    current_group.cluster_id = record['cluster']
            else:
                # add error
                current_group.error = True
                current_group.messages.append(["error: ", record['error']])
                current_group.messages.append([record['level'], record['message']])

        elif processing_group and 'offset' not in record:
            # could be in group, could not be
            if record['level'] == "error:":
                # add error
                current_group.error = True
                current_group.messages.append(["error: ", record['error']])
                current_group.messages.append([record['level'], record['message']])
            else:
                # regular info line
                current_group.messages.append([record['level'], record['message']])
        elif 'offset' in record and current_offset != record['offset']:
            # processing a group, next offset doesn't match (new grouping)
            processing_group = True
            groupings.append(current_group)
            current_group = ConsumedGrouping()
            current_group.offset = record['offset']
            current_offset = record['offset']
            current_group.messages.append([record['level'], record['message']])
            current_group.timestamp = record['time']

    # add last item if needed
    if current_group is not None:
        groupings.append(current_group)

    return groupings


# @param log_file: pass the path to the log file to parse
# @return value: returns a list of ConsumedGroups. One group is based on Consumed, and the other Consumed Offset
def get_groups(log_file):
    """Take an aggregator log file as input, produce groupings, and print/return"""

    logs = get_log_list(log_file)

    # get log groupings for the two types
    results = get_consumed_groups(logs)

    return results


# @param log_file: pass the path to the log file to parse
# @return value: returns a list of ConsumedGroups in json format
def get_groups_as_json(log_file):
    json_groups = []
    regular_groups = get_groups(log_file)
    for group in regular_groups:
        json_groups.append(group.__dict__)
    return json_groups


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Error: Please provide a path to a file")
        sys.exit(1)

    try:
        groups = get_groups(sys.argv[1])
        print(len(groups))
    except Exception as e:
        print("error: ", e)
