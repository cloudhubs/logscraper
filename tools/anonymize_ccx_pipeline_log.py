#!/usr/bin/env python3

"""Anonymize CCX data pipeline log files by hashing organization ID and cluster ID."""

# Usage:
# anonymize_aggegator_log.py [-h] -s SALT < input.log > output.log
#
# optional arguments:
#   -h, --help            show this help message and exit
#   -s SALT, --salt SALT  salt for hashing algorithm


from hashlib import blake2b
from argparse import ArgumentParser
from sys import stdin
from re import sub, match


def split_by_two_strings(line, str1, str2):
    """Split the input line into three parts separated by two given strings."""
    # start of second part
    i1 = line.index(str1) + len(str1)
    # end of second part
    i2 = line.index(str2)
    return line[:i1], line[i1:i2], line[i2:]


def hash_org_id(line, salt):
    """Hash organization ID and return new line with encrypted value."""
    beginning, org_id, ending = split_by_two_strings(line,
                                                     'Message context: OrgId=',
                                                     ', ClusterName="')

    # hash to 8 bytes might be enough for organization ID (long originally)
    h = blake2b(digest_size=8, salt=salt)
    h.update(org_id.encode('utf-8'))
    new_org_id = int(h.hexdigest(), 16)

    # format into expected output
    return "{}{}{}".format(beginning, new_org_id, ending)


def hash_cluster_id(line, salt):
    """Hash cluster ID and return new line with encrypted value."""
    beginning, cluster_id, ending = split_by_two_strings(line,
                                                         ' ClusterName="',
                                                         '", LastChecked="')

    # hash to 16 bytes (32 hexa characters) might be enough for cluster ID
    h = blake2b(digest_size=16, salt=salt)
    h.update(cluster_id.encode('utf-8'))
    x = h.hexdigest()

    # format into expected output
    return "{}{}-{}-{}-{}-{}{}".format(beginning, x[0:8], x[8:12], x[12:16],
                                       x[16:20], x[20:], ending)


def hash_sensitive_values(line, salt=b'foo'):
    """Hash all sensitive values on line."""
    return hash_cluster_id(hash_org_id(line, salt), salt)


def hash_url(line, salt=b'foo'):
    """Change URL part from log line (not relevant anyway)."""
    url = match(r'.* (https://\S+)', line).groups()[0]
    h = blake2b(digest_size=32, salt=salt)
    h.update(url.encode('utf-8'))
    x = h.hexdigest()
    return sub(r'https://\S+', "https://example.com/" + x, line)


def anonymize_payload_hash(line):
    """Change payload hash (not relevant anyway)."""
    return sub(r'[a-z0-9]{32}', '{anonymized}', line)


def main():
    """Entry point to this tool."""
    parser = ArgumentParser()
    parser.add_argument("-s", "--salt", dest="salt",
                        help="salt for hashing algorithm",
                        action="store", default="foo", required=True)
    args = parser.parse_args()

    salt = args.salt.encode('utf-8')

    # this script works as a standard Unix filter (input->output)
    for cnt, line in enumerate(stdin):
        line = line.strip()
        if 'Message context: OrgId=' in line and ', ClusterName="' in line:
            line = hash_sensitive_values(line, salt=salt)
        if "https://" in line:
            line = hash_url(line, salt=salt)
        if "Payload Tracker update successfully sent:" in line:
            line = anonymize_payload_hash(line)
        print(line)


if __name__ == "__main__":
    main()
