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
    # Start of second part in the input string.
    i1 = line.index(str1) + len(str1)

    # End of second part in the input string.
    i2 = line.index(str2)

    # Now we know where the second part can be found in input string. It is
    # therefore easy to figure out where the first part and third part are.
    return line[:i1], line[i1:i2], line[i2:]


def hash_org_id(line, salt):
    """Hash organization ID and return new line with encrypted value."""
    # First we need to retrieve the organization ID from input line.
    beginning, org_id, ending = split_by_two_strings(line,
                                                     'Message context: OrgId=',
                                                     ', ClusterName="')

    # Initialize hashing algorithm. Hash to 8 bytes might be enough for
    # organization ID (`long int` originally).
    h = blake2b(digest_size=8, salt=salt)

    # Convert string with organization ID to bytes and perform the hashing.
    h.update(org_id.encode('utf-8'))

    # Now it is possible to retrieve the hash in hex format and convert it
    # back to integer.
    new_org_id = int(h.hexdigest(), 16)

    # Format all three parts of log entry into expected output.
    return "{}{}{}".format(beginning, new_org_id, ending)


def hash_cluster_id(line, salt):
    """Hash cluster ID and return new line with encrypted value."""
    # First we need to retrieve the cluster ID from input line.
    beginning, cluster_id, ending = split_by_two_strings(line,
                                                         ' ClusterName="',
                                                         '", LastChecked="')

    # Initialize hashing algorithm. Hash to 16 bytes is enough for
    # cluster ID (32 hexa characters).
    h = blake2b(digest_size=16, salt=salt)

    # Convert string with cluster ID to bytes and perform the hashing.
    h.update(cluster_id.encode('utf-8'))

    # Now it is possible to retrieve the hash in hex format.
    x = h.hexdigest()

    # Format all parts of log entry into expected output.
    return "{}{}-{}-{}-{}-{}{}".format(beginning, x[0:8], x[8:12], x[12:16],
                                       x[16:20], x[20:], ending)


def hash_sensitive_values(line, salt=b'foo'):
    """Hash all sensitive values on line."""
    return hash_cluster_id(hash_org_id(line, salt), salt)


def hash_url(line, salt=b'foo'):
    """Change URL part from log line (not relevant anyway)."""
    # Retrieve the URL part from log line
    url = match(r'.* (https://\S+)', line).groups()[0]

    # Initialize hashing algorithm. Hash to 32 bytes is enough for
    # unique URL
    h = blake2b(digest_size=32, salt=salt)

    # Convert string with URL to bytes and perform the hashing.
    h.update(url.encode('utf-8'))

    # Now it is possible to retrieve the hash in hex format.
    x = h.hexdigest()

    # And use the hash instead of original URL.
    return sub(r'https://\S+', "https://example.com/" + x, line)


def anonymize_payload_hash(line):
    """Change payload hash (not relevant anyway)."""
    # Payload Tracker uses an unique string which is 32 characters long
    # and contains only alphanumeric characters. It is easy to replace such
    # string with its anonymized version (it has no meaning for log processing).
    return sub(r'[a-z0-9]{32}', '{anonymized}', line)


def main():
    """Entry point to this tool."""
    # First of all, we need to specify all command line flags that are
    # recognized by this tool.
    parser = ArgumentParser()
    parser.add_argument("-s", "--salt", dest="salt",
                        help="salt for hashing algorithm",
                        action="store", default="foo", required=True)

    # Now it is time to parse flags, check the actual content of command line
    # and fill in the object named `args`.
    args = parser.parse_args()

    # Retrieve the salt to be used by hashing algorithm.
    salt = args.salt.encode('utf-8')

    # This script works as a standard Unix filter (input->output), so we have
    # to process input in line-by-line basis.
    for cnt, line in enumerate(stdin):
        line = line.strip()
        # If the line contains any information that need to be anonymized,
        # do so.
        if 'Message context: OrgId=' in line and ', ClusterName="' in line:
            line = hash_sensitive_values(line, salt=salt)
        if "https://" in line:
            line = hash_url(line, salt=salt)
        if "Payload Tracker update successfully sent:" in line:
            line = anonymize_payload_hash(line)
        print(line)


# If this script is started from command line, run the `main` function which is
# entry point to the processing.
if __name__ == "__main__":
    main()
