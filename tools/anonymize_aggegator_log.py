#!/usr/bin/env python3

# Copyright © 2020 Pavel Tisnovsky
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Anonymize aggregator log files by hashing organization ID and cluster ID."""

# Usage:
# anonymize_aggegator_log.py [-h] -s SALT < input.log > output.log
#
# optional arguments:
#   -h, --help            show this help message and exit
#   -s SALT, --salt SALT  salt for hashing algorithm

# Example:
# anonymize_aggegator_log.py -s foobar < original.log > anonymized.log

from hashlib import blake2b
from argparse import ArgumentParser
from sys import stdin


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
                                                     '"organization":',
                                                     ',"cluster":"')

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
                                                         '"cluster":"',
                                                         '","time":"')

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
        if '"organization":' in line and '"cluster":"' in line:
            line = hash_sensitive_values(line, salt=salt)
        print(line)


# If this script is started from command line, run the `main` function which is
# entry point to the processing.
if __name__ == "__main__":
    main()
