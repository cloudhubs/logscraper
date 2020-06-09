#!/usr/bin/env python3

"""Produce messages from selected input file to Kafka topic."""

# Usage:
# log_producer.py [-h] [-v] [-b BROKER] [-t TOPIC] [-d DELAY] -i INPUT
#
# optional arguments:
#   -h, --help            show this help message and exit
#   -v, --verbose         make it verbose
#   -b BROKER, --broker BROKER
#                         broker (default 'localhost:9092')
#   -t TOPIC, --topic TOPIC
#                         topic (default 'logs')
#   -d DELAY, --delay DELAY
#                         delay between messages
#   -i INPUT, --input INPUT
#                         input file with messages to be produced


from kafka import KafkaProducer
from kafka.common import KafkaTimeoutError
from time import sleep
from json import dumps
from argparse import ArgumentParser


def connect_to_broker(broker, verbose):
    """Prepare connection to Kafka broker."""
    if verbose:
        print("Connecting to Kafka")

    producer = KafkaProducer(bootstrap_servers=[broker],
                             value_serializer=lambda x: dumps(x).encode('utf-8'))
    if verbose:
        print("Successfully connected to Kafka")

    return producer


def produce_messages(producer, topic, input_file, delay, verbose):
    """Try to read given file with logs and produce messages."""
    passes = 0
    failures = 0

    with open(input_file) as fin:
        for cnt, line in enumerate(fin):
            # text editors count from one
            line_no = cnt + 1

            # we don't need EOL characters
            line = line.strip()

            if verbose:
                print("Producing message '{}' read from line {}".format(line, cnt))

            try:
                producer.send(topic, value=line)
                sleep(delay)
                passes += 1
            except KafkaTimeoutError as e:
                print(e)
                failures += 1

    return passes, failures


def plural(how_many):
    """Construct postfix for singular or plural."""
    if how_many > 1 or how_many == 0:
        return "s"
    return ""


def display_report(passes, failures, verbose):
    """Display report about number of passes and failures."""
    if verbose:
        if failures == 0:
            if passes == 0:
                print("no messages to be produced")
            else:
                print("all {} message{} successfully produced".format(passes, plural(passes)))
        else:
            print("{} production failure{}".format(failures, plural(failures)))

    print("{} passe{}".format(passes, plural(passes)))
    print("{} failure{}".format(failures, plural(failures)))


def main():
    """Entry point to this tool."""
    parser = ArgumentParser()
    parser.add_argument("-v", "--verbose", dest="verbose", help="make it verbose",
                        action="store_true", default=None)
    parser.add_argument("-b", "--broker", dest="broker", help="broker (default 'localhost:9092')",
                        action="store", default="localhost:9092")
    parser.add_argument("-t", "--topic", dest="topic", help="topic (default 'logs')",
                        action="store", default="logs")
    parser.add_argument("-d", "--delay", dest="delay", help="delay between messages",
                        action="store", default=0)
    parser.add_argument("-i", "--input", dest="input",
                        help="input file with messages to be produced",
                        action="store", default=None, required=True)
    args = parser.parse_args()

    producer = connect_to_broker(args.broker, args.verbose)

    passes, failures = produce_messages(producer, args.topic, args.input, args.delay, args.verbose)

    producer.close()

    display_report(passes, failures, args.verbose)


if __name__ == "__main__":
    main()
