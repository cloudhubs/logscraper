# LogScraper

## Description

## Git

```bash
git clone https://github.com/cloudhubs/logscraper.git
cd logscraper
git checkout poc1
git fetch
```

## Setup

```bash
export venvspath="/path/to/venv/folder"
export logscraperpath="/path/to/logscraper"
cd $venvspath
python3 -m venv logscraper
source logscraper/bin/activate
pip install -r $logscraperpath/requirements.txt
export FLASK_APP=$logscraperpath/webserver.py
flask run
```

## Query LogScraper

Get processed logs from pipeline and aggregate logs.
Argument: path - path to log files, usually inside directory logs of logscraper repository

```bash
curl -X GET 'http://localhost:5000/logs/default?path=/path/to/logs'
```
This endpoint makes following transformations:

* Filtering of message type, only consuming messages, not taking URI requests
* Grouping based on related messages
* Filtering based on offset


## API

Get overview of available APIs.
```
curl -X GET 'http://localhost:5000/
```

## Documentation

Documentation is hosted on Github Pages <https://cloudhubs.github.io/logscraper/>.
Sources are located in [docs](https://github.com/cloudhubs/logscraper/blob/master/docs/).

## Tools

### `log_producer.py`

Produces messages from selected input file to Kafka topic.

#### Usage:

```
 log_producer.py [-h] [-v] [-b BROKER] [-t TOPIC] [-d DELAY] -i INPUT

 optional arguments:
   -h, --help            show this help message and exit
   -v, --verbose         make it verbose
   -b BROKER, --broker BROKER
                         broker (default 'localhost:9092')
   -t TOPIC, --topic TOPIC
                         topic (default 'logs')
   -d DELAY, --delay DELAY
                         delay between messages
   -i INPUT, --input INPUT
                         input file with messages to be produced
```


### `anonymize_aggegator_log.py`

Anonymize aggregator log files by hashing organization ID and cluster ID.
This tool works as a standard Unix filter.


#### Usage:

```
 anonymize_aggegator_log.py [-h] -s SALT

 optional arguments:
   -h, --help            show this help message and exit
   -s SALT, --salt SALT  salt for hashing algorithm
```

### Example:

```
 anonymize_aggegator_log.py -s foobar < original.log > anonymized.log
```

[Annotated source code](https://cloudhubs.github.io/logscraper/anonymize_aggegator_log.html)

### `anonymize_ccx_pipeline_log.py`

Anonymize CCX data pipeline log files by hashing organization ID and cluster ID.
This tool works as a standard Unix filter.

#### Usage:

```
 anonymize_aggegator_log.py [-h] -s SALT < input.log > output.log

 optional arguments:
   -h, --help            show this help message and exit
   -s SALT, --salt SALT  salt for hashing algorithm
```

### Example:

```
 anonymize_ccx_pipeline_log.py -s foobar < original.log > anonymized.log
```

[Annotated source code](https://cloudhubs.github.io/logscraper/anonymize_ccx_pipeline_log.html)

