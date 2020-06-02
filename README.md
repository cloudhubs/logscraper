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

Produce messages from selected input file to Kafka topic.

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
