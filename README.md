# LogScraper

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