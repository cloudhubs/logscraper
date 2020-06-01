import json
import flask
import fnmatch
import os
import aggregatorScript
import pipelineScript
from flask import Response, jsonify
from flask import request

app = flask.Flask(__name__)
app.config["DEBUG"] = True


class LogItem:
    orgId = -1
    clusterName = ""
    partition = -1
    offset = -1
    timestamp = ""
    isError = False
    isWarning = False
    messages = []


@app.route('/', methods=['GET'])
def home():
    return '''<h1><center>Log Scraper Server</center></h1>
<p>Home page of Team A to parse log files.</p>
<p> To parse logs use "/logs/default" with argument path: path to log files (usually thisRepositoryHome/logs)</p>
'''

# Functionality - Parse logs from given directory
# Parameters - path: path to logs usually thisRepositoryHome/logs
# Return - The call to scripts will print the parsed JSON
@app.route('/logs/default', methods=['GET'])
def get_logs():
    # dirpath='./logs'
    dirpath= request.args.get('path', default = './logs', type = str) #'./logs'
    print(dirpath)
    list = []
    for file in os.listdir(dirpath):
        if fnmatch.fnmatch(file, 'aggregator*.log'):
            list.append(file)
            list.append(aggregatorScript.get_groups_as_json((os.path.abspath(os.path.join(dirpath, file)))))
        if fnmatch.fnmatch(file, 'pipeline*.log'):
            list.append(file)
            list.append(pipelineScript.get_log_items((os.path.abspath(os.path.join(dirpath, file)))))
    return jsonify(list)

app.run()
