import flask
import fnmatch
import os
from src import aggregatorscript, pipelinescript, offsetanalysis, clusteranalysis
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

    dirpath = request.args.get('path', default='./logs', type=str)
    logCount = 0
    print(dirpath)
    list = []
    if os.path.exists(dirpath):
        for file in os.listdir(dirpath):
            if fnmatch.fnmatch(file, 'aggregator*.log'):
                list.append(file)
                list.append(aggregatorscript.get_groups_as_json((os.path.abspath(os.path.join(dirpath, file)))))
                logCount += 1
            if fnmatch.fnmatch(file, 'pipeline*.log'):
                list.append(file)
                list.append(pipelinescript.get_log_items((os.path.abspath(os.path.join(dirpath, file)))))
                logCount += 1
    else:
        abort(404)

    # If logCount ends up being 0, then that means there was no valid log files given
    # and therefore wrong path to logs are provided
    if(logCount == 0):
        abort(404)
    else:
        return jsonify(list)

# Functionality - Return the status of results for given cluster and org id's
# Parameters
# dirpath: path to a directory of logs
# cluster: The given cluster id to search for
# org: The given org id to search for
# Return - The call to scripts will print the JSON status and result
@app.route('/logs/clusterorgstatus', methods=['GET'])
def search_by_clusterid_orgid():

    dirpath = request.args.get('path', default='*', type=str)
    cluster = request.args.get('cluster_id', default='*', type=str)
    org = request.args.get('org_id', default='*', type=str)
    logCount = 0
    list = []
    if os.path.exists(dirpath):
        for file in os.listdir(dirpath):
            if fnmatch.fnmatch(file, 'aggregator*.log'):
                logCount += 1
            if fnmatch.fnmatch(file, 'pipeline*.log'):
                logCount += 1
        if (logCount == 0):
            abort(404)
        else:
            list = search_by_cluster(dirpath, org, cluster)
    else:
        abort(404)

    return jsonify(list)


# Functionality - Return the status of results for given offset
# Parameters
# dirpath: path to a directory of logs
# offset: The given offset to search for
# Return - The call to scripts will print the JSON status and result
@app.route('/logs/offsetStatus', methods=['GET'])
def search_by_offset():
    dirpath = request.args.get('path', default='*', type=str)
    offset = request.args.get('offset', default='*', type=str)
    list = []

    if os.path.exists(dirpath):
        for file in os.listdir(dirpath):
            if fnmatch.fnmatch(file, 'aggregator*.log'):
                logCount += 1
            if fnmatch.fnmatch(file, 'pipeline*.log'):
                logCount += 1
        if (logCount == 0):
            abort(404)
        else:
            list = search_by_cluster(dirpath, offset)
    else:
        abort(404)
    return jsonify(list)

app.run()
