import flask
import fnmatch
import os
import aggregatorscript
import pipelinescript
import offsetanalysis
import clusteranalysis
from flask import Response, jsonify, abort
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
<p> Home page of Team A to parse log files.</p>
<p> To parse logs use "/logs/default" with argument path: path to log files (usually thisRepositoryHome/logs)</p>
<p> To search by cluster id and group id use "/logs/clusterorgstatus" with parameters path, clusterid, and orgid </p>
<p> To search by offset use "/logs/offstatus" with parameters path and offset </p>
<p> Example path for intended organization format "logs/pipeline" This will only parse pipeline.log for Mark </p>
<p> Example path for intended organization format "logs/aggregate" This will only parse aggregator.log for Mark </p>
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
                list.append(pipelinescript.get_chunks((os.path.abspath(os.path.join(dirpath, file)))))
                logCount += 1
    else:
        abort(404)

    # If logCount ends up being 0, then that means there was no valid log files given
    # and therefore wrong path to logs are provided
    if(logCount == 0):
        abort(404)
    else:
        return jsonify(list)


# Functionality - Parse pipeline.log only: This is an example of one file for team 3's viewing
# Parameters - path: path to logs usually thisRepositoryHome/logs
# Return - The call to scripts will print the parsed JSON
@app.route('/logs/pipeline', methods=['GET'])
def get_pipelinelog():

    dirpath = request.args.get('path', default='./logs', type=str)
    logCount = 0
    print(dirpath)
    list = []
    if os.path.exists(dirpath):
        for file in os.listdir(dirpath):
            if fnmatch.fnmatch(file, 'pipeline.log'):
                list.append(file)
                list.append(pipelinescript.get_chunks((os.path.abspath(os.path.join(dirpath, file)))))
                logCount += 1
    else:
        abort(404)

    # If logCount ends up being 0, then that means there was no valid log files given
    # and therefore wrong path to logs are provided
    if(logCount == 0):
        abort(404)
    else:
        return jsonify(list)


# Functionality - Parse aggregator.log only: This is an example of one file for team 3's viewing
# Parameters - path: path to logs usually thisRepositoryHome/logs
# Return - The call to scripts will print the parsed JSON
@app.route('/logs/aggregate', methods=['GET'])
def get_agglog():

    dirpath = request.args.get('path', default='./logs', type=str)
    logCount = 0
    list = []
    if os.path.exists(dirpath):
        for file in os.listdir(dirpath):
            if fnmatch.fnmatch(file, 'aggregator.log'):
                list.append(file)
                list.append(aggregatorscript.get_groups_as_json((os.path.abspath(os.path.join(dirpath, file)))))
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
    dirpath = request.args.get('path', default='./logs', type=str)
    cluster = request.args.get('cluster_id', default='92c04d4a-9f4c-441d-9df9-1e50c426df11', type=str)
    org = request.args.get('org_id', default='11789772', type=str)
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
            list = clusteranalysis.search_by_org_cluster(dirpath, org, cluster)
    else:
        abort(404)

    return jsonify(list)


# Functionality - Return the status of results for given offset
# Parameters
# dirpath: path to a directory of logs
# offset: The given offset to search for
# Return - The call to scripts will print the JSON status and result
@app.route('/logs/offstatus', methods=['GET'])
def search_by_offset():
    offset = request.args.get('offset', default='29', type=str)
    dirpath = request.args.get('path', default='./logs', type=str)
    list = []
    logCount = 0
    if os.path.exists(dirpath):
        for file in os.listdir(dirpath):
            if fnmatch.fnmatch(file, 'aggregator*.log'):
                logCount += 1
            if fnmatch.fnmatch(file, 'pipeline*.log'):
                logCount += 1
        if (logCount == 0):
            abort(404)
        else:
            list = offsetanalysis.search_by_offset(dirpath, offset)
    else:
        abort(404)
    return jsonify(list)


app.run()
