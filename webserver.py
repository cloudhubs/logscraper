import flask
import fnmatch
import os
import aggregatorScript
import pipelineScript
from flask import request, jsonify

app = flask.Flask(__name__)
app.config["DEBUG"] = True

# Create some test data for our catalog in the form of a list of dictionaries.
books = [
    {'id': 0,
     'title': 'A Fire Upon the Deep',
     'author': 'Vernor Vinge',
     'first_sentence': 'The coldsleep itself was dreamless.',
     'year_published': '1992'},
    {'id': 1,
     'title': 'The Ones Who Walk Away From Omelas',
     'author': 'Ursula K. Le Guin',
     'first_sentence': 'With a clamor of bells that set the swallows soaring, the Festival of Summer came to the city Omelas, bright-towered by the sea.',
     'published': '1973'},
    {'id': 2,
     'title': 'Dhalgren',
     'author': 'Samuel R. Delany',
     'first_sentence': 'to wound the autumnal city.',
     'published': '1975'}
]

@app.route('/', methods=['GET'])
def home():
    return '''<h1><center>Team A Server</center></h1>
<p>Home page of Team A to parse log files.</p>
<p> To test enter the path "/api/test".</p>
<p> To parse the example given by Red Hat use "/logs/default".</p>
<p> To parse a folder use "/logs/folder" with the path to that folder as an argument.</p>'''

# A route to return all of the available entries in books object as an example
@app.route('/api/test', methods=['GET'])
def api_all():
    return jsonify(books)

# Functionality - Prints parsed JSON of aggregate logs and pipeline logs from Posted directories
# Parameters 
# pipeline_Path - Path to pipeline.log files
# aggregate_Path - Path to aggregate.log file
# Return - The call to scripts will print the parsed JSON
@app.route('/logs/folder', methods=['POST'])
def post_logs(pipeline_Path, aggregate_Path):
    for file in os.listdir(pipeline_Path):
	    pipelineScript.get_log_items((os.path.abspath(os.path.join(pipeline_Path, file))))
    for file in os.listdir(aggregate_Path):
        aggregator-group.get_log_list((os.path.abspath(os.path.join(aggregate_Path, file))))


# Functionality - Will print the example log files given by Mr. Tišnovský from Red Hat
# Parameters - None
# Return - The call to scripts will print the parsed JSON
@app.route('/logs/default', methods=['GET'])
def get_logs():
    dirpath='./logs'
    for file in os.listdir(dirpath):
        if fnmatch.fnmatch(file, 'aggregator*.log'):
            aggregatorScript.get_log_list((os.path.abspath(os.path.join(dirpath, file))))
        if fnmatch.fnmatch(file, 'pipeline*.log'):
            pipelineScript.get_log_items((os.path.abspath(os.path.join(dirpath, file))))


app.run()
