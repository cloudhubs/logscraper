import flask
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

# A route to return all of the available entries in our catalog.
@app.route('/api/test', methods=['GET'])
def api_all():
    return jsonify(books)

# A route to return JSON for log files in given path
# This will call the pipeline function for all pipeline.log files found and will call the aggregate
# function for all aggregate.log files found in the path and return results to the user
@app.route('/logs/folder', methods=['POST'])
def post_logs(path):
    #Search Path here
        #If pipeline.log call pipeline function written by Jackson
        #If aggregate.log call function written by Brooklynn. 
        #Append the result of these calls to the JSON object to return to the user
    #Return error if unable to find or open path or files 
    #return JSON Object
    return jsonify(books)


# A route to return JSON for default log files
# This will call some combo of our pipeline and aggregate functions to return the proper json output
# from the logs file given to us by Red Hat
@app.route('/logs/default', methods=['GET'])
def get_logs():
    return jsonify(books)


app.run()
