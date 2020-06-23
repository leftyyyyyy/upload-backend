from flask import Flask, render_template, session, redirect, make_response, request, jsonify, Response
from flask_session import Session
import redis
from time import sleep
import uuid
from flask_cors import CORS
from werkzeug.utils import secure_filename
import os

from helpers import *
from stats import *

app     = Flask(__name__)
app.config.from_object("config")
CORS(app)

"""
Validate enviroment variables are present
"""
if app.config['S3_BUCKET'] is None: raise ValueError('Environment variable has not been set.')
if app.config['S3_KEY'] is None: raise ValueError('Environment variable has not been set.')
if app.config['S3_SECRET'] is None: raise ValueError('Environment variable has not been set.')
if app.config['SECRET_KEY'] is None: raise ValueError('Environment variable has not been set.')
if app.config['SESSION_TYPE'] is None: raise ValueError('Environment variable has not been set.')

"""
Instantiate the session manager
"""
sess = Session()
sess.init_app(app)

"""
Redis client for storing and retrieving user/files information
"""
redisClient = redis.StrictRedis(host='127.0.0.1', port=6379)

"""
Determine if the file is in an accepted format
"""
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']
    
@app.route("/", methods=['GET'])
def maintain_session():

    user_id = request.cookies.get('session')

    if not user_id:

        value = str(uuid.uuid4())
        
        """
        Generate new identifier if it exists already
        """

        while redisClient.get(value):
            value = uuid.uuid4()

        session['key'] = value

        res = make_response(redirect('/'))
        res.headers['Access-Control-Allow-Origin'] = '*'

        return res
    
    return jsonify({"Status" : "OK"})

"""
Return list of files
"""
@app.route("/fetchfiles", methods=['GET'])
def fetch_files():

    _files = []
    
    user_id = request.cookies.get('session')

    _file_identifiers = redisClient.lrange(user_id, 0, -1 )

    for _id in _file_identifiers:
        _file_info = redisClient.hgetall(_id.decode("utf-8"))
        unidict = {k.decode('utf8'): v.decode('utf8') for k, v in _file_info.items()}

        if 'CustomStatIdentifier' not in unidict.keys():
            unidict['Stats'] = {}
        else:
            unidict['Stats'] = redisClient.hgetall(unidict['CustomStatIdentifier'])

        _files.append(unidict)

    return jsonify(_files)

"""
Check if the user has the file
Retrieve file name
Check if the file exists locally
If not fetch from S3
"""
@app.route("/download", methods=['GET'])
def download_file():

    """
    Ensure the session cookie exists in the request
    """
    try:
        user_id = request.cookies.get('session')
        _input_file_identifier = request.args.get('file_identifier')
    except:
        return redirect("/")

    """
    Ensure the session cookie exists in redis
    """   
    if not redisClient.get('session:{}'.format(user_id)):
        return redirect("/")

    """
    Retrieve the file identifiers for the user
    """
    _file_identifiers = redisClient.lrange(user_id, 0, -1 )

    """
    Check if the input identifier belongs to the user, if so fetch its name
    """
    for _id in _file_identifiers:
        if _id.decode("utf-8") == _input_file_identifier:
            _file_name = redisClient.hgetall(_id)[b'Name'].decode("utf-8")

    if not _file_name:
        return jsonify({"Error": "File not found"})
    
    _file_name_in_s3 = _input_file_identifier + ".csv"

    return Response(
        get_object(_file_name_in_s3),
        mimetype='text/plain',
        headers={"Content-Disposition": "attachment;filename=test.txt"}
    )

@app.route("/upload", methods=['POST'])
def upload_file():

    _file = {}

    try:
        user_id = request.cookies.get('session')
    except:
        return redirect("/")
    
    if not redisClient.get('session:{}'.format(user_id)):
        return redirect("/")

	# A
    if "file" not in request.files:
        return jsonify({"Error": "No file key in request.files"})

	# B
    file = request.files['file']

	# C.
    if file.filename == "":
        return jsonify({"Error": "Please select a file"})

	# D.
    if file and allowed_file(file.filename):

        file.filename = secure_filename(file.filename)

        _file_identifier = str(uuid.uuid4())

        while redisClient.get(_file_identifier):
            _file_identifier = uuid.uuid4()

        _stat_identifier = str(uuid.uuid4())

        while redisClient.get(_stat_identifier):
            _stat_identifier = uuid.uuid4()

        _filename = '{}.csv'.format(_file_identifier)

        upload_object(_filename, file)

        try:
            file.seek(0)
        except:
            pass

        try:
            _custom_stat = calculate(file)
        except:
            _custom_stat = {}

        _file = {"Name": file.filename, "Identifier": _file_identifier, "CustomStatIdentifier": _stat_identifier}
        
        redisClient.hmset(_file_identifier, _file)

        redisClient.hmset(_stat_identifier, _custom_stat)

        redisClient.lpush(user_id, _file_identifier)

        _file['Stats'] = _custom_stat

    return jsonify(_file)

if __name__ == "__main__":

    app.run()