from flask import Flask, session, redirect, request, jsonify, Response
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

"""
Allow connection for the local client
"""
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

"""
Generate a user session if it doesn't exist in redis
"""
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

        return redirect("/")
    
    return jsonify({"Status" : "OK"})

"""
Return list of files and their information (name, identifier, stats)
"""
@app.route("/fetchfiles", methods=['GET'])
def fetch_files():

    """
    Ensure the session cookie exists in the request
    """
    try:
        user_id = request.cookies.get('session')
    except:
        return redirect("/")

    """
    Ensure the session cookie exists in redis
    """   
    if not redisClient.get('session:{}'.format(user_id)):
        return redirect("/")

    _files = []

    """
    Get the list of files that belong to the user
    """
    _file_identifiers = redisClient.lrange(user_id, 0, -1 )

    for _id in _file_identifiers:
        _file_info = redisClient.hgetall(_id.decode("utf-8"))
        unidict = {k.decode('utf8'): v.decode('utf8') for k, v in _file_info.items()}

        """
        Check if the file has custom stats
        """
        if 'CustomStatIdentifier' not in unidict.keys():
            unidict['Stats'] = {}
        else:
            unidict['Stats'] = redisClient.hgetall(unidict['CustomStatIdentifier'])

        _files.append(unidict)

    return jsonify(_files)

"""
Check if the user has the file
Retrieve file name
Fetch from S3
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
    if not _input_file_identifier or not redisClient.get('session:{}'.format(user_id)):
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

    """
    Ensure the session cookie exists in the request
    """
    try:
        user_id = request.cookies.get('session')
    except:
        return redirect("/")

    """
    Ensure the session cookie exists in redis
    """   
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

    """
    Check if the file is an allowed format and the name is secure. If so, then upload and calculate stats
    """
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

        """
        Reset the head of the file after the S3 upload to be read next for stats calculation
        """
        try:
            file.seek(0)
        except:
            pass

        try:
            _custom_stat = calculate(file)
            """
            Save the file stats under the stats identifier
            """
            redisClient.hmset(_stat_identifier, _custom_stat)
            _file = {"Name": file.filename, "Identifier": _file_identifier, "CustomStatIdentifier": _stat_identifier}
        
        except:
            _custom_stat = {}
            _file = {"Name": file.filename, "Identifier": _file_identifier}


        """
        Save the file's data under the file identifier
        """
        redisClient.hmset(_file_identifier, _file)

        """
        Add the file identifier to the list of files of the user
        """
        redisClient.lpush(user_id, _file_identifier)

        _file['Stats'] = _custom_stat

    return jsonify(_file)

if __name__ == "__main__":

    app.run()