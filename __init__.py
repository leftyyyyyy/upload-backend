from flask import Flask, render_template, session, redirect, make_response, request, jsonify
from flask_session import Session
import redis
from multiprocessing import Process, Queue
from time import sleep
import uuid
from flask_cors import CORS
from werkzeug.utils import secure_filename
import os

from helpers import *

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
Queues for file uploading/downloading
"""
uploadQueue = Queue()
downloadQueue = Queue()

"""
Determine if the file is in an accepted format
"""
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

"""
Functions for rate limiting uploads and downloads to not overwhelm the system
"""
def process_files_to_s3():
  
    while True:

        processes = []

        while uploadQueue.qsize() > 0:

            _filename = uploadQueue.get()
            processes.append(Process(target=upload_file_to_s3, args=(_filename, app.config["S3_BUCKET"],)))

            if(len(processes) == app.config['MAX_UPLOAD_PROCESSES']):
                break
            
        for process in processes:
            process.start()

        for process in processes:
            process.join()

        sleep(2)

def process_files_from_s3():
  
    while True:

        processes = []

        while downloadQueue.qsize() > 0:
            _filename = downloadQueue.get()
            processes.append(Process(target=download_file_from_s3, args=(_filename, app.config["S3_BUCKET"],)))

            if(len(processes) == app.config['MAX_DOWNLOAD_PROCESSES']):
                break
            
        for process in processes:
            process.start()

        for process in processes:
            process.join()

        sleep(2)

    
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
    
    return ('', 200)

"""
Return list of files
"""
@app.route("/fetchfiles", methods=['GET'])
def fetch_files():

    print("fetching files")
    _files = []
    
    user_id = request.cookies.get('session')

    print("userid is", user_id)

    _file_identifiers = redisClient.lrange(user_id, 0, -1 )

    print("IDs is", _file_identifiers)
    for _id in _file_identifiers:
        _file_info = redisClient.hgetall(_id.decode("utf-8"))
        unidict = {k.decode('utf8'): v.decode('utf8') for k, v in _file_info.items()}
        _files.append(unidict)

    print(_files)
    print(jsonify(_files))
    return jsonify(_files)

"""
Check if the user has the file.
Retrieve file name.
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
        _input_file_identifier = request.args.get('file')
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
        if _id == _input_file_identifier:
            _file_name = redisClient.hgetall(_id)['Name']

    if not _file_name:
        return False
    
    """
    Check if the file exists locally first
    """
    _local_file_path = os.path.join(app.config['UPLOAD_FOLDER'], _input_file_identifier + '.csv')
    if(os.path.isfile(_local_file_path)):
        return send_file(_local_file_path, attachment_filename=_file_name)

    """
    If it doesn't exist locally, download from S3, return the file, then remove it
    """  
    #for i in range(redisClient.llen(user_id)):
    #    if redisClient.lindex(user_id, i).decode("utf-8") == _file_identifier:

    return Response(
        get_object(s3, total_bytes),
        mimetype='text/plain',
        headers={"Content-Disposition": "attachment;filename=test.txt"}
    )
	#return send_file('/var/www/PythonProgramming/PythonProgramming/static/images/python.jpg', attachment_filename='python.jpg')

    return False

@app.route("/upload", methods=['POST'])
def upload_file():

    try:
        user_id = request.cookies.get('session')
    except:
        return redirect("/")
    
    if not redisClient.get('session:{}'.format(user_id)):
        return redirect("/")

	# A
    if "file" not in request.files:
        return "No file key in request.files"

	# B
    file = request.files['file']
    """
        These attributes are also available

        file.filename               # The actual name of the file
        file.content_type
        file.content_length
        file.mimetype

    """

	# C.
    if file.filename == "":
        return "Please select a file"

	# D.
    if file and allowed_file(file.filename):

        file.filename = secure_filename(file.filename)

        _file_identifier = str(uuid.uuid4())

        while redisClient.get(_file_identifier):
            _file_identifier = uuid.uuid4()

        _filename = '{}.csv'.format(_file_identifier)

        upload_file_to_s3(_filename, file)

        _file = {"Name": file.filename, "Identifier": _file_identifier}

        redisClient.hmset(_file_identifier, _file)
        redisClient.lpush(user_id, _file_identifier)

        
    # chunk_size = 1000000
    # counter = 0
    # while True:
    #     chunk = request.stream.read(chunk_size)
    #     counter+=1
    #     if len(chunk) == 0:
    #         break
            
    #     upload_chunk_to_s3(_filename, chunk)

    return ('Upload successful',200)

if __name__ == "__main__":

    #Process(target=process_files_to_s3, args=()).start()
    #Process(target=process_files_from_s3, args=()).start()
    app.run()