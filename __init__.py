from flask import Flask, render_template, session
from flask_session import Session
import redis
from multiprocessing import Process, Queue
from time import sleep
import uuid

from helpers import *

app     = Flask(__name__)
app.config.from_object("config")

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
Functions for rate limiting uploads and downloads to not overwhelm the system
"""
def process_files_to_s3(queue):
  
    while True:

        #print("Checking for processes")

        processes = []

        while queue.qsize() > 0:

            proc = queue.get()
            processes.add(proc)

            if(len(processes) == app.config['MAX_UPLOAD_PROCESSES']):
                break
            
        for process in processes:
            process.start()
            print('Process started')

        for process in processes:
            process.join()

        #print("Batch of processes is done")

        sleep(1)

def process_files_from_s3(queue):
  
    while True:

        #print("Checking for processes")

        processes = []

        while uploadQueue.qsize() > 0:

            proc = uploadQueue.get()
            processes.add(proc)

            if(len(processes) == app.config['MAX_DOWNLOAD_PROCESSES']):
                break
            
        for process in processes:
            process.start()
            print('Process started')

        for process in processes:
            process.join()

        #print("Batch of processes is done")

        sleep(1)

    
@app.route("/", methods=['GET'])
def set_session():
    value = str(uuid.uuid4())
    
    """
    Generate new identifier if it exists already
    """
    while redisClient.get(value):
        value = uuid.uuid4()

    session['key'] = value

    return value
    
"""
Return list of files
"""
@app.route("/fetchfiles", methods=['GET'])
def fetch_files():
    return 0

"""
Check if the user has the file.
Retrieve file name.
Check if the file exists locally
If not fetch from S3
"""

@app.route("/download", methods=['GET'])
def download_file():

    try:
        user_id = request.cookies.get('user_id')
        _file_identifier = request.args.get('file')
    except:
        return redirect("/")
    
    if not redisClient.get(user_id):
        return redirect("/")
    
    for i in range(redisClient.llen(user_id)):
        if redisClient.lindex(user_id, i).decode("utf-8") == _file_identifier:


		return send_file('/var/www/PythonProgramming/PythonProgramming/static/images/python.jpg', attachment_filename='python.jpg')

    return 0

@app.route("/upload", methods=['POST'])
def upload_file():

    try:
        user_id = request.cookies.get('user_id')
    except:
        return redirect("/")
    
    if not redisClient.get(user_id):
        return redirect("/")

	# A
    if "user_file" not in request.files:
        return "No user_file key in request.files"

	# B
    file    = request.files["user_file"]

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

        _file_identifier = str(uuid.uuid4()

        while redisClient.get(_file_identifier):
            _file_identifier = uuid.uuid4()

        _filename = '{}.csv'.format(_file_identifier)

        file.save(os.path.join(app.config['UPLOAD_FOLDER'], _filename))

        _file = {"Name": file.filename}

        redisClient.hmset(_file_identifier, _file)
        redisClient.lpush(user_id, _file_identifier)

        uploadQueue.put(Process(target=upload_file_to_s3, args=(file, _filename, app.config["S3_BUCKET"],)))

        return _file_identifier

    else:
        return redirect("/")

if __name__ == "__main__":

    Process(target=process_files_to_s3, args=(uploadQueue,)).start()
    Process(target=process_files_from_s3, args=(downloadQueue,)).start()
    app.run()