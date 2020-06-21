from flask import Flask, render_template
from flask_session import Session
import redis

app     = Flask(__name__)
app.config.from_object("config")

print(app.config)

@app.route("/fetchffiles", methods=['GET'])
def fetch_files():
    return 0

@app.route("/download", methods=['GET'])
def download():
    """Check if the user has the file.
    Retrieve file name.
    Check if the file exists locally
    If not fetch from S3
    """

    return 0

@app.route("/upload", methods=['POST'])
def upload():

    return 0

if __name__ == "__main__":
    app.run()