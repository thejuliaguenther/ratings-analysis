import os

from jinja2 import StrictUndefined

from flask import Flask, render_template, redirect, request, flash, session

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "GHIKLMNO")

app.jinja_env.undefined = StrictUndefined

@app.route('/', methods=["GET"])
def index():
    return render_template("index.html")

@app.route('/movies', methods=["GET"])
def display_movies():
    return render_template("movie.html")

@app.route('/clusters', methods=["GET"])
def display_clusters():
    return render_template("clusters.html")

if __name__ == "__main__":
    PORT = int(os.environ.get("PORT", 5000))
    DEBUG = "NO_DEBUG" not in os.environ
    app.run(debug=DEBUG, host="0.0.0.0", port=PORT)