import os

import redis

import json 

from store import r1,r2, r3, r4, movie_json,tag_json

from app import remove_duplicate_tags

from jinja2 import StrictUndefined

from flask import Flask, render_template, redirect, request, flash, session, url_for

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "GHIKLMNO")

app.jinja_env.undefined = StrictUndefined


@app.route('/', methods=["GET"])
def index():
    return render_template("index.html")

@app.route('/find_movie', methods=["POST"])
def find_movie():
    movie_name = request.form.get("movie_name")
    print "MOVIE NAME"
    print movie_name

    #TODO Exception Handiling 
    movie_id = r3.get(str(movie_name))
    print "ID"
    print movie_id

    return redirect(url_for('sh ow_movie_tags', movie_id=str(movie_id)))

@app.route('/movies', methods=["GET"])
def display_movies():
    movie_list = []
    for i in movie_json:
        value = r1.get(str(i))
        movie_list.append((i,value))
    return render_template("movie.html", movie_list=movie_list)

@app.route('/movie_detail/<movie_id>', methods=["GET"])
@app.route('/movie_detail/', methods=["GET"])
def show_movie_tags(movie_id):
    movie_title = r1.get(str(movie_id))
    movie_tags = r2.get(str(movie_id))

    unique_tags = remove_duplicate_tags(movie_tags)

    num_ratings = r4.get(str(movie_id))

    rating_dates = 

    return render_template("movie_detail.html", movie_title=movie_title, unique_tags=unique_tags, num_ratings=num_ratings)




# @app.route('/movie_tags.json')
# def create_movie_tags_json():


@app.route('/clusters', methods=["GET"])
def display_clusters():
    return render_template("clusters.html")

if __name__ == "__main__":
    PORT = int(os.environ.get("PORT", 5000))
    DEBUG = "NO_DEBUG" not in os.environ
    app.run(debug=DEBUG, host="0.0.0.0", port=PORT)