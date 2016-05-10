import os

import redis

import json 

import re

from store import r1,r2, r3, r4, r5,r6,r7, movie_json,tag_json, movie_letter_json

from app import remove_duplicate_tags, process_timestamps, get_rating_breakdown, get_first_letter

from jinja2 import StrictUndefined

from flask import Flask, render_template, redirect, request, flash, session, url_for, jsonify

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "GHIKLMNO")

app.jinja_env.undefined = StrictUndefined


@app.route('/', methods=["GET"])
def index():
    """ Loads the main index page with search bar """
    return render_template("index.html")

@app.route('/find_movie', methods=["POST"])
def find_movie():
    """ 
    Renders the page showing data on a specific movie when the user searches  
    for that movie_id
    """   
    movie_name = request.form.get("movie_name")

    #TODO Exception Handiling 
    movie_id = r3.get(str(movie_name))

    return redirect(url_for('show_movie_tags', movie_id=str(movie_id)))

@app.route('/movies', methods=["GET"])
def display_movies():
    """ Renders the page containing a list of movies """
    movie_list = []
    for i in movie_json:
        value = r1.get(str(i[0]))
        movie_list.append((i[0],value))
    return render_template("movie.html", movie_list=movie_list)

@app.route('/movies/<letter>', methods=["GET"])
def show_movies_by_letter(letter):

    movies_with_letter = []

    if letter == '#':
        letter = " "
    
    movie_letter_list = movie_letter_json[letter]

    for i in movie_letter_list:
        value = r1.get(str(i[0]))
        movies_with_letter.append((i[0],value))
    print movies_with_letter
    

    return render_template("movie.html", movie_list=movies_with_letter)

@app.route('/movie_detail/<movie_id>', methods=["GET"])
@app.route('/movie_detail/', methods=["GET"])
def show_movie_tags(movie_id):
    """ 
    Renders the page showing data on a specific movie when the user clicks on 
    from the movie list page
    """  
    movie_title = r7.get(str(movie_id))
    movie_tags = r2.get(str(movie_id))

    unique_tags = remove_duplicate_tags(movie_tags)

    num_ratings = r4.get(str(movie_id))

    return render_template("movie_detail.html", movie_title=movie_title, unique_tags=unique_tags, num_ratings=num_ratings, movie_id= movie_id)

@app.route('/timestamp_counts.json/<movie_id>', methods=["GET"])
def get_timestamp_counts(movie_id):
    """ Gets the json showing the number of ratings per month """   
    rating_dates = r5.get(str(movie_id))

    ratings_per_month_and_year = process_timestamps(rating_dates)

    return jsonify(data=ratings_per_month_and_year)

@app.route('/rating_counts.json/<movie_id>', methods=["GET"])
def get_rating_counts(movie_id):
    """ Gets the json showing the number of ratings per month """  
    movie_ratings = r6.get(str(movie_id))

    rating_breakdown = get_rating_breakdown(movie_ratings)

    return jsonify(data=rating_breakdown)

@app.route('/clusters', methods=["GET"])
def display_clusters():
    """ Renders the clusters page """
    return render_template("clusters.html")

if __name__ == "__main__":
    PORT = int(os.environ.get("PORT", 5000))
    DEBUG = "NO_DEBUG" not in os.environ
    app.run(debug=DEBUG, host="0.0.0.0", port=PORT)