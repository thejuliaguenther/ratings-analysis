import os

import redis

import json 

import re

from store import r1,r2, r3, r4, r5,r6,r7,r8, movie_json,tag_json, movie_letter_json

from app import remove_duplicate_tags, process_timestamps, get_rating_breakdown, get_first_letter

from jinja2 import StrictUndefined

from flask import Flask, render_template, redirect, request, flash, session, url_for, jsonify

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "GHIKLMNO")

app.jinja_env.undefined = StrictUndefined

def autocomplete(prefix, count=10):
    results = []
    range_length = 50 
    print "START!!!!"
    start = r8.zrank('substrings',prefix)
    print start    
    if not start:
         return results
         print "GOT TO RESULTS"
    while (len(results) != count):    

        values = r8.zrange('substrings',start,start+range_length-1) 
        print "GOT TO VALUES"     
        print values  
        print len(values) 
        start += range_length
        if not values or len(values) == 0:
            break
        for i in xrange(len(values)):
            item = values[i]
            min_len = min(len(item),len(prefix))             
            if item[0:min_len] != prefix[0:min_len]:                
                count = len(results)
                break           
            if item[-1] == '*' and len(results) != count:                 
                results.append(item[0:-1])
    return results

def get_movies_with_letter(movies_with_letter, letter):
    movie_letter_list = movie_letter_json[letter]

    for i in movie_letter_list:
        value = r1.get(i[0].encode('ascii', 'ignore'))
        movies_with_letter.append((i[0],value))
    return movies_with_letter

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

    print movie_name

    #TODO Exception Handiling 
    movie_id = r3.get(str(movie_name))

    return redirect(url_for('show_movie_tags', movie_id=str(movie_id)))

@app.route('/movies/view-all', methods=["GET"])
def display_movies():
    """ Renders the page containing a list of movies """
    movie_list = []
    for i in movie_json:
        value = r1.get(str(i[0]))
        movie_list.append((i[0],value))
    return render_template("movie.html", movie_list=movie_list)

@app.route('/autocomplete.json', methods=["POST"])
def get_autocomplete():
    movie_name = request.form.get("movie_name")

    print "MOVIE NAME"
    print movie_name

    autocomplete_results = autocomplete(movie_name)

    return jsonify(suggestions=autocomplete_results)

@app.route('/movies/<letter>', methods=["GET"])
@app.route('/movies', methods=["GET"])
def show_movies_by_letter(letter=" "):

    movies_with_letter = []


    if letter == " ":
        values_to_add = []
        keys_to_add = [' ', "'", '(', '.', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9']

        for key in keys_to_add:
            movies_with_letter = get_movies_with_letter(movies_with_letter, key)
    else:

        movies_with_letter = get_movies_with_letter(movies_with_letter, letter)
    

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

    if movie_tags != None :
        unique_tags = remove_duplicate_tags(movie_tags)
    else:
        unique_tags = ['This movie has no tags']

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