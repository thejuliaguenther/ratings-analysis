# Ratings Analytics 

Ratings Analytics uses Apache Spark to process a 20 million line movie ratings file. Designed for data geeks, Ratings Analytics provides an organic user experience for viewing insights about movie ratings.

## Technical Stack 
Apache Spark, Spark SQL, Redis, Python, Flask, JavaScript, D3.js, C3.js, Jquery, Ajax, HTML/CSS, Bootstrap

## Data
The data for this visualization comes from [grouplens.org](http://grouplens.org).
The original source of this data is the movie recommendation service [MovieLens](http://movielens.org). Here is the breakdown of the data:
- Total movie ratings: 20000263
- Number of movies rated: 27278 
- Number of anonymized users who rated movies: 138493 
- Time frame within which movies were rated: January 09, 1995 and March 31, 2015

The ratings range from 0.0-5.0 with increments of 0.5. The tags for each movie were submitted as free-tet by the users.

The users represented in this data set are randomly selected users that have rated at least 20 movies and are identified only by their anonymous user id; no other information is provided. 