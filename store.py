import redis

import json 

from app import get_first_letter

r1 = redis.StrictRedis(host='localhost', port=6379, db=0)

movie_data = open("movies.json").read()
movies_to_load = movie_data.encode('ascii', 'ignore')
movie_json = json.loads(movies_to_load)

# letters_to_movies = {}
# for line in movie_json:
#     encoded_id = line[0].encode('ascii', 'ignore')
#     encoded_title = line[1].encode('ascii', 'ignore')
#     first_letter = get_first_letter(encoded_title)
#     r1.set(str(encoded_id), str(encoded_title))

    
# r2 = redis.StrictRedis(host='localhost', port=6379, db=1)

# tag_data = open("tags.json").read()
# tags_to_load = tag_data.encode('ascii', 'ignore')
# tag_json = json.loads(tags_to_load)

# for movie in tag_json:
#     tag_list = []
#     for tag in tag_json[movie]:
#         tag_list.append(str(tag))

#     r2.set(str(movie), tag_list)

# # Maps movie title to movieis; this can be done as all movie titles unique 
# r3 = redis.StrictRedis(host='localhost', port=6379, db=2)

# for id in movie_json:
#     title = id[1]
#     id_for_title = id[0]
#     r3.set(str(title), str(id_for_title))


# r4 = redis.StrictRedis(host='localhost', port=6379, db=3)

# ratings_data = open("ratings.json").read()
# ratings_to_load = ratings_data.encode('ascii', 'ignore')
# ratings_json = json.loads(ratings_to_load)

# for rating in ratings_json:

#     r4.set(str(rating), str(ratings_json[rating]))


# r5 = redis.StrictRedis(host='localhost', port=6379, db=4)

# time_data = open("timestamps_per_movie.json").read()

# times_to_load = time_data.encode('ascii', 'ignore')
# time_json = json.loads(times_to_load)


# for time in time_json:
#     for seconds in xrange(len(time_json[time])):
#         ascii_seconds = time_json[time][seconds].encode('ascii', 'ignore')
#         time_json[time][seconds] = ascii_seconds
#     r5.set(str(time), str(time_json[time]))

# r6 = redis.StrictRedis(host='localhost', port=6379, db=5)

# rating_counts_data = open("rating_counts.json").read()
# rating_counts_to_load = rating_counts_data.encode('ascii', 'ignore')
# rating_counts_json = json.loads(rating_counts_to_load)


# for item in rating_counts_json:
#     encoded_item = item.encode('ascii', 'ignore')
#     ratings_to_encode = rating_counts_json[item]
#     encoded_ratings_per_movie = []

#     for non_encoded_rating in ratings_to_encode:
#         encoded_ratings_per_movie.append(non_encoded_rating.encode('ascii', 'ignore'))
#     r6.set(str(encoded_item), encoded_ratings_per_movie)

# r7 = redis.StrictRedis(host='localhost', port=6379, db=6)

# movie_letter_data = open("movies-and-letters.json").read()
# movies__letters_to_load = movie_letter_data.encode('ascii', 'ignore')
# movie_letter_json = json.loads(movies__letters_to_load)

# r8 = redis.Redis(host='localhost', port=6379, db=7)

# for x in movie_json:
#     movie_name = x[1]
#     movie_id = x[0]
#     for letter in xrange(len(movie_name[0:-6])):
#         prefix = movie_name[0:letter]
#         r8.zadd('substrings',prefix,0)
#     r8.zadd('substrings',movie_name+'*',0)
 
r9 = redis.StrictRedis(host='localhost', port=6379, db=8)

movie_genre_data = open("movies-and-genres.json").read()
movies_genres_to_load = movie_genre_data.encode('ascii', 'ignore')
movies_genres_json = json.loads(movies_genres_to_load)

for k in movies_genres_json:
    genre_list = []
    genres = movies_genres_json[k]

    for genre in genres:
        genre_list.append(genre.encode('ascii', 'ignore'))

    r9.set(str(k), genre_list)
    print r9.get(k)



















    







    



