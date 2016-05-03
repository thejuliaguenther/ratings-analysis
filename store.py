import redis

import json 

r1 = redis.StrictRedis(host='localhost', port=6379, db=0)

movie_data = open("movies.json").read()
movies_to_load = movie_data.encode('ascii', 'ignore')
movie_json = json.loads(movies_to_load)

for line in movie_json:
    r1.set(str(line), str(movie_json[line]))
    
r2 = redis.StrictRedis(host='localhost', port=6379, db=1)

tag_data = open("tags.json").read()
tags_to_load = tag_data.encode('ascii', 'ignore')
tag_json = json.loads(tags_to_load)

for movie in tag_json:
    tag_list = []
    for tag in tag_json[movie]:
        tag_list.append(str(tag))

    r2.set(str(movie), tag_list)

# Maps movie title to movieis; this can be done as all movie titles unique 
r3 = redis.StrictRedis(host='localhost', port=6379, db=2)

for id in movie_json:
    title = movie_json[id]
    title = title[0:-7]
    r3.set(str(title), str(id))


r4 = redis.StrictRedis(host='localhost', port=6379, db=3)

ratings_data = open("ratings.json").read()
ratings_to_load = ratings_data.encode('ascii', 'ignore')
ratings_json = json.loads(ratings_to_load)

for rating in ratings_json:

    r4.set(str(rating), str(ratings_json[rating]))


r5 = redis.StrictRedis(host='localhost', port=6379, db=4)

time_data = open("timestamps_per_movie.json").read()

times_to_load = time_data.encode('ascii', 'ignore')
time_json = json.loads(times_to_load)

for time in time_json:
    r5.set(str(time), str(time_json[time]))
    # print r5.get(time)











    







    



