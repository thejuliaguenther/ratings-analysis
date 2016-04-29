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

for i in movie_json:
    r3.set(str(movie_json[line]), str([i])



