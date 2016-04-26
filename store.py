import redis
import json 

r = redis.StrictRedis(host='localhost', port=6379, db=0)

movie_data = open("movies.json").read()
movies_to_load = movie_data.encode('ascii', 'ignore')
movie_json = json.loads(movies_to_load)

for line in movie_json:
    r.set(str(line), str(movie_json[line]))
