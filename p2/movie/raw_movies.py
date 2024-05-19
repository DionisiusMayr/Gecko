"""
Dependency installation: pip install tmdbsimple pyarrow fastparquet
"""
import tmdbsimple as tmdb
from datetime import date, datetime, timedelta
import requests
import os
import pandas as pd
import json
import gzip
import pyarrow.parquet as pq
import pyarrow as pa
import io


API_KEY = 'f95b4a84e123d3cf04a4b4730e1bcf32'
tmdb.API_KEY = API_KEY
tmdb.REQUESTS_TIMEOUT = 5  

def raw_movie_day(path):
    '''call request for the day'''
    day_before = date.today() - timedelta(days=1)
    current_day = str(day_before.day).zfill(2)
    current_month = str(day_before.month).zfill(2)
    current_year = day_before.year

    url = f'http://files.tmdb.org/p/exports/movie_ids_{current_month}_{current_day}_{current_year}.json.gz'
    response = requests.get(url, stream=True)
    json_movies_id = gzip.decompress(response.content).decode('utf8')

    day_before_str = day_before.strftime('%Y-%m-%d')
    df = pd.read_json(json_movies_id, lines=True)
    df.to_parquet(path+ 'raw_movies_'+ day_before_str +'.parquet', engine='fastparquet')

        
def raw_movies_id(path):
    '''stores all movies id available'''
    try:
        file = path + [f for f in os.listdir(path) if f.startswith('raw_movies_')][0]
        os.remove(file)
        raw_movie_day(path)
    except:   
        raw_movie_day(path)

def movie_info(movie_id, path):
    '''stores info of a movie'''
    try:
        file = path + [f for f in os.listdir(path) if f == 'movie_info_'+str(movie_id)+'.parquet' ][0]
    except:
        movie = tmdb.Movies(movie_id).info()
        fname = path + 'movie_info_'+str(movie_id)+'.parquet'
        df = pd.DataFrame([movie])
        df.to_parquet(fname, engine='fastparquet')

def movie_review(movie_id, path):
    '''get reviews for a movie'''
    try:
        file = path + [f for f in os.listdir(path) if f == 'movie_review_'+str(movie_id)+'.parquet' ][0]
    except:
        reviews = tmdb.Movies(movie_id).reviews()
        fname = path + 'movie_review_'+str(movie_id)+'.parquet'
        #df = pd.read_json(reviews, lines=True)
        df = pd.DataFrame([reviews])
        df.to_parquet(fname, engine='fastparquet')

def get_movies_id(path):
    '''gets the id of all movies'''
    try:
        file_movieid = path + [f for f in os.listdir(path) if f.startswith('raw_movies_')][0]
        df = pd.read_parquet(file_movieid)
        movie_ids = df['id'].values
        return movie_ids
    except:
        print('missing movie id file')


def main():
    path_movie_id = f'./raw_data/collections/'
    raw_movies_id(path_movie_id)
    movies_ids = get_movies_id(path_movie_id)
 
    path_movie_info = './raw_data/movies_info/'
    path_movie_review = './raw_data/movies_review/'
    c = 1
    for movie in movies_ids[:90]:
        c +=1 # just a counter to check progress
        if (c % 1000) == 0:
            print('movies collected: ',c)
        movie_info(movie, path_movie_info)
        movie_review(movie, path_movie_review)
    

if __name__ == "__main__":
    main()

#movie info 1kb json
#movie review 70kb json
