from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, length, from_json, expr, split, lit, to_date, explode, count, lower, trim, regexp_replace
from pyspark.sql.functions import substring, max as spark_max, ceil, input_file_name, from_unixtime, regexp_extract, concat
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructType, StructField, MapType, ArrayType, DoubleType, DateType, IntegerType

import xml.etree.ElementTree as ET
import requests
import os
import collections
import time
import html
import json

import pandas as pd
import yake
import numpy as np
from tqdm import tqdm
from graphdatascience import GraphDataScience

import utils

AWS_ACCESS_KEY_ID = 'test_key_id'
AWS_SECRET_ACCESS_KEY = 'test_access_key'
HOST = 's3'
ENDPOINT_URL = f'http://{HOST}:4566'

TEMP_DIR = './local_data'
DOWNLOAD_FROM_S3 = False

spark = (
    SparkSession
    .builder
    .config("spark.jars.packages", "org.neo4j:neo4j-connector-apache-spark_2.12:5.3.0_for_spark_3")
    .getOrCreate()
)

# Collect data from S3

CONTENTS = ['movie', 'boardgame', 'videogame', 'anime']

def download_raw_data_of_content(content):
    print(f'Downloading raw-data of {content}...')
    
    target_dir = f"{TEMP_DIR}/{content}"
    
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
        
    s3 = utils.S3_conn()

    paginator = s3.s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket='raw-data', Prefix=content)
    for page in page_iterator:
        if 'Contents' in page:
            for obj in tqdm(page['Contents']):
                key = obj['Key']
                local_file_path = f'{target_dir}/{key[len(content) + 1:]}'# os.path.join(target_dir, key[len(kind):])
                local_file_dir = os.path.dirname(local_file_path)
                
                if not os.path.exists(local_file_dir):
                    os.makedirs(local_file_dir)
                
                s3.s3_client.download_file('raw-data', key, local_file_path)

if DOWNLOAD_FROM_S3:
    for content in CONTENTS:
        download_raw_data_of_content(content)

# Transform data

s3 = utils.S3_conn()

def store_processed_parquet(local_directory, prefix):
    bucket_name = 'processed-data'
    
    for root, dirs, files in tqdm(os.walk(local_directory)):
        for filename in files:
            # Construct the full local path
            local_path = os.path.join(root, filename)
            
            # Construct the relative path for S3
            relative_path = os.path.relpath(local_path, local_directory)
            s3_path = os.path.join(prefix, relative_path).replace("\\", "/")  # Ensure Unix-style paths for S3
            
            # Upload the file to S3
            s3.s3_client.upload_file(local_path, bucket_name, s3_path)

## Boardgames

BOARDGAME_USERS_XML_PATH = './local_data/boardgame/collection'
BOARDGAME_USERS_PARQUET_PATH = './local_data/boardgame/processed_data/boardgame_users.parquet'
BOARDGAME_CONTENT_XML_PATH = './local_data/boardgame/boardgame'
BOARDGAME_CONTENT_PARQUET_PATH = './local_data/boardgame/processed_data/boardgame_content.parquet'

def xml_collection_to_dataframe(xml_file) -> pd.DataFrame:
    with open(xml_file, 'r') as f:
        r_text = f.read()
        root = ET.fromstring(r_text)

    df_user_id = []
    df_type = []
    df_content_id = []
    df_rating = []
    df_rating_date = []
    
    for bg in root:
        bg_name = bg[0].text
        coll_id = bg.attrib['collid']
        object_id = bg.attrib['objectid']  # This is the boardgame identifier

        rating_val = None
        for field in bg:
            if field.tag == 'stats':
                rating_val = field[0].attrib['value']
                if rating_val == 'N/A':
                    rating_val = None
            if field.tag == 'yearpublished':
                year_published = field.text
            if field.tag == 'status':
                date_of_rating = field.attrib['lastmodified']  # Not really the rating date, but it is as close as possible with the current information.

        df_user_id.append(xml_file.split('/')[-1][:-4])
        df_type.append('boardgame')
        df_content_id.append(object_id)
        df_rating.append(rating_val)
        df_rating_date.append(date_of_rating)

    return pd.DataFrame({
        'user_id': pd.Series(df_user_id, dtype='str'),
        'type': pd.Series(df_type, dtype='category'),
        'content_id': pd.Series(df_content_id, dtype='str'),
        'rating': pd.Series(df_rating, dtype='float64'),
        'rating_date': pd.Series(df_rating_date, dtype='datetime64[ms]')
    })

def create_boardgame_users_parquet():
    if not os.path.exists(BOARDGAME_USERS_PARQUET_PATH):
        os.makedirs(BOARDGAME_USERS_PARQUET_PATH)
        
    for xml in filter(lambda x: x.endswith('.xml'), os.listdir(BOARDGAME_USERS_XML_PATH)):
        try:
            df = xml_collection_to_dataframe(f'{BOARDGAME_USERS_XML_PATH}/{xml}')
            parquet_path = f'{BOARDGAME_USERS_PARQUET_PATH}/{xml[:-4]}.parquet'
            df.to_parquet(parquet_path)
        except Exception as e:
            print(e)
            print(f'Error: Invalid xml file: {xml}')

def xml_boardgame_to_dataframe():
    df_content_id = []
    df_content_description = []
    df_content_year = []
    df_title = []

    for folder in os.listdir(BOARDGAME_CONTENT_XML_PATH):
        with open(f"{BOARDGAME_CONTENT_XML_PATH}/{folder}/1.xml", 'r') as f:
            r_text = f.read()
        df_content_id.append(folder)
        root = ET.fromstring(r_text)
        for bg in root:
            for field in bg:
                if field.tag == 'name' and field.attrib['type'] == 'primary':
                    df_title.append(field.attrib['value'])
                if field.tag == 'description':
                    df_content_description.append(html.unescape(field.text))
                if field.tag == 'yearpublished':
                    df_content_year.append(int(field.attrib['value']))

    return pd.DataFrame({
        'content_id': pd.Series(df_content_id, dtype='str'),
        'description': pd.Series(df_content_description, dtype='str'),
        'release_year': pd.Series(df_content_year, dtype='Int16'),
        'title': pd.Series(df_title, dtype='str')
    })

def create_boardgame_content_parquet():
    if not os.path.exists(BOARDGAME_CONTENT_PARQUET_PATH):
        os.makedirs(BOARDGAME_CONTENT_PARQUET_PATH)

    schema = StructType([
        StructField("content_id", StringType(), True),
        StructField("description", StringType(), True),
        StructField("release_year", IntegerType(), True),
        StructField("title", StringType(), True)
    ])
    
    df = xml_boardgame_to_dataframe()
    df['description'] = df['description'].astype('str')
    df = df.replace([np.nan], [None])
    
    boardgame_content = (
        spark
        .createDataFrame(df, schema=schema)
        .withColumn('type', lit('boardgame'))
    )
    
    # Save parquet to processed-data zone
    boardgame_content.write.mode('overwrite').parquet(BOARDGAME_CONTENT_PARQUET_PATH)

def get_boardgame_users_df():
    boardgame_users = spark.read.parquet(BOARDGAME_USERS_PARQUET_PATH)
    return boardgame_users

def get_boardgame_content_df():
    boardgame_content = spark.read.parquet(BOARDGAME_CONTENT_PARQUET_PATH)
    return boardgame_content

create_boardgame_users_parquet()

create_boardgame_content_parquet()

boardgame_users = get_boardgame_users_df()

boardgame_content = get_boardgame_content_df()

store_processed_parquet(BOARDGAME_USERS_PARQUET_PATH, prefix='boardgame')

store_processed_parquet(BOARDGAME_CONTENT_PARQUET_PATH, prefix='boardgame')

## Movies

MOVIE_BASE_PARQUET_PATH = './local_data/movie/review'
MOVIE_BASE_INFO_PATH = './local_data/movie/info'
MOVIE_USERS_PARQUET_PATH = "./local_data/movie/processed_data/movie_users.parquet"
MOVIE_CONTENT_PARQUET_PATH = "./local_data/movie/processed_data/movie_content.parquet"

def create_movie_users_parquet():
    schema = ArrayType(
        StructType([
            StructField("author", StringType(), True),
            StructField("author_details", StructType([
                StructField("rating", StringType(), True)
            ]), True),
            StructField("created_at", StringType(), True),
        ])
    )
    
    movie_users = spark.read.parquet(MOVIE_BASE_PARQUET_PATH)\
              .filter(length("results")>2)\
              .withColumn("results_test", col('results'))\
              .withColumn("results_parsed", from_json(col("results_test"), schema))\
              .withColumn("result_exploded", explode(col("results_parsed")))\
              .withColumn('result_exploded', col("result_exploded").cast(StringType()))
    
    split_col = split(movie_users['result_exploded'], ', ')
    
    movie_users = movie_users.withColumn('author', split_col.getItem(0)) \
               .withColumn('author', expr("substring(author,2, length(author) -1)")) \
               .withColumn('rating', split_col.getItem(1)) \
               .withColumn("rating", expr("substring(rating, 2, length(rating) - 2)"))\
               .withColumn("rating", col('rating').cast(DoubleType()))\
               .withColumn('rating_date', split_col.getItem(2))\
               .withColumn('rating_date', expr("substring(rating_date,1, length(rating_date) -1)"))\
               .withColumn("rating_date", to_date(col("rating_date"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))\
               .select(col('author').alias('user_id'), lit('movie').alias('type'), col('id').alias('content_id').cast(StringType()), 'rating', 'rating_date')

    if not os.path.exists(MOVIE_USERS_PARQUET_PATH):
        os.makedirs(MOVIE_USERS_PARQUET_PATH)

    movie_users.repartition(1).write.mode('overwrite').parquet(MOVIE_USERS_PARQUET_PATH)

def create_movie_content_parquet():
    if not os.path.exists(MOVIE_CONTENT_PARQUET_PATH):
        os.makedirs(MOVIE_CONTENT_PARQUET_PATH)
    
    movie_content = (
        spark
        .read.parquet(MOVIE_BASE_INFO_PATH)
        .select(col('id').alias('content_id'), col('overview').alias('description'), col('release_date').alias('release_year'),col('original_title').alias('title'))
        .withColumn('release_year', substring("release_year", 1, 4))
        .withColumn('type', lit('movie'))
        .repartition(1).write.mode('overwrite').parquet(MOVIE_CONTENT_PARQUET_PATH)
    )

def get_movie_users_df():
    movie_users = spark.read.parquet(MOVIE_USERS_PARQUET_PATH)
    return movie_users

def get_movie_content_df():
    movie_content = spark.read.parquet(MOVIE_CONTENT_PARQUET_PATH)
    return movie_content

create_movie_users_parquet()

create_movie_content_parquet()

movie_users = get_movie_users_df()

movie_content = get_movie_content_df()

store_processed_parquet(MOVIE_USERS_PARQUET_PATH, prefix='movie')

store_processed_parquet(MOVIE_CONTENT_PARQUET_PATH, prefix='movie')

## Anime

ANIME_BASE_CONTENT_PATH = './local_data/anime/info'
ANIME_BASE_USERS_PATH = './local_data/anime/user_info'
ANIME_TEMP_PARQUET_PATH = './local_data/anime/temp'
ANIME_USERS_PARQUET_PATH = "./local_data/anime/processed_data/anime_users.parquet"
ANIME_CONTENT_PARQUET_PATH = "./local_data/anime/processed_data/anime_content.parquet"

def create_anime_users_parquet():
    if not os.path.exists(ANIME_USERS_PARQUET_PATH):
        os.makedirs(ANIME_USERS_PARQUET_PATH)
    path_for_anime_lists = ANIME_BASE_USERS_PATH
    user_anime_lists_paths = os.listdir(path_for_anime_lists)
    
    df = spark.read.json(
        path = [f'{path_for_anime_lists}/{i}' for i in user_anime_lists_paths],
        multiLine = True, 
        mode = 'DROPMALFORMED'
    ).withColumn('file_name', input_file_name()).select(
        from_unixtime(col('updated_at')).alias('rating_date'),
        col('score').alias('rating'),
        col('anime_id').alias('content_id'),
        regexp_extract(col('file_name'), '\/([^\/]+)\.json$', 1).alias('user_id'),
    )\
    .withColumn('type', lit('anime'))\
    .coalesce(1).write.mode('overwrite').parquet(ANIME_USERS_PARQUET_PATH)

def create_anime_content_parquet():
    if not os.path.exists(ANIME_CONTENT_PARQUET_PATH):
        os.makedirs(ANIME_CONTENT_PARQUET_PATH)

    if not os.path.exists(ANIME_TEMP_PARQUET_PATH):
        os.makedirs(ANIME_TEMP_PARQUET_PATH)
    
    path_for_animes = ANIME_BASE_CONTENT_PATH
    anime_paths = os.listdir(path_for_animes)
    
    batch_size = 1000
    cnt = 0
    
    while len(anime_paths) > cnt * batch_size :
        df = spark.read.json(
            path = [f'{path_for_animes}/{i}' for i in anime_paths][cnt * batch_size: (cnt + 1) * batch_size],
            multiLine = True, 
            mode = 'DROPMALFORMED'
        )\
        .dropna(subset=['data.aired.prop.from.year'])
        df.write.mode('overwrite').parquet(f'{ANIME_TEMP_PARQUET_PATH}/{cnt}')
        cnt += 1
    
    parquet_files_path = ANIME_TEMP_PARQUET_PATH
    parquet_files = os.listdir(parquet_files_path)
    df = spark.read.parquet(*[f'{parquet_files_path}/{i}' for i in parquet_files])
    df.select(
        col('data.synopsis').alias('description'),
        col('data.title').alias('title'),
        col('data.mal_id').cast(StringType()).alias('content_id'),
        col('data.aired.prop.from.year').alias('release_year')
    )\
    .withColumn('type', lit('anime'))\
    .coalesce(1).write.mode('overwrite').parquet(ANIME_CONTENT_PARQUET_PATH)

def get_anime_users_df():
    anime_users = spark.read.parquet(ANIME_USERS_PARQUET_PATH)
    return anime_users

def get_anime_content_df():
    anime_content = spark.read.parquet(ANIME_CONTENT_PARQUET_PATH)
    return anime_content

create_anime_users_parquet()

create_anime_content_parquet()

anime_users = get_anime_users_df()

anime_content = get_anime_content_df()

store_processed_parquet(ANIME_USERS_PARQUET_PATH, prefix='anime')

store_processed_parquet(ANIME_CONTENT_PARQUET_PATH, prefix='anime')

## Videogames

VIDEOGAME_BASE_SUMMARIES_PATH = './local_data/videogame/player_profile.json'
VIDEOGAME_BASE_PROFILES_PATH = './local_data/videogame/games_played.json'
VIDEOGAME_BASE_GAMES_PATH = './local_data/videogame/steam_games.json'
VIDEOGAME_USERS_PARQUET_PATH = "./local_data/videogame/processed_data/v_users.parquet"
VIDEOGAME_CONTENT_PARQUET_PATH = "./local_data/videogame/processed_data/videogame_content.parquet"

def create_videogame_users_parquet():
    # Load player_summaries.json
    with open(VIDEOGAME_BASE_SUMMARIES_PATH, 'r') as f:
        player_summaries_data = json.load(f)
    
    # Load steam_profiles.json
    with open(VIDEOGAME_BASE_PROFILES_PATH, 'r') as f:
        steam_profiles_data = json.load(f)
    
    # Initialize list to store data
    common_rows = []
    
    # Process data from steam_profiles_data
    for steam_profiles in steam_profiles_data:
        steamid = list(steam_profiles.keys())[0]
        games = steam_profiles[steamid]
        player_summary = next((summary for summary in player_summaries_data if steamid in summary), None)
        # Check if the player summary data is available and not empty
        if player_summary and player_summary[steamid]:
            personaname = player_summary[steamid].get('personaname', 'Unknown')
            for game in games:
                appid = game['appid']
                playtime_forever = game['playtime_forever']
                if playtime_forever > 0:  # Skip if playtime_forever is 0
                    common_rows.append({'user_id': personaname, 'type': 'videogame', 'content_id': appid, 'temp_rating': playtime_forever})

    common_df = spark.createDataFrame(common_rows)
    max_playtime = common_df.groupBy('user_id').agg(spark_max('temp_rating').alias('max_temp_rating'))
    common_df = common_df.join(max_playtime, on='user_id')
    common_df = common_df.withColumn('rating', (col('temp_rating') / col('max_temp_rating')) * 10)
    common_df = common_df.withColumn('rating', ceil(col('rating')))
    common_df = common_df.drop('temp_rating', 'max_temp_rating')
    common_df = common_df.withColumn('rating_date', lit(None).cast('string'))
    common_df.write.mode('overwrite').parquet(VIDEOGAME_USERS_PARQUET_PATH)

def create_videogame_content_parquet():
    # Load the dataset from games.json
    dataset = {}
    if os.path.exists(VIDEOGAME_BASE_GAMES_PATH):
        with open(VIDEOGAME_BASE_GAMES_PATH, 'r', encoding='utf-8') as fin:
            text = fin.read()
            if len(text) > 0:
                dataset = json.loads(text)
    
    # Initialize list to store data
    rows = []
    
    # Extract the relevant data
    for app_id, game_info in dataset.items():
        name = game_info.get('name', '')
        release_date = game_info.get('release_date', '')
        # Extract the year from the release_date
        if release_date:
            release_year = release_date.split()[-1]
        else:
            release_year = ''
        description = game_info.get('detailed_description', '')
    
        rows.append(Row(content_id=app_id, title=name, release_year=release_year, description=description))
    
    # Create Spark DataFrame
    df = spark.createDataFrame(rows)
    df = df.withColumn('type', lit('videogame'))
    df.write.mode('overwrite').parquet(VIDEOGAME_CONTENT_PARQUET_PATH)

def get_videogame_users_df():
    videogame_users = spark.read.parquet(VIDEOGAME_USERS_PARQUET_PATH)
    return videogame_users

def get_videogame_content_df():
    videogame_content = spark.read.parquet(VIDEOGAME_CONTENT_PARQUET_PATH)
    return videogame_content

create_videogame_users_parquet()

create_videogame_content_parquet()

videogame_users = get_videogame_users_df()

videogame_content = get_videogame_content_df()

store_processed_parquet(VIDEOGAME_USERS_PARQUET_PATH, prefix='videogame')

store_processed_parquet(VIDEOGAME_CONTENT_PARQUET_PATH, prefix='videogame')

# Merging all content

merged_users = (
    boardgame_users
    .union(movie_users.select(['user_id', 'type', 'content_id', 'rating', 'rating_date']))
    .union(anime_users.select(['user_id', 'type', 'content_id', 'rating', 'rating_date']))
    .union(videogame_users.select(['user_id', 'type', 'content_id', 'rating', 'rating_date']))
    .withColumn('user_id', trim(lower(col('user_id'))))
    .withColumn('type', lower(col('type')))
)

limit_rows = 5000
merged_content = (
    boardgame_content.limit(limit_rows)
    .unionByName(movie_content.limit(limit_rows).select(['content_id', 'description', 'release_year', 'type','title']))
    .unionByName(anime_content.limit(limit_rows).select(['content_id', 'description', 'release_year', 'type','title']))
    .unionByName(videogame_content.limit(limit_rows).select(['content_id', 'description', 'release_year', 'type','title']))
)

# Yake

def get_kw(text):
   kw_extractor = yake.KeywordExtractor(
       lan='en',
       n=1,  # Max n-gram size
       top=15  # Number of keywords
   )
    
   return list(map(lambda x: str.lower(x[0]) if x else '', kw_extractor.extract_keywords(text)))

get_kw = udf(get_kw, ArrayType(StringType()))

# Neo4j

# Utility code to drop the Neo4j database
# gds.run_cypher('MATCH (n) DETACH DELETE n;')
# gds.graph.drop('embedding-projection')

NEO4J_URL = 'bolt://neo4j:7687'

gds = GraphDataScience("neo4j://neo4j",aura_ds=False)

## Uploads Keywords

gds.run_cypher('CREATE INDEX content_id IF NOT EXISTS FOR (n:content) ON (n.content_id)')
gds.run_cypher('CREATE INDEX keywords IF NOT EXISTS FOR (n:keyword) ON (n.keyword)')
gds.run_cypher('CREATE INDEX users_id IF NOT EXISTS FOR (n:users) ON (n.users_id)')
gds.run_cypher('CREATE INDEX content_type IF NOT EXISTS FOR (n:content) ON (n.type)')

kw_df = (
    merged_content
    .withColumn('yake',get_kw(col('description')))
    .select(
        explode(col('yake')).alias('keywords'),
        'description',
        'release_year',
        'type',
        'title',
        'content_id'
    )
)

CONTENT_KEYWORDS_PARQUET_PATH = "./local_data/processed_data/content.parquet"
kw_df.write.mode('overwrite').parquet(CONTENT_KEYWORDS_PARQUET_PATH)
store_processed_parquet(CONTENT_KEYWORDS_PARQUET_PATH, 'merged')

for kind in CONTENTS:
    (
        kw_df.filter(col('type') == kind).write
        # // Create new relationships
        .mode('Append')
        .format("org.neo4j.spark.DataSource")
        # // Assign a type to the relationships
        .option("relationship", "has_keyword")
        # // Use `keys` strategy
        .option("relationship.save.strategy", "keys")
        # // Overwrite source nodes and assign them a label
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.source.labels", ':content')
        # // Map the DataFrame columns to node properties
        .option("relationship.source.node.properties", "title,content_id,type,description")
        # // Node keys are mandatory for overwrite save mode
        .option("relationship.source.node.keys", "content_id,type")
        # // Overwrite target nodes and assign them a label
        .option("relationship.target.save.mode", "Overwrite")
        .option("relationship.target.labels", ":keyword")
        # // Map the DataFrame columns to node properties
        .option("relationship.target.node.properties", "keywords")
        # // Node keys are mandatory for overwrite save mode
        .option("relationship.target.node.keys", "keywords")
        .option("url", NEO4J_URL)
        .save()
    )

## Upload users

filter_users = merged_users\
.join(
    merged_content, 
    (merged_content.type == merged_users.type) & (merged_content.content_id == merged_users.content_id), 
    how='right'
).select(['user_id',merged_users['type'],merged_users['content_id'],'rating'])

USERS_PARQUET_PATH = "./local_data/processed_data/users.parquet"
filter_users.write.mode('overwrite').parquet(USERS_PARQUET_PATH)
store_processed_parquet(USERS_PARQUET_PATH, 'merged')

for kind in CONTENTS:
    for r_type in ['like', 'dislike']:
        if r_type == 'like':
            df = filter_users.filter(col('rating') >= 8)
        else :
            df = filter_users.filter((col('rating') >= 1) & (col('rating')<8))
        (
        df.filter(col('type') == kind).write
          # // Create new relationships
          .mode('Append')
          .format("org.neo4j.spark.DataSource")
          # // Assign a type to the relationships
          .option("relationship", r_type)
          # // Use `keys` strategy
          .option("relationship.save.strategy", "keys")
          # // Overwrite source nodes and assign them a label
          .option("relationship.source.save.mode", "Overwrite")
          .option("relationship.source.labels", ':content')
          # // Node keys are mandatory for overwrite save mode
          .option("relationship.source.node.keys", "content_id,type")
          # // Overwrite target nodes and assign them a label
          .option("relationship.target.save.mode", "Overwrite")
          .option("relationship.target.labels", ":users")
          # // Node keys are mandatory for overwrite save mode
          .option("relationship.target.node.keys", "user_id,type")
          .option("url", NEO4J_URL)
          .save()
        )

## Neo4j Data Science
# Project the graph embedding
g0, res = gds.graph.project(
    'embedding-projection', 
    ['content', 'users','keyword' ],
    {
        'like':{'orientation':'UNDIRECTED'},
        'has_keyword':{'orientation':'UNDIRECTED'},
    },
)

gds.fastRP.mutate(g0, mutateProperty='embedding', embeddingDimension=256, randomSeed=7474);

gds.graph.writeNodeProperties(g0, ["embedding"], ["content","users"])

## Centrality
# ArticleRank is a variant of the Page Rank algorithm, which measures the transitive influence of nodes.
gds.run_cypher('''
    CALL gds.articleRank.mutate('embedding-projection', {
      mutateProperty: 'centrality'
    })
    YIELD nodePropertiesWritten, ranIterations
    '''
)

gds.graph.writeNodeProperties(g0, ["centrality"], ["content"])

## Page Rank
gds.run_cypher('''
    CALL gds.pageRank.mutate('embedding-projection', {
      maxIterations: 20,
      dampingFactor: 0.85,
      mutateProperty: 'pagerank'
    })
    YIELD nodePropertiesWritten, ranIterations
    '''
)
gds.graph.writeNodeProperties(g0, ["pagerank"], ["content"])