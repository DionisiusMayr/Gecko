import os
import json
from pyspark.sql import SparkSession
from pyspark.sql import Row

OUTPUT_PARQUET_FILE = 'SteamGames.parquet'

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SteamGames") \
    .config("spark.rpc.message.maxSize", "512") \
    .getOrCreate()

# Load the dataset from games.json
dataset = {}
if os.path.exists('games.json'):
    with open('games.json', 'r', encoding='utf-8') as fin:
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
    
    rows.append(Row(content_id=app_id, content_title=name, release_year=release_year, description=description))

# Create Spark DataFrame
df = spark.createDataFrame(rows)

# Display the first few rows of the DataFrame
df.show()

# Save DataFrame as Parquet file
df.write.parquet(OUTPUT_PARQUET_FILE)

# Stop Spark session
spark.stop()
