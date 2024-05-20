#Structure of combined Steam file
# Username id, type of content, id of content, rating (0,10), date of rating

# Table of content(platform dependant)
# Table of username (platform dependant)

from pyspark.sql import SparkSession
import json
import pandas as pd
import math
import numpy as np

OUTPUT_PARQUET_FILE = 'steam_users.parquet'

spark = SparkSession.builder.getOrCreate()

# Load player_summaries.json
with open('player_summaries.json', 'r') as f:
    player_summaries_data = json.load(f)

# Load steam_profiles.json
with open('steam_profiles.json', 'r') as f:
    steam_profiles_data = json.load(f)

# Initialize lists to store data
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
                common_rows.append({'user_id': personaname, 'type': 'Steam', 'content_id': appid, 'temp_rating': playtime_forever})

# Create common dataframe
common_df = pd.DataFrame(common_rows)

# Calculate max playtime_forever for each user_id
max_playtime = common_df.groupby('user_id')['temp_rating'].max()

# 'Normalize' ratings from 0 to 10
common_df['rating'] = common_df.groupby('user_id')['temp_rating'].transform(lambda x: (x / x.max()) * 10)
common_df['rating'] = common_df['rating'].apply(lambda x: math.ceil(x))

# Drop the 'temp_rating' column
common_df.drop(columns=['temp_rating'], inplace=True)

# Add a new column 'rating_date' filled with null values
common_df['rating_date'] = np.nan

# Display the Pandas Dataframe
print(common_df.head())

# Save DataFrame as Parquet file
common_df.to_parquet(OUTPUT_PARQUET_FILE)

#Load Parquet file as Spark Dataframe
videogames = spark.read.parquet(OUTPUT_PARQUET_FILE)

#Display Spark Dataframe
videogames.show(10)
videogames.printSchema()