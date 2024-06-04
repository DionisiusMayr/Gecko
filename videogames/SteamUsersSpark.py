from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, lit, ceil
import json
import math

OUTPUT_PARQUET_FILE = 'steam_users.parquet'

spark = SparkSession.builder \
    .appName("SteamUsers") \
    .config("spark.rpc.message.maxSize", "2") \
    .getOrCreate()

# Load player_summaries.json
with open('player_summaries.json', 'r') as f:
    player_summaries_data = json.load(f)

# Load steam_profiles.json
with open('steam_profiles.json', 'r') as f:
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
                common_rows.append({'user_id': personaname, 'type': 'Steam', 'content_id': appid, 'temp_rating': playtime_forever})

# Create Spark DataFrame
common_df = spark.createDataFrame(common_rows)

# Calculate max playtime_forever for each user_id
max_playtime = common_df.groupBy('user_id').agg(spark_max('temp_rating').alias('max_temp_rating'))

# Join max_playtime with common_df to calculate normalized ratings
common_df = common_df.join(max_playtime, on='user_id')
common_df = common_df.withColumn('rating', (col('temp_rating') / col('max_temp_rating')) * 10)

# Apply ceiling to the ratings
common_df = common_df.withColumn('rating', ceil(col('rating')))

# Drop the 'temp_rating' and 'max_temp_rating' columns
common_df = common_df.drop('temp_rating', 'max_temp_rating')

# Add a new column 'rating_date' filled with null values
common_df = common_df.withColumn('rating_date', lit(None).cast('string'))

# Display the Spark DataFrame
common_df.show(10)

# Take a sample of the data. Comment or uncomment 
# sample_df = common_df.sample(withReplacement=False, fraction=0.001)
# sample_df.write.parquet('sample_steam_users.parquet')

# Save DataFrame as Parquet file
common_df.write.parquet(OUTPUT_PARQUET_FILE)

spark.stop()
