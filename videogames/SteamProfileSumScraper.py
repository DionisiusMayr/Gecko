import requests
import json
import pandas as pd
import os
import time
from json.decoder import JSONDecodeError

API_KEY = '6B09F8A45ACB9DAA88DBB9056F499BC2'
OUTPUT_JSON_FILE = 'player_summaries.json'
# OUTPUT_PARQUET_FILE = 'player_summaries.parquet'
MAX_RETRIES = 3

def get_player_summary(steamid):
    url = f'http://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/?key={API_KEY}&steamids={steamid}&format=json'
    retries = 0
    while retries < MAX_RETRIES:
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.RequestException, JSONDecodeError) as e:
            print(f"Error fetching data for SteamID: {steamid}. Retrying... ({retries+1}/{MAX_RETRIES})")
            retries += 1
            time.sleep(2)  # Add a delay before retrying
    print(f"Failed to fetch data for SteamID: {steamid} after {MAX_RETRIES} retries.")
    return None

# def save_as_parquet(results):
#     # Convert JSON to DataFrame
#     df = pd.json_normalize(results)

#     # Save DataFrame as Parquet file
#     df.to_parquet(OUTPUT_PARQUET_FILE)

def main():
    start_steamid = 76561197960265730
    results = []

    # Check if JSON file exists
    if os.path.exists(OUTPUT_JSON_FILE):
        with open(OUTPUT_JSON_FILE, 'r') as file:
            existing_data = json.load(file)
            if existing_data:
                last_processed_steamid = max(existing_data[-1].keys())
                start_steamid = int(last_processed_steamid) + 1
                results.extend(existing_data)
        print(f"Resuming from SteamID: {start_steamid}")

    steamid = start_steamid

    while steamid < 76561198000000000:
        # Fetch player summary for one SteamID
        data = get_player_summary(steamid)
        
        if data is not None and 'response' in data and 'players' in data['response']:
            player_data = data['response']['players'][0] if len(data['response']['players']) > 0 else {}
            results.append({steamid: player_data})

        steamid += 1

        if steamid % 100 == 0:
            with open(OUTPUT_JSON_FILE, 'w') as file:
                json.dump(results, file, indent=4)
            print(f"Data saved up to SteamID: {steamid}")

            # # Save as Parquet file
            # save_as_parquet(results)
            # print(f"Parquet file saved up to SteamID: {steamid}")

        time.sleep(1.5)  # Rate limit set to 1.5 seconds

    with open(OUTPUT_JSON_FILE, 'w') as file:
        json.dump(results, file, indent=4)
    
    # # Save as Parquet file for the entire dataset
    # save_as_parquet(results)
    
    print("Script completed successfully.")

if __name__ == "__main__":
    main()
