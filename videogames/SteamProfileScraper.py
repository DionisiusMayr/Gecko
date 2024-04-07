import requests
import json
import pandas as pd
import os
import time
from json.decoder import JSONDecodeError

API_KEY = '6B09F8A45ACB9DAA88DBB9056F499BC2' #my API key, change if someone else
OUTPUT_JSON_FILE = 'steam_profiles.json'
OUTPUT_PARQUET_FILE = 'steam_profiles.parquet'

def get_owned_games(steamid):
    url = f'http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/?key={API_KEY}&steamid={steamid}&include_played_free_games=TRUE&format=json'
    response = requests.get(url)
    try:
        return response.json()
    except JSONDecodeError:
        print(f"Error decoding JSON response for SteamID: {steamid}")
        return None

def save_as_parquet(results):
    # Convert JSON to DataFrame
    df = pd.json_normalize(results)

    # Save DataFrame as Parquet file
    df.to_parquet(OUTPUT_PARQUET_FILE)

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
        data = get_owned_games(steamid)
        if data is not None and 'response' in data and data['response']:
            if 'games' in data['response']:
                results.append({steamid: data['response']['games']})
        steamid += 1

        if steamid % 100 == 0:
            with open(OUTPUT_JSON_FILE, 'w') as file:
                json.dump(results, file, indent=4)
            print(f"Data saved up to SteamID: {steamid}")

            # Save as Parquet file
            save_as_parquet(results)
            print(f"Parquet file saved up to SteamID: {steamid}")

        time.sleep(1.5)  # Rate limit set to 1.5 seconds

    with open(OUTPUT_JSON_FILE, 'w') as file:
        json.dump(results, file, indent=4)
    
    # Save as Parquet file for the entire dataset
    save_as_parquet(results)
    
    print("Script completed successfully.")

if __name__ == "__main__":
    main()
