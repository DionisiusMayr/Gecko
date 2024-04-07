import requests
import json
import time
from json.decoder import JSONDecodeError

API_KEY = '6B09F8A45ACB9DAA88DBB9056F499BC2' #my API key, change if someone else
OUTPUT_FILE = 'steam_profiles.json'

def get_owned_games(steamid):
    url = f'http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/?key={API_KEY}&steamid={steamid}&include_played_free_games=TRUE&format=json'
    response = requests.get(url)
    try:
        return response.json()
    except JSONDecodeError:  # Catch JSONDecodeError
        print(f"Error decoding JSON response for SteamID: {steamid}")
        return None

def main():
    steamid = 76561197960265730
    results = []

    while steamid < 76561198000000000:
        data = get_owned_games(steamid)
        if data is not None and 'response' in data and data['response']:
            if 'games' in data['response']:
                results.append({steamid: data['response']['games']})
        steamid += 1

        if steamid % 100 == 0:
            with open(OUTPUT_FILE, 'w') as file:
                json.dump(results, file, indent=4)
            print(f"Data saved up to SteamID: {steamid}")

        time.sleep(1.5)  # Rate limit set to 1.5 seconds

    with open(OUTPUT_FILE, 'w') as file:
        json.dump(results, file, indent=4)
    print("Script completed successfully.")

if __name__ == "__main__":
    main()