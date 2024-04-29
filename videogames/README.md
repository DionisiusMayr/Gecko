# Getting Steam Games
In order to get the JSON file of steam games, I used another github. 
```sh
gh repo clone FronkonGames/Steam-Games-Scraper
```
After cloning the repo, follow the README to get the JSON file.

# Getting Steam Profiles
First, check the SteamProfileGameScraper.py file and adjust it for your own personal steam API key. You can acquire one at https://steamcommunity.com/dev/apikey
Next, run the python file with
```sh
python3 SteamProfileGameScraper.py
```
Similarly, run the SteamProfileSumScraper.py and adjust it for your API key. 
```sh
python3 SteamProfileSumScraper.py
```
Then, you will have steam_profiles.json and parquet files, and player_summaries.json and parquet files.
