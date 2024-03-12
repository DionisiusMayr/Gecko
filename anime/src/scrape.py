#!/usr/bin/env python3
import requests
import datetime
import time
import json
import os
from jikanpy import Jikan

TOTAL_REQUESTS_PER_SECOND = 2
PREVIOUS_REQUEST_TIME = datetime.datetime.now()
MAX_RETRIES = 3
jikan = Jikan()
TOTAL_NEW_ENTRIES = 0

def directory_up(path: str, n: int):
    for _ in range(n):
        path = directory_up(path.rpartition("/")[0], 0)
    return path

def change_directory_root():
    root_path = os.path.dirname(os.path.realpath(__file__))
    # Change working directory to root of the project.
    os.chdir(directory_up(root_path, 1))
def make_directories():
    '''
    Creates neccesary directories if they don't exist.
    '''
    necessary_folders = [
        'user_anime_list',
        'user_info',
        'anime_info',
        'anime_reviews'
    ]
    for folder in necessary_folders:
        folder_route = './data/'+folder
        if not os.path.exists(folder_route):
            os.makedirs(folder_route)


def wait_for_request():
    '''
    Waits for a request in order to not get blacklisted by MAL
    '''
    min_waiting_in_ms = (1000.0/2)
    current_waiting_time_in_ms = (datetime.datetime.now() - PREVIOUS_REQUEST_TIME).microseconds
    if min_waiting_in_ms <= current_waiting_time_in_ms:
        return
    time_to_wait_in_ms = min_waiting_in_ms - current_waiting_time_in_ms
    time.sleep(time_to_wait_in_ms / 1000)


def request_user_anime_list(username, retry = 0, offset = 0):
    wait_for_request()
    data = requests.get('https://myanimelist.net/animelist/{}/load.json?offset={}'.format(username, offset))
    if retry >= MAX_RETRIES:
        return None
    if data.status_code != 200 and retry < MAX_RETRIES:
        return request_user_anime_list(username, retry = retry+1)

    json_array = data.json()
    # 300 is the pagination size of MAL
    if len(json_array) % 300 == 0 :
        return json_array + request_user_anime_list(username, offset = offset + 300)
    return json_array

def request_anime_reviews(anime_id, retry = 0):
    wait_for_request()
    data = requests.get('https://api.jikan.moe/v4/anime/{}/reviews'.format(anime_id))
    if retry >= MAX_RETRIES:
        return None
    if data.status_code != 200 and retry < MAX_RETRIES:
        return request_anime_reviews(anime_id, retry = retry+1)

    json_array = data.json()
    return json_array

# TODO should I keep this function
def request_user_info(username , retry = 0):
    wait_for_request()
    try:
        data = jikan.users(username=username)
    except:
        if retry < MAX_RETRIES :
            data = request_user_info(username= username, retry= retry+1)
        else :
            data = None
    return data

def request_random_user_info():
    wait_for_request()
    data = requests.get('https://api.jikan.moe/v4/random/users')
    if data.status_code != 200:
        return None
    return data.json()

def request_anime_info(anime_id , retry = 0):
    wait_for_request()
    try:
        data = jikan.anime(anime_id)
    except:
        if retry < MAX_RETRIES :
            data = request_anime_info(anime_id= anime_id, retry= retry+1)
        else :
            data = None
    return data

def save_json(data, path):
    if data is None:
        return
    global TOTAL_NEW_ENTRIES
    TOTAL_NEW_ENTRIES += 1
    if TOTAL_NEW_ENTRIES % 100 == 0:
        print('Saved {} new files'.format(TOTAL_NEW_ENTRIES))
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def get_json(path):
    if os.path.isfile(path):
        with open(path) as f:
            return json.load(f)
    return None

def get_user_name_list_path(username):
    return 'data/user_anime_list/{}.json'.format(username)

def get_user_info_path(username):
    return 'data/user_info/{}.json'.format(username)

def get_anime_info_path(anime_id):
    return 'data/anime_info/{}.json'.format(anime_id)

def get_anime_review_path(anime_id):
    return 'data/anime_reviews/{}.json'.format(anime_id)

def get_user_anime_list(username):
    path = get_user_name_list_path(username)
    data = get_json(path)
    if data is None:
        data = request_user_anime_list(username)
        save_json(data, path)
    return data

# TODO should I keep this function
def get_user_info(username):
    path = get_user_info_path(username)
    data = get_json(path)
    if data is None:
        data = request_user_info(username)
        save_json(data, path)
    return data

def get_anime_info(anime_id):
    path = get_anime_info_path(anime_id)
    data = get_json(path)
    if data is None:
        data = request_anime_info(anime_id)
        save_json(data, path)
    return data

def get_anime_reviews(anime_id):
    path = get_anime_review_path(anime_id)
    data = get_json(path)
    if data is None:
        data = request_anime_reviews(anime_id)
        save_json(data, path)
    return data

def save_user_info(data):
    path = get_user_info_path(data['data']['username'])
    save_json(data, path)

def main():
    change_directory_root()
    make_directories()
    usernames_to_query = []

    while True:
        if len(usernames_to_query) == 0:
            random_user = request_random_user_info()
            save_user_info(random_user)
            username = random_user['data']['username']
        else:
            username = usernames_to_query.pop()
            get_user_info(username)


        random_user_anime_list = get_user_anime_list(username)
        if random_user_anime_list is not None:
            for anime in random_user_anime_list:
                get_anime_info(anime['anime_id'])
                anime_reviews = get_anime_reviews(anime['anime_id'])
                if anime_reviews is None:
                    continue
                for user in anime_reviews['data']:
                    usernames_to_query.append(user['user']['username'])

if __name__ == "__main__":
    main()
