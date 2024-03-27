"""
This script collects data from the BoardgameGeek API.
No authentication mechanism is necessary.

As result, it will populate the folder `../raw_data/` with the boardgame collection (a list of games with their ratings) a user has inside the folder `../raw_data/collection/` and also the list of reviews a boardgame has inside `../raw_data/boardgame/`.
The reviews are split into multiple files since there are multiple "pages" of reviews. Each file corresponds to a single review page, containing up to 100 reviews.

The two main functions here are `get_collection_of_user` and `users_who_rated_game`. The first function can be used to get the collection of a single user, while the second function is used to mine a list of users to be queried.
"""

import xml.etree.ElementTree as ET
import requests
import os
import time
import math
from typing import List


SLEEP_BEFORE_QUEUE = 10  # Wait for 10s before checking if the request is ready
SLEEP_BETWEEN_REQUESTS = 5


def get_collection_of_user(user: str, queued_users: List[str]) -> str:
    """
    Requests the collection of @user.
    It has a cache mechanism where it will try to first use the stored result of the request.
    Therefore, it only goes to BoardgameGeek in case the file doesn't exist.

    If it does not receive the reply right away, it will add the user to @queued_users which later should be queried.

    It returns the xml string corresponding to the collection of that user.
    """
    fname = f'../raw_data/collection/{user}.xml'

    if os.path.isfile(fname):
        # No need to call the API again
        with open(fname, 'r') as f:
            r_text = f.read()
            return r_text
    else:
        # Request the API
        time.sleep(SLEEP_BETWEEN_REQUESTS)
        r = requests.get(f'https://boardgamegeek.com/xmlapi/collection/{user}')
        if r.status_code == 200:
            # Success! Store the file
            fname = f'../raw_data/collection/{user}.xml'
            with open(fname, 'w') as f:
                f.write(r.text)
        elif r.status_code == 202:
            # Request is in the queue
            queued_users.append(user)
        else:
            print(f"ERROR (?): Return status code: {r.status_code}")

        return ""


def request_users_who_rated_game(game_id: int, page: int, queued_games: List[str]) -> str:
    """
    Basic function to collect a single review @page of a @game_id.
    """

    time.sleep(SLEEP_BETWEEN_REQUESTS)
    r = requests.get(f'https://boardgamegeek.com/xmlapi2/thing?id={game_id}&ratingcomments=1&page={page}')

    path = f'../raw_data/boardgame/{game_id}'

    if not os.path.exists(path):
        os.makedirs(path)

    if r.status_code == 200:
        # Success! Store the file
        fname = f'{path}/{page}.xml'

        with open(fname, 'w') as f:
            f.write(r.text)
            return r.text
    elif r.status_code == 202:
        # Request is in the queue
        queued_games.append(user)
    else:
        print(f"ERROR (?): Return status code: {r.status_code}")


def grab_all_pages(game_id: int, root, queued_games):
    """
    This function parses an xml element @root to find out how many total review pages are there for that specific @game_id and then get all the pages for that game.
    Each page is then stored in a separated file inside `../raw_data/boardgame/@game_id/<page>`.
    """
    for c in root[0]:
        if c.tag == 'comments':
            total_ratings = int(c.attrib['totalitems'])
            break

    total_pages = math.ceil(total_ratings / 100)
    for pnumber in range(2, total_pages):
        path = f'../raw_data/boardgame/{game_id}'
        fname = f'{path}/{pnumber}.xml'

        if os.path.isfile(fname):
            # No need to call the API again
            with open(fname, 'r') as f:
                r_text = f.read()
        else:
            request_users_who_rated_game(game_id, pnumber, queued_games)


def get_reviews_of_game(game_id: int, queued_games: List[str]) -> List[str]:
    """
    This function populates the directory `../raw_data/boardgame/@game_id` with all the reviews that game received.
    """
    path = f'../raw_data/boardgame/{game_id}'
    page = 1  # Start with the first page of reviews
    fname = f'{path}/{page}.xml'

    if not os.path.exists(path):
        os.makedirs(path)

    if os.path.isfile(fname):
        # No need to call the API again
        with open(fname, 'r') as f:
            r_text = f.read()
    else:
        r_text = request_users_who_rated_game(game_id, page, queued_games)

    root = ET.fromstring(r_text)
    grab_all_pages(game_id, root, queued_games)
    # print("Queued Games:", len(queued_games), queued_games)

    # users = []
    # for c in root[0]:
    #     if c.tag == 'comments':
    #         total_ratings = c.attrib['totalitems']
    #         for comment in c:
    #             username = comment.attrib['username']
    #             rating = comment.attrib['rating']
    #             users.append(username)

    # return users


def users_who_rated_game(game_id: int) -> List[str]:
    """
    Returns a list of users who rated @game_id.
    This should usually be called after `get_reviews_of_game`.
    """
    users = set()
    path = f'../raw_data/boardgame/{game_id}'
    xmls = os.listdir(path)

    for xml in xmls:
        # Each xml here corresponds to a page of reviews
        fname = f'{path}/{xml}'
        with open(fname, 'r') as f:
            r_text = f.read()

        root = ET.fromstring(r_text)

        for c in root[0]:
            if c.tag == 'comments':
                total_ratings = c.attrib['totalitems']
                for comment in c:
                    username = comment.attrib['username']
                    rating = comment.attrib['rating']
                    users.add(username)

    return list(users)


if __name__ == "__main__":
    queued_users = []
    queued_games = []
    failed_users = []

    # Dune Imperium
    # get_reviews_of_game(game_id=316554, queued_games=queued_games)
    # users = users_who_rated_game(game_id=316554)

    # Spirit Island
    get_reviews_of_game(game_id=162886, queued_games=queued_games)
    users = users_who_rated_game(game_id=162886)

    for user in users:
        get_collection_of_user(user, queued_users)

    print(f"Requests in queue: {len(queued_users)}")
    failed_users = []

    if queued_users:
        time.sleep(SLEEP_BEFORE_QUEUE)
        for user in queued_users:
            get_collection_of_user(user, failed_users)
