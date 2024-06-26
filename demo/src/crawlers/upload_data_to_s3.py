import os

import utils

S3 = utils.S3_conn()
BUCKET_LIST = ['raw-data', 'processed-data', 'unified-data']
BUCKET = 'raw-data'

def create_buckets():
    for b in BUCKET_LIST:
        S3.create_bucket(b)


def upload_boardgames():
    collection_path = './local_raw_data/boardgame/collection'
    xmls = os.listdir(collection_path)
    for xml in xmls:
        fkey = f"boardgame/collection/{xml}"
        fpath = f"{collection_path}/{xml}"
        S3.put_file_in_bucket(BUCKET, fkey, fpath)

    boardgame_path = './local_raw_data/boardgame/boardgame'
    folders = os.listdir(boardgame_path)
    for folder in folders:
        for xml in os.listdir(f"{boardgame_path}/{folder}"):
            fkey = f"boardgame/boardgame/{folder}/{xml}"
            fpath = f"{boardgame_path}/{folder}/{xml}"
            S3.put_file_in_bucket(BUCKET, fkey, fpath)

    S3.list_all_files_in_bucket(BUCKET)


def upload_movies():
    review_path = f'./local_raw_data/movies_reviews'
    reviews = os.listdir(review_path)
    for r in reviews:
        fkey = f"movie/review/{r}"
        fpath = f"{review_path}/{r}"
        S3.put_file_in_bucket(BUCKET, fkey, fpath)


    movie_path = f'./local_raw_data/movies_info'
    movies = os.listdir(movie_path)
    for m in movies:
        fkey = f"movie/info/{m}"
        fpath = f"{movie_path}/{m}"
        S3.put_file_in_bucket(BUCKET, fkey, fpath)


def upload_animes():
    info_path =  f'./local_raw_data/anime/anime_info'
    infos = os.listdir(info_path)
    for info in infos:
        fkey = f"anime/info/{info}"
        fpath = f"{info_path}/{info}"
        S3.put_file_in_bucket(BUCKET, fkey, fpath)

    user_path =  f'./local_raw_data/anime/user_anime_list'
    user_infos = os.listdir(user_path)
    for user_info in user_infos:
        fkey = f"anime/user_info/{user_info}"
        fpath = f"{user_path}/{user_info}"
        S3.put_file_in_bucket(BUCKET, fkey, fpath)

def upload_videogames():
    # Profile information of the player
    profiles = f'./local_raw_data/videogames/player_summaries.json'
    fkey = f'videogame/player_profile.json'
    S3.put_file_in_bucket(BUCKET, fkey, profiles)

    # Games which a player has played
    games = f'./local_raw_data/videogames/steam_profiles.json'
    fkey = f'videogame/games_played.json'
    S3.put_file_in_bucket(BUCKET, fkey, games)

    steam = f'./local_raw_data/videogames/games.json'
    fkey = f'videogame/steam_games.json'
    S3.put_file_in_bucket(BUCKET, fkey, steam)

def directory_up(path: str, n: int):
    for _ in range(n):
        path = directory_up(path.rpartition("/")[0], 0)
    return path

def set_working_directory_as_root():
    root_path = os.path.dirname(os.path.realpath(__file__))
    # Change working directory to root of the project.
    os.chdir(directory_up(root_path, 2))


if __name__ == '__main__':
    set_working_directory_as_root()
    create_buckets()
    S3.delete_everything_from_bucket(BUCKET)

    upload_boardgames()
    upload_movies()
    upload_animes()
    upload_videogames()

    S3.list_all_files_in_bucket(BUCKET)
