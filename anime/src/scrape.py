#!/usr/bin/env python3
import os

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
    necessary_folders = ['user_anime_list', 'user_info', 'anime_info']
    for folder in necessary_folders:
        folder_route = './data/'+folder
        if not os.path.exists(folder_route):
            os.makedirs(folder_route)

def main():
    change_directory_root()
    make_directories()

if __name__ == "__main__":
    main()
