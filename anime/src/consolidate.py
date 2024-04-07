import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def directory_up(path: str, n: int):
    for _ in range(n):
        path = directory_up(path.rpartition("/")[0], 0)
    return path

def change_directory_root():
    root_path = os.path.dirname(os.path.realpath(__file__))
    # Change working directory to root of the project.
    os.chdir(directory_up(root_path, 1))

def make_directory():
    '''
    Creates neccesary directories if they don't exist.
    '''
    folder_route = './parquets/'
    if not os.path.exists(folder_route):
        os.makedirs(folder_route)

def read_jsons_to_dataframe(file_names, folder_path):
    data = []

    for filename in file_names:
        if filename.endswith('.json'):
            with open(os.path.join(folder_path, filename), 'r') as file:
                json_content = json.load(file)
                data.append(json_content)

    df = pd.DataFrame({'id': file_names, 'json': data})
    df.json = df.json.astype(str)
    return df

def generate_partitioned_parquets_for_folder_path(folder_name):
    folder_path = f'./data/{folder_name}'
    files = os.listdir(folder_path)
    pagination_size = 1
    elements = 0
    cnt = 0
    while elements < len(files):
        cnt += 1
        elements += pagination_size
        dataframe = read_jsons_to_dataframe(
            files[pagination_size*(cnt-1):min(pagination_size*cnt, len(files))],
            folder_path
        )
        output_file = f'./parquets/{folder_name}_{cnt}.parquet'
        write_dataframe_to_parquet(dataframe, output_file)
        print(f"Parquet file '{output_file}' has been generated successfully.")


def write_dataframe_to_parquet(dataframe, output_file):
    table = pa.Table.from_pandas(dataframe)
    pq.write_table(table, output_file)

def generate_parquet_for_folder_path(folder_path, output_file):
    files = os.listdir(folder_path)
    dataframe = read_jsons_to_dataframe(files, folder_path)
    write_dataframe_to_parquet(dataframe, output_file)
    print(f"Parquet file '{output_file}' has been generated successfully.")


def main ():
    change_directory_root()
    make_directory()
    folder_names = [
        'anime_info',
        'anime_reviews',
        'user_info'
    ]
    generate_partitioned_parquets_for_folder_path('user_anime_list')

    for folder_name in folder_names:
        generate_parquet_for_folder_path(
            f'./data/{folder_name}',
            f'./parquets/{folder_name}.parquet'
        )

if __name__ == "__main__":
    main()
