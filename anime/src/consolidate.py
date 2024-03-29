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

def read_jsons_to_dataframe(folder_path):
    data = []
    file_names = []

    for filename in os.listdir(folder_path):
        if filename.endswith('.json'):
            with open(os.path.join(folder_path, filename), 'r') as file:
                json_content = json.load(file)
                data.append(json_content)
                file_names.append(filename)

    df = pd.DataFrame({'id': file_names, 'json': data})
    return df

def write_dataframe_to_parquet(dataframe, output_file):
    table = pa.Table.from_pandas(dataframe)
    pq.write_table(table, output_file)

def generate_parquet_for_folder_path(folder_path, output_file):
    change_directory_root()

    dataframe = read_jsons_to_dataframe(folder_path)
    write_dataframe_to_parquet(dataframe, output_file)
    print(f"Parquet file '{output_file}' has been generated successfully.")


def main ():
    make_directory()
    folder_names = [
        'anime_info',
        'anime_reviews',
        'user_info'
        # TODO uncomment this line, this is very heavy so i haven't runned it
        # 'user_anime_list',
    ]
    for folder_name in folder_names:
        generate_parquet_for_folder_path(
            f'./data/{folder_name}',
            f'./parquets/{folder_name}'
        )

if __name__ == "__main__":
    main()
