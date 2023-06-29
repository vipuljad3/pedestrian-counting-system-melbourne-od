import yaml
import os
import datetime
import platform

OS_PATH_PARTITION = '\\' if platform.system() == 'Windows' else '/'
DATA_INGESTION_DIRECTORY = 'ingestion'
DATA_DIRECTORY = 'data'

## Reads the config file with yaml extension
def read_config(file):
    with open(file, "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

def check_directory(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory '{directory_path}' created successfully.")
    else:
        print(f"Directory '{directory_path}' already exists.")

def remove_files_in_directory(directory_path):
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)
            print(f"File '{filename}' removed successfully.")

def stage_data(namespace, path, df, type, remove_flag):
    filename = datetime.datetime.now()
    file_path = f'{DATA_INGESTION_DIRECTORY}/{namespace}/{DATA_DIRECTORY}/{path}/{OS_PATH_PARTITION}/'
    check_directory(file_path)
    if remove_flag == True:
        remove_files_in_directory(file_path)
    if type.lower() == 'csv':
        file_end_path = f"{file_path}{filename}.{type.lower()}"
        df.to_csv(file_end_path, index = False)
    else:
        exit()
    return file_end_path