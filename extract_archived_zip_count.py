import requests
import zipfile
from utils.utilities import *

url = 'https://melbournetestbed.opendatasoft.com/api/datasets/1.0/pedestrian-counting-system-monthly-counts-per-hour/attachments/pedestrian_counting_system_monthly_counts_per_hour_may_2009_to_14_dec_2022_csv_zip/'

save_path = 'historical_data'
filename = 'zipfile.zip'
file_path =f'{save_path}{OS_PATH_PARTITION}{filename}'
check_directory(save_path)
response = requests.get(url)
response.raise_for_status()  # Check if the request was successful

with open(file_path, "wb") as file:
    file.write(response.content)

print("Zip file downloaded successfully.")


with zipfile.ZipFile(file_path, "r") as zip_ref:
    zip_ref.extractall(save_path)

print("Zip file extracted successfully.")