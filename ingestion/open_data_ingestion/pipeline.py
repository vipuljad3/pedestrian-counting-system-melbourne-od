import utils.utilities as utils_
import ingestion._init
import database.databases
import pandas as pd

NAMESPACE = 'open_data_ingestion'

config_path = 'ingestion/open_data_ingestion/config.yaml'
config = utils_.read_config(config_path)

for key, value in config.items():
    print(key,value)
# staging_path = ingestion._init.ingestion.open_data_ingestion.open_data_ingestion.open_api_handler(config, 'pedestrian-counting-system-monthly-counts-per-hour')
# data = pd.read_csv(staging_path)

# database.databases.load_data(data,config['namespace'],'pedestrian-counting-system-monthly-counts-per-hour')
# location_df = ingestion._init.ingestion.open_data_ingestion.open_data_ingestion.open_api_handler(config, 'pedestrian-counting-system-sensor-locations')
