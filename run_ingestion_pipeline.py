import utils.utilities as utils_
import ingestion._init
import utils.databases as db
import pandas as pd
import os

environment = 'prod'
os.environ["environment"] = environment

NAMESPACE = 'open_data_ingestion'

config_path = 'ingestion/open_data_ingestion/config/config.yaml'
config = utils_.read_config(config_path)

for dataset, args in config[NAMESPACE].items():
    staging_path = ingestion._init.ingestion.open_data_ingestion.open_data_ingestion.open_api_handler(config, dataset)
    data = pd.read_csv(staging_path)
    db.load_data(data,NAMESPACE, dataset.replace('-','_'), args['db_load_type'])
    query = '''SELECT name FROM sqlite_master WHERE type='table' '''
    print(db.get_query_df(query))
