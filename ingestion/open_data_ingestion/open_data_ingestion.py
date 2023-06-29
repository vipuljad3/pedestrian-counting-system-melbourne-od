import requests
import pandas as pd
from datetime import datetime
from dateutil.tz import tzutc
import utils.utilities

NAMESPACE = 'open_data_ingestion'

def open_api_handler(config, database):
    url = config['open_api_url']
    if config[NAMESPACE][database]['lookback'] == False:
        date = None
        df = open_api_to_df(url, database, date)
    else:
        lookback_days= config[NAMESPACE][database]['lookback_days']
        df = lookback_collect(url, database, lookback_days)
    field_list = list(df['fields'])
    df = pd.DataFrame(field_list)
    staging_path = utils.utilities.stage_data(NAMESPACE, database, df, 'csv', config[NAMESPACE][database]['remove_staged'])
    return staging_path

def lookback_collect(url, database, lookback_days):
    today = datetime.now()
    subset_date = pd.to_datetime(today) - pd.DateOffset(days=lookback_days)
    dates = pd.date_range(start=subset_date, end=today)
    # Print each date
    date_list = [date.strftime('%Y/%m/%d') for date in dates]
    print("Looking for these dates: \n",date_list)
    df = pd.DataFrame()
    for date in date_list:
        batch = open_api_to_df(url, database, date)
        print(len(batch))
        df = pd.concat([df, batch], ignore_index=True)
    return df

def open_api_to_df(url, database, date):
    if date != None:
        params = {
            "dataset": database,  
            "rows": 10000,
            "start": 0, 
            "q":f"record_timestamp = {date}",
            "sort": ["timestamp"], 
            "format": "json", 
            "timezone": "UTC"  
        }
    else : 
        params = {
        "dataset": database,  
        "rows": 10000,
        "start": 0, 
        "format": "json", 
        "timezone": "UTC"  
    }       
    print(params)
    # Make the GET request
    response = requests.get(url, params=params)
    # Make sure the request was successful
    response.raise_for_status()
    # Convert the response to JSON
    data = response.json()
    # Extract the records from the data
    records = data['records']
    # Convert the records to a pandas DataFrame
    df = pd.DataFrame(records)
    #print(df)
    return(df)


def subset_date(df, lookback_date_column, lookback_days):
    df[lookback_date_column] = pd.to_datetime(df[lookback_date_column])
    # Get today's date
    now = datetime.now(tzutc())
    # Get the date n days ago
    subset_date = pd.to_datetime(now - pd.DateOffset(days=lookback_days))
    if min(df[lookback_date_column] <= subset_date):
        df.to_csv('test.csv')
        df_lookback_days = df[df[lookback_date_column] >= subset_date]
        return df_lookback_days
    else:
        return None


