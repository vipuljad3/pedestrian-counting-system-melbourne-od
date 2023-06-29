import pandas as pd
import utils.databases as db
import pytz
import calendar
melbourne_tz = pytz.timezone('Australia/Melbourne')

#connection = db.get_db_connection()
sensor_locations = '''select * from open_data_ingestion_pedestrian_counting_system_sensor_locations'''
sensor_locations_df = db.get_query_df(sensor_locations)

counts = '''select * from open_data_ingestion_pedestrian_counting_system_monthly_counts_per_hour'''
counts_df = db.get_query_df(counts)
counts_df = counts_df[['timestamp', 'total_of_directions','locationid']]
counts_df['timestamp'] = pd.to_datetime(counts_df['timestamp'], format='%Y-%m-%dT%H:%M:%S%z', errors='coerce')
counts_df['timestamp'] = counts_df['timestamp'].dt.tz_convert(melbourne_tz)
counts_df['timestamp'] = counts_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')


archive_count = pd.read_csv('historical_data/Pedestrian_Counting_System_Monthly_counts_per_hour_may_2009_to_14_dec_2022.csv')
archive_count['Date_Time'] = pd.to_datetime(archive_count['Date_Time'], format='%B %d, %Y %I:%M:%S %p', errors='coerce')
new_column_names = {'Sensor_ID': 'locationid', 'Date_Time': 'timestamp', 'Hourly_Counts': 'total_of_directions'}
archive_count = archive_count.rename(columns=new_column_names)
counts_df = pd.concat([archive_count, counts_df], ignore_index= True)

counts_df['timestamp'] = pd.to_datetime(counts_df['timestamp'])
counts_df['Day'] = counts_df['timestamp'].dt.day_name()
#counts_df['Day'] = pd.to_datetime(counts_df['Day'], format='%w').dt.strftime('%A')
counts_df['Mdate'] = counts_df['timestamp'].dt.day
counts_df['Month'] = counts_df['timestamp'].dt.month
counts_df['Year'] = counts_df['timestamp'].dt.year
counts_df['Time'] = counts_df['timestamp'].dt.hour
#counts_df = counts_df[['locationid','timestamp','Year', 'Month', 'Time', 'Sensor_Name', 'total_of_directions']]

joined_results = pd.merge(counts_df, sensor_locations_df, left_on='locationid', right_on='location_id', how='left')
joined_results['Sensor_Name'].fillna(joined_results['sensor_description'], inplace=True)
joined_results = joined_results[['locationid','timestamp','Year', 'Day', 'Mdate', 'Month', 'Time', 'Sensor_Name', 'total_of_directions']]


# Top 10 locations by day
print('\nTop 10 locations by day: ')
top_10_locations_day = joined_results.groupby(['locationid', 'Sensor_Name', 'Day'])['total_of_directions'].sum().reset_index()
top_10_locations_day = top_10_locations_day.groupby('Day').apply(lambda x: x.nlargest(10, 'total_of_directions')).reset_index(drop=True)
top_10_locations_day = top_10_locations_day.sort_values(by=['Day', 'total_of_directions'], ascending=[True, False])
print(top_10_locations_day)

print('\nTop 10 locations each day: ')
top_locations_each_day = top_10_locations_day.groupby('Day').max('total_of_directions').reset_index()
print(top_locations_each_day)

# Top 10 locations by month
print('\nTop 10 locations by month:')
joined_results['Month'] = joined_results['timestamp'].dt.month
top_10_locations_month = joined_results.groupby(['locationid', 'Month'])['total_of_directions'].sum().reset_index()
top_10_locations_month = top_10_locations_month.groupby('Month').apply(lambda x: x.nlargest(10, 'total_of_directions')).reset_index(drop=True)
print(top_10_locations_month)

# Location with most decline due to lockdowns in the last 3 years
print('\nLocation with most decline due to lockdowns in the last 3 years: ')
lockdown_start_date = pd.to_datetime('2020-03-01')
lockdown_end_date = pd.to_datetime('2023-03-01')
lockdown_data = joined_results[(joined_results['timestamp'] >= lockdown_start_date) & (joined_results['timestamp'] < lockdown_end_date)]
decline_location = lockdown_data.groupby('locationid')['total_of_directions'].sum().idxmin()
print(joined_results.Sensor_Name[joined_results.locationid == decline_location].iloc[0])

# Location with most growth in the last year
print('\nLocation with most growth in the last year: ')
growth_start_date = pd.to_datetime('2022-06-29')
growth_end_date = pd.to_datetime('2023-06-29')
last_year = joined_results[joined_results['timestamp'].dt.year >= 2022]
growth_location = last_year.groupby('locationid').sum('total_of_directions')['total_of_directions'].idxmax()
print(joined_results.Sensor_Name[joined_results.locationid == growth_location].iloc[0])