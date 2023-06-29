import sqlite3
import pandas as pd

def get_db_connection():
# Connect to the in-memory SQLite database
    try:
        connection = sqlite3.connect('belong.db')
        
        print("database connection successful")
        return connection
    except:
        print("failed to connect to the db")

def get_query_df(query):
    connection = get_db_connection()
    df = pd.read_sql(query, connection)
    connection.close()
    return df

def load_data(df,namespace, table_name, load_condition):
    connection = get_db_connection()
    table_name = f'{namespace}_{table_name}'
    print(table_name)
    df.to_sql(table_name, connection, if_exists=load_condition, index=False)
    connection.close()


