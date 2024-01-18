from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

# Code for ETL operations on Country-GDP data

# Importing the required libraries

url = 'https://en.wikipedia.org/wiki/List_of_largest_banks'
db_name = 'Banks.db'
table_name = 'Largest_banks'
table_attribs =['Name', 'MC_USD_Billion']
csv_path = './exchange_rate.csv'
output_path = './Largest_banks_data.csv'
log_file = 'code_log.txt'


# Step 1: Logging function
def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file.'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ' : ' + message + '\n')    

# Step 2 : Extraction of data
def extract(url, table_attribs):
    #1: Extract the web page as text
    page = requests.get(url).text
    #2: Parse the text into an HTML object
    data = BeautifulSoup(page, 'html.parser')
    #3: Create an empty pandas DataFrame named df with cols as the table_attribs
    df = pd.DataFrame(columns=table_attribs)

    #4: Find the table with class 'wikitable'
    table = data.find('table', {'class': 'wikitable'})

    # Find all rows in the table body
    rows = table.find('tbody').find_all('tr')
    #5: Check the contents of each row, having attribute ‘td’, for the following conditions
    for row in rows:
        # Extract columns from each row
        columns = row.find_all('td')

        # Check if there are at least three columns
        if len(columns) >= 3:
            # Extracted Name
            name = columns[1].text.strip()

            # Extracted MC_USD_Billion
            mc_usd_billion = float(columns[2].text.strip())

            # Store all entries matching the conditions in a dictionary
            data_dict = {'Name': name, 'MC_USD_Billion': mc_usd_billion}

            # Append the dictionary to the DataFrame
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df


# Step3 : Transformation of data
def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three cols to the data frame, each
    containing the transformed version of Market Cap col to
    respective currencies'''
    # Step 3.1: Read the exchange rate CSV file and convert to a dictionary
    exchange_rate = pd.read_csv(csv_path).set_index('Currency').to_dict()['Rate']

    # Task 3.2: Add columns MC_GBP_Billion, MC_EUR_Billion, MC_INR_Billion
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

    return df

# Step4: Loading to CSV
def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path.'''
    df.to_csv(output_path)

# Step 5: Loading to Database
def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

# Step 6: Function to Run queries on Database
def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

# Funtion calls
log_progress('Preliminaries complete. Initiating ETL process')

log_progress('Data extraction complete. Initiating Transformation process')
df = extract(url, table_attribs)

log_progress('Data transformation complete. Initiating Loading process')
df = transform(df, csv_path)

log_progress('Data saved to CSV file')
load_to_csv(df, output_path)

log_progress('SQL Connection initiated')
sql_connection = sqlite3.connect(db_name)

log_progress('Loading to Database initiated')
load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as a table, Executing queries')
query_statement_1 = f"SELECT * FROM {table_name}"
query_statement_2 = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
query_statement_3 = f"SELECT Name from {table_name} LIMIT 5"

run_query(query_statement_1, sql_connection)
run_query(query_statement_2, sql_connection)
run_query(query_statement_3, sql_connection)

log_progress('Process Complete')
log_progress('Server Connection closed')
sql_connection.close()

