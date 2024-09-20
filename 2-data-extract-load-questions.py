# Extract: Process to pull data from Source system
# Load: Process to write data to a destination system

# Common upstream & downstream systems
# OLTP Databases: Postgres, MySQL, sqlite3, etc
# OLAP Databases: Snowflake, BigQuery, Clickhouse, DuckDB, etc
# Cloud data storage: AWS S3, GCP Cloud Store, Minio, etc
# Queue systems: Kafka, Redpanda, etc
# API
# Local disk: csv, excel, json, xml files
# SFTP\FTP server

# Databases: When reading or writing to a database we use a database driver. Database drivers are libraries that we can use to read or write to a database.
# Question: How do you read data from a sqlite3 database and write to a DuckDB database?
# Hint: Look at importing the database libraries for sqlite3 and duckdb and create connections to talk to the respective databases

# Fetch data from the SQLite Customer table

# Insert data into the DuckDB Customer table

# Hint: Look for Commit and close the connections
# Commit tells the DB connection to send the data to the database and commit it, if you don't commit the data will not be inserted

# We should close the connection, as DB connections are expensive
import sqlite3
import duckdb

sqlite_conn = sqlite3.connect('example_sqlite.db')
sqlite_cursor = sqlite_conn.cursor()

sqlite_cursor.execute("SELECT * FROM Customer")
customer_data = sqlite_cursor.fetchall()

duckdb_conn = duckdb.connect('example_duckdb.db')
duckdb_cursor = duckdb_conn.cursor()



for row in customer_data:
    duckdb_cursor.execute("INSERT INTO Customer VALUES (?, ?, ?)", row)

duckdb_conn.commit()
sqlite_conn.close()
duckdb_conn.close()

# Cloud storage
# Question: How do you read data from the S3 location given below and write the data to a DuckDB database?
# Data source: https://docs.opendata.aws/noaa-ghcn-pds/readme.html station data at path "csv.gz/by_station/ASN00002022.csv.gz"
# Hint: Use boto3 client with UNSIGNED config to access the S3 bucket
# Hint: The data will be zipped you have to unzip it and decode it to utf-8

# AWS S3 bucket and file details
bucket_name = "noaa-ghcn-pds"
file_key = "csv.gz/by_station/ASN00002022.csv.gz"
# Create a boto3 client with anonymous access

# Download the CSV file from S3
# Decompress the gzip data
# Read the CSV file using csv.reader
# Connect to the DuckDB database (assume WeatherData table exists)

# Insert data into the DuckDB WeatherData table
pip install boto3 duckdb

import boto3
import gzip
import csv
import duckdb
from botocore import UNSIGNED
from botocore.config import Config


s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))


response = s3.get_object(Bucket=bucket_name, Key=file_key)
gzipped_data = response['Body'].read()

Decompress the gzip data
csv_data = gzip.decompress(gzipped_data).decode('utf-8')

#Read the CSV file 
csv_reader = csv.reader(csv_data.splitlines())


# Connect to DuckDB database 
duckdb_conn = duckdb.connect('weather_data.db')


# Insert data into the DuckDB WeatherData table

# Skip the header if it exists
next(csv_reader)

# Insert rows into the WeatherData table
insert_query = "INSERT INTO WeatherData VALUES (?, ?, ?, ?, ?)"
for row in csv_reader:
    
    duckdb_conn.execute(insert_query)  

duckdb_conn.commit()
duckdb_conn.close()

print("Data successfully loaded from S3 to DuckDB!")


# API
# Question: How do you read data from the CoinCap API given below and write the data to a DuckDB database?
# URL: "https://api.coincap.io/v2/exchanges"
# Hint: use requests library

# Define the API endpoint
url = "https://api.coincap.io/v2/exchanges"

# Fetch data from the CoinCap API
# Connect to the DuckDB database

# Insert data into the DuckDB Exchanges table
# Prepare data for insertion
# Hint: Ensure that the data types of the data to be inserted is compatible with DuckDBs data column types in ./setup_db.py
pip install requests

import requests
import duckdb

url = "https://api.coincap.io/v2/exchanges"

# Fetch data from the CoinCap API
response = requests.get(url)
if response.status_code == 200:
    exchanges_data = response.json()["data"]
else:
    print(f"Failed to fetch data: {response.status_code}")
    exchanges_data = []

# Connect to DuckDB
duckdb_conn = duckdb.connect('crypto_data.db')



# Prepare data for insertion
insert_query = '''
    INSERT INTO Exchanges 
    (id, name, rank, percentTotalVolume, volumeUsd, tradingPairs, socket, exchangeUrl, updated) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
'''

# Insert data into DuckDB
for exchange in exchanges_data:
    
    row = (
        exchange.get("id"),
        exchange.get("name"),
        int(exchange.get("rank", 0)),  # Rank as integer
        float(exchange.get("percentTotalVolume", 0.0)),  # Volume percentage as float
        float(exchange.get("volumeUsd", 0.0)),  # USD volume as float
        exchange.get("tradingPairs", ""),
        bool(exchange.get("socket", False)),  # Convert socket value to boolean
        exchange.get("exchangeUrl", ""),
        exchange.get("updated", "")
    )
    
    duckdb_conn.execute(insert_query, row)

# Step 7: Commit and close the DuckDB connection
duckdb_conn.commit()
duckdb_conn.close()



# Local disk
# Question: How do you read a CSV file from local disk and write it to a database?
# Look up open function with csvreader for python
import csv
import sqlite3

# establish database connection
connection = sqlite3.connect('database_path')  
cursor = connection.cursor()


# Read the CSV file from local disk
with open('sales_data.csv', newline='') as csvfile:
    csvreader = csv.reader(csvfile)
    
    #Skip the header row (optional, depending on your CSV file)
    next(csvreader)
    
    #Insert each row into the database
    for row in csvreader:
        year = int(row[0])
        sales = float(row[1])
        
        cursor.execute('''
            INSERT INTO sales_data (year, sales)
            VALUES (?, ?)
        ''', (year, sales))

# Commit the transaction and close the connection
connection.commit()
connection.close()

# Web scraping
# Questions: Use beatiful soup to scrape the below website and print all the links in that website
# URL of the website to scrape
url = 'https://example.com'

import requests
from bs4 import BeautifulSoup

#get data from the website using requests

response = requests.get(url)

# extract HTML content
soup = BeautifulSoup(response.text, 'html.parser')

# Example: Find and print all the links on the webpage
for b in soup.find_all('a'):
    print(b.get('href'))
