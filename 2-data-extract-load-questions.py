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


# Local disk
# Question: How do you read a CSV file from local disk and write it to a database?
# Look up open function with csvreader for python
import csv
import sqlite3

# establish database connection
connection = sqlite3.connect('database_path')  
cursor = connection.cursor()


cursor.execute('''
    CREATE TABLE IF NOT EXISTS sales_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        year INTEGER,
        sales REAL
    )
''')

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
