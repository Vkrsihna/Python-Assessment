#!/usr/bin/env python
# coding: utf-8

# In[ ]:


"""Q1: One of our products is in charge of downloading and ingesting millions of records from our
clients.
Recently during ingesting a large dataset we had our entire DB(Postgres) go down and the entire
ingestion process from pandas data frame to SQL took around 2-3 hours because of the RAM
unavailability.
Now this has two simple fixes.
- Increase ram/ scale the DB on demand
- change our code to accommodate these restrictions and make the entire ingestion process
much faster on the way.
How would you approach this? We are not looking for a full-blown ingestion logic. Just a small script
to take a given CSV file and upload it to DB in an efficient manner.
Write code to take a large csv file (> 1GB ) and ingest it to table - public.test_od"""


# In[ ]:


import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Function to establish connection with PostgreSQL database
def create_db_connection():
    try:
        conn = psycopg2.connect(
            host="your_host",
            database="your_database",
            user="your_username",
            password="your_password"
        )
        return conn
    except Exception as e:
        print("Error connecting to PostgreSQL:", e)

# Function to ingest CSV data into PostgreSQL table
def ingest_csv_to_postgres(csv_file_path, table_name):
    try:
        # Create a database connection
        conn = create_db_connection()
        
        # Create a SQLAlchemy engine
        engine = create_engine('postgresql://your_username:your_password@your_host:5432/your_database')
        
        # Load CSV data into Pandas DataFrame (chunksize for memory efficiency)
        chunksize = 100000  # Adjust as needed based on available memory
        for chunk in pd.read_csv(csv_file_path, chunksize=chunksize):
            chunk.to_sql(table_name, engine, if_exists='append', index=False)
            print("Chunk ingested successfully")

        print("CSV data ingested into PostgreSQL table successfully")
    except Exception as e:
        print("Error ingesting CSV data into PostgreSQL:", e)
    finally:
        # Close database connection
        if conn:
            conn.close()

# Path to the CSV file
csv_file_path = "path/to/your/large.csv"

# Table name in PostgreSQL database
table_name = "public.test_od"

# Ingest CSV data into PostgreSQL table
ingest_csv_to_postgres(csv_file_path, table_name)

