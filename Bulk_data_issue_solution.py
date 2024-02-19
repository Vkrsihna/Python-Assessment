#!/usr/bin/env python
# coding: utf-8

# In[ ]:


"""Q2: We have two products running on live
1. Smart Series-1
2. Smart Series-2
both these products run on any frameworks flask/Django/Fast API and upload a CSV file and insert it
into a new timestamped table.
given this API request.
curl --location --request POST 'http://localhost:5000/api/file-import' \
--form 'files=@"/Users/master_study_list.csv"' \
--form 'create_usr_id="ashish"
--form 'schema="public"'
Create a new table with the data in given schema as
public.master_study_list_2022_01_21_17_09_11
Where 2022_01_21 is the date and 17_09_11 is the time with seconds"""


# In[ ]:


from flask import Flask, request, jsonify
import pandas as pd
import psycopg2
from datetime import datetime
import pytest  # For optional testing example

app = Flask(_name_)

# Database connection details
db_params = {
    "database": "your_database",
    "user": "your_user",
    "password": "your_password",
    "host": "your_host",
}

# Function to create a new timestamped table using prepared statements
def create_table(conn, schema, table_name_prefix):
    try:
        cur = conn.cursor()

        # Use prepared statement for dynamic table name
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS %(schema)s.%(table_name)s (
                ProductID INT PRIMARY KEY,
                Name VARCHAR(100),
                Description TEXT,
                Category VARCHAR(50),
                Price DECIMAL(10, 2),
                StockQuantity INT,
                Weight DECIMAL(10, 2),
                CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
        """
        cur.execute(create_table_sql, {
            "schema": f"{schema}",
            "table_name": f"{table_name_prefix}{datetime.now().strftime('%Y%m_%d_%H_%M_%S')}"
        })

        conn.commit()
        cur.close()

        return table_name
    except Exception as e:
        return str(e)

@app.route('/api/file-import', methods=['POST'])
def import_data():
    try:
        # Access uploaded file and form data
        csv_file = request.files['files']
        create_usr_id = request.form['create_usr_id']
        schema = request.form['schema']  # Use received schema

        # Read CSV data
        df = pd.read_csv(csv_file)

        # Connect to database
        conn = psycopg2.connect(**db_params)

        # Create timestamped table
        table_name = create_table(conn, schema, "master_study_list")

        # Basic data validation (remove rows with missing ID or name)
        df.dropna(subset=['ProductID', 'Name'], inplace=True)

        # Consider more advanced data validation based on your schema

        # Insert data into the table
        df.to_sql(table_name, conn, if_exists='append', index=False)

        # Close connection
        conn.close()

        return jsonify({"message": "Data uploaded and processed successfully!"})

    except Exception as e:
        # Handle errors gracefully
        return jsonify({"error": str(e)})

if _name_ == '_main_':
    app.run(debug=True)

# Optional example test using pytest
@pytest.mark.parametrize("schema, expected_table_name", [
    ("public", "public.master_study_list_2024_02_19_17_16_28"),
    ("test", "test.master_study_list_2024_02_19_17_16_28"),
])
def test_create_table(schema, expected_table_name):
    # Mock database connection, call create_table with test params,
    # assert correct table creation and return value

