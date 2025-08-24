from io import StringIO
import requests
import snowflake.snowpark as snowpark
import pandas as pd
from snowflake.snowpark import Session
import os

# Define connection parameters
connection_parameters = {
    "account": "LTNLEZV-NG67871",
    "user": "LATEHAR",
    "password": "Latehar@12345678",
    "role": "ACCOUNTADMIN",  # optional
    "warehouse": "COMPUTE_WH",  # optional
    "database": "LEARNING",  # optional
    "schema": "TEST"  # optional
    
}


def extract_from_github(url):
    response = requests.get(url)
    response.raise_for_status()  # Ensure we got a successful response
    csv_data = StringIO(response.text)
    df = pd.read_csv(csv_data)
    return df

def extract(session,source):
    df = pd.read_sql(source)
    return df

def extract_from_snowflake(session, table_name):
    df = session.table(table_name).to_pandas()
    return df

def transform(df):
    # Perform any necessary transformations on the DataFrame
    # For example, let's just return the DataFrame as is
    return df

def load(session,df,target):  
    session.write_pandas(
            df,
            table_name=target,
            auto_create_table=True, # Automatically creates the table
            overwrite=True          # Overwrites the table if it exists
        )
    print(f"Data successfully loaded into table: {target}")

def load_to_snowflake(session, df, target):
    df = session.create_dataframe(df)
    df.write.mode("overwrite").save_as_table(target
    )
    print(f"Data successfully loaded into table: {target}")

#https://raw.githubusercontent.com/hfinfo92/Learning/refs/heads/main/datasources/Employee.csv
# github_raw_url = "https://raw.githubusercontent.com/datasets/human-resources/main/data/HRDataset_v14.csv"
if __name__ == "__main__":
    
    # Create a Snowflake session
    session = Session.builder.configs(connection_parameters).create()


    # Read CSV from datasources folder in the repository
    github_raw_url = "https://raw.githubusercontent.com/hfinfo92/Learning/refs/heads/main/datasources/HRDataset_v14.csv"
    
    # Extract data
    source = 'learning.test."hr_dataset"'
    target = 'ods.learning.hr_dataset'

    # extract data
    df = extract_from_snowflake(session,source)
    print(df.head())

    # Transform data
    transformed_df = transform(df)

    # Load data into Snowflake
    res=load_to_snowflake(session, transformed_df,target)
    print(res)

    # Close the session
    session.close()