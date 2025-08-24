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
    "schema": "TEST",  # optional
}
# Create a Snowpark session
session = snowpark.Session.builder.configs(connection_parameters).create()

def read_csv_to_snowflake(session, file_path, table_name):
    # Read CSV file into a Pandas DataFrame
    df = pd.read_csv(file_path)
    # Convert Pandas DataFrame to Snowpark DataFrame
    snowpark_df = session.create_dataframe(df)
    # Write Snowpark DataFrame to Snowflake table
    snowpark_df.write.mode("overwrite").save_as_table(table_name)


#/workspaces/Learning/datasources/iris.csv
if __name__ == "__main__":
    # Read CSV from datasources folder in the repository
    repo_root = os.path.dirname(os.path.abspath(__file__))
    data_folder = os.path.join(repo_root, "datasources")
    file_path = os.path.join(data_folder, "iris.csv")
    table_name = "test"
    print(f"Reading data from {file_path} and writing to Snowflake table {table_name}") 
    read_csv_to_snowflake(session, file_path, table_name)
    df = session.table(table_name).collect()
    print(df)
    session.close()