#%%
import snowflake.snowpark as snowpark
from snowpark import session

connection_parameters = {
  "account": "LTNLEZV-NG67871.snowflakecomputing.com",
  "user": "LATEHAR",
  "authenticator" : "externalbrowser",
  "role": "ACCOUNTADMIN",  # optional
  "warehouse": "COMPUTE_WH",  # optional
  "database": "LEARNING",  # optional
  "schema": "TEST",  # optional
}

session = Session.builder.configs(connection_parameters).create()

