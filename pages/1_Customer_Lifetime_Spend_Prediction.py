import streamlit as st
import snowflake.connector

# Connect to Snowflake
conn = snowflake.connector.connect(
    user='alekyakastury',
    password='@Noon1240',
    account='dl84836.us-east-2.aws',
    warehouse='COMPUTE_WH',
    database='SNOWFLAKE_SAMPLE_DATA',
    schema='TPCH_SF10'
)

# Define a SQL query to fetch data from a table
query = 'SELECT * FROM customer limit 10'

# Execute the query and fetch the results
cur = conn.cursor()
cur.execute(query)
results = cur.fetchall()

# Display the results in Streamlit
st.write(results)
