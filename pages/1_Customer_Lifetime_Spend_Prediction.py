import streamlit as st
import snowflake.connector
import pandas as pd

# Connect to Snowflake
conn = snowflake.connector.connect(
    user= st.secrets["user"],
    password= st.secrets["password"],
    account= st.secrets["account"],
    warehouse= st.secrets["warehouse"],
    database= st.secrets["database"],
    schema= st.secrets["schema"]
)


# Define a SQL query to fetch data from a table
query = 'select * from orders limit 100'

# Execute the query and fetch the results
cur = conn.cursor()
cur.execute(query)
results = cur.fetchall()
df = pd.DataFrame(results, columns=[i[0] for i in cur.description])

# Display the results in Streamlit
st.write(df.head())
