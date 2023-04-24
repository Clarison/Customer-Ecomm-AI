import streamlit as st
import snowflake.connector
import pandas as pd
import altair as alt

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
query = 'select ss_item_sk,count(*) from store_sales_new group by ss_item_sk order by 2 desc limit 50 '

# Execute the query and fetch the results into a DataFrame
@st.cache_data()
def get_data_from_snowflake(query):
    cur = conn.cursor()
    cur.execute(query)
    results = cur.fetchall()
    df = pd.DataFrame(results, columns=[i[0] for i in cur.description])
    return df

df = get_data_from_snowflake(query)


    
st.write("Select the Product to view its targeted audience:")
# Display the results in Streamlit
st.write(df.head())

    

