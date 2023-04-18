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

# Execute the query and fetch the results into a DataFrame
@st.cache(allow_output_mutation=True)
def get_data_from_snowflake(query):
    cur = conn.cursor()
    cur.execute(query)
    results = cur.fetchall()
    df = pd.DataFrame(results, columns=[i[0] for i in cur.description])
    return df

df = get_data_from_snowflake(query)

# Display the results in Streamlit
st.write(df.head())


#selecting only required columns
df= df[['O_CUSTKEY','O_ORDERKEY','O_TOTALPRICE','O_ORDERDATE']]

# Drop Null values
df.dropna(subset=['O_CUSTKEY'], inplace=True)


# Create a dataframe with orders
df_orders = df.groupby(['O_CUSTKEY',
                        'O_ORDERKEY']).agg({'O_TOTALPRICE': sum,
                                           'O_ORDERDATE': max})

# Display the results in Streamlit
st.write(df_orders)

