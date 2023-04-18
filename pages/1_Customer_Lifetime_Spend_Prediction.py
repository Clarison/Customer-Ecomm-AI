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

st.write(df.columns)

#selecting only required columns
df= df[['o_custkey','o_orderkey','o_totalprice','o_orderdate']]

# Drop Null values
df.dropna(subset=['o_custkey'], inplace=True)


# Create a dataframe with orders
df_orders = df.groupby(['o_custkey',
                        'o_orderkey']).agg({'o_totalprice': sum,
                                           'o_orderdate': max})

# Display the results in Streamlit
st.write(df_orders.head())

