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
query = 'select * from orders'

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




# Sort by Age in ascending order
df_orders = df.sort_values('O_CUSTKEY')

# Display the results in Streamlit
st.write(df_orders)

# total amount of purchases by each customer
def groupby_mean(x):
    return x.mean()

def groupby_count(x):
    return x.count()

def purchase_duration(x):
    return (x.max() - x.min()).days

def avg_frequency(x):
    return (x.max() - x.min()).days / x.count()

groupby_mean.__name__ = 'avg'
groupby_count.__name__ = 'count'
purchase_duration.__name__ = 'purchase_duration'
avg_frequency.__name__ = 'purchase_frequency'

df_summary = df_orders.reset_index().groupby('O_CUSTKEY').agg({
            'O_TOTALPRICE': [min, max, sum, groupby_mean, groupby_count],
            'O_ORDERDATE': [min, max, purchase_duration, avg_frequency]
             })
df_summary.columns = ['_'.join(col).lower() for col in df_summary.columns]
#df_summary = df_summary.loc[df_summary['invoicedate_purchase_duration'] > 0]

df_summary = df_summary.sort_values('o_orderdate_purchase_frequency', ascending=False)

# Display the results in Streamlit
st.write(df_summary)
