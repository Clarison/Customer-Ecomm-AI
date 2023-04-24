import streamlit as st
import snowflake.connector
import pandas as pd
import altair as alt

from mlxtend.frequent_patterns import apriori, association_rules

# Connect to Snowflake
conn = snowflake.connector.connect(
    user= st.secrets["user"],
    password= st.secrets["password"],
    account= st.secrets["account"],
    warehouse= st.secrets["warehouse"],
    database= st.secrets["database"],
    schema= st.secrets["schema"]
)

st.write('Recomended Products Pairs')

# Define a SQL query to fetch data from a table
query = 'select * from product_recom'

# Execute the query and fetch the results into a DataFrame
@st.cache_data()
def get_data_from_snowflake(query):
    cur = conn.cursor()
    cur.execute(query)
    results = cur.fetchall()
    df = pd.DataFrame(results, columns=[i[0] for i in cur.description])
    return df

df = get_data_from_snowflake(query)

st.write(df.head())


df_grouped = df.groupby('SS_TICKET_NUMBER')['SS_ITEM_SK'].agg(lambda x: ','.join(map(str, x))).reset_index()

# transform dataset into a one-hot encoded format
df_encoded = df_grouped['SS_ITEM_SK'].str.get_dummies(',')

# apply Apriori algorithm to identify frequent itemsets
frequent_itemsets = apriori(df_encoded, min_support=0.004, use_colnames=True)

# check if there are any frequent itemsets
if len(frequent_itemsets) > 0:
    # generate association rules from frequent itemsets
    rules = association_rules(frequent_itemsets, metric='lift', min_threshold=1)

    # print the resulting association rules
    st.write(rules)
else:
    st.write("No frequent itemsets found with the given minimum support value.")


