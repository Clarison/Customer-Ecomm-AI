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

column_data = df["SS_ITEM_SK"]
    
st.write("Select the Product to view its targeted audience:")

selected_value = st.selectbox("Select a value", column_data)
# Display the results in Streamlit
st.write(selected_value)



query = f"SELECT * FROM Item WHERE i_item_sk = '{selected_value}'"

# Execute the query
df = get_data_from_snowflake(query)
# Display the result
st.write(df)

# Display the row as key-value pairs using Streamlit
st.write("### Selected row:")
for col_name, col_val in df.iteritems():
    st.write(f"- {col_name}: {col_val}")
    
query = f"select * from  customer_pattern where ss_item_sk= '{selected_value}' limit 1000"

# Execute the query
df = get_data_from_snowflake(query)
# Display the result
st.write(df.head())


# find the most common value in the 'col1' column
most_common_education = df['CD_EDUCATION_STATUS'].mode()[0]
most_common_gender = df['CD_GENDER'].mode()[0]
most_common_marital = df['CD_MARITAL_STATUS'].mode()[0]
most_common_credit = df['CD_CREDIT_RATING'].mode()[0]


st.write(most_common_education)
st.write(most_common_gender)
st.write(most_common_marital)
st.write(most_common_credit)

