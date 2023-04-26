import streamlit as st
import snowflake.connector
import pandas as pd
import altair as alt
import plotly.express as px 

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

selected_value = st.selectbox("Select a Product ID", column_data)
# Display the results in Streamlit
#st.write(selected_value)



query = f"SELECT I_ITEM_ID,I_PRODUCT_NAME,I_CLASS,I_CATEGORY,I_ITEM_DESC FROM Item WHERE i_item_sk = '{selected_value}'"

# Execute the query
df = get_data_from_snowflake(query)
# Display the result
st.write(df)


    
query = f"select * from  customer_pattern where ss_item_sk= '{selected_value}' limit 10000"

# Execute the query
df = get_data_from_snowflake(query)

# Replace values in the 'gender' column
df['CD_GENDER'] = df['CD_GENDER'].replace({'F': 'Female', 'M': 'Male'})

df['CD_MARITAL_STATUS'] = df['CD_MARITAL_STATUS'].replace({
    'S': 'Single',
    'M': 'Married',
    'D': 'Divorced',
    'W': 'Widowed',
    'U': 'Unknown'
})
# Display the result
#st.write(df.head())


# find the most common value in the 'col1' column
most_common_education = df['CD_EDUCATION_STATUS'].mode()[0]
most_common_gender = df['CD_GENDER'].mode()[0]
most_common_marital = df['CD_MARITAL_STATUS'].mode()[0]
most_common_credit = df['CD_CREDIT_RATING'].mode()[0]
avg_lower_bound= round(df['IB_LOWER_BOUND'].mean())
avg_upper_bound=round(df['IB_UPPER_BOUND'].mean())
avg_purchase_estimate=round(df['CD_PURCHASE_ESTIMATE'].mean())




st.markdown(f"<h3>Most of our customers for this Product are <b>{most_common_marital} {most_common_gender}</b> having <b>{most_common_education}</b> education.<br><br>Their average Purchasing Power is <b>{avg_purchase_estimate}</b> and Income somewhere between <b>{avg_lower_bound}</b> and <b>{avg_upper_bound}</b> having <b>{most_common_credit}</b> credit score.</h3>", unsafe_allow_html=True)


st.title(":bar_chart: Target Customer Dashboard")
st.markdown("##")

# TOP KPI's
total_customers = int(df["SS_ITEM_SK"].count())
average_age = round(df["AGE"].mean(), 1)
average_purchase_estimate = round(df["CD_PURCHASE_ESTIMATE"].mean(), 2)


left_column, middle_column, right_column = st.columns(3)
with left_column:
    st.subheader("Total Customers:")
    st.subheader(f"{total_customers:,}")
with middle_column:
    st.subheader("Average Age:")
    st.subheader(f"{average_age}")
with right_column:
    st.subheader("Average Purchases Estimate:")
    st.subheader(f"{average_purchase_estimate}")

st.markdown("""---""")



#histogram by age

fig_age = px.histogram(df,
	x="AGE",
	title="<b>Distribution by age</b>",
	color_discrete_sequence=["#0083B8"] ,
	template="plotly_white"
)



#histogram by Purchase

fig_tenure = px.histogram(df,
	x="CD_PURCHASE_ESTIMATE",
	title="<b>Distribution by Estmate Spendings</b>",
	color_discrete_sequence=["#0083B8"] ,
	template="plotly_white"
)



#histogram by Income

fig_purchases = px.histogram(df,
	x="IB_UPPER_BOUND",
	title="<b>Distribution by Income/b>",
	color_discrete_sequence=["#0083B8"] ,
	template="plotly_white"
)



left_column, mid_column, right_column = st.columns(3)
left_column.plotly_chart(fig_age, use_container_width=True)
mid_column.plotly_chart(fig_tenure, use_container_width=True)
right_column.plotly_chart(fig_purchases, use_container_width=True)

