import streamlit as st
import snowflake.connector
import pandas as pd
import altair as alt
from decimal import Decimal

# Connect to Snowflake
conn = snowflake.connector.connect(
    user= st.secrets["user"],
    password= st.secrets["password"],
    account= st.secrets["account"],
    warehouse= st.secrets["warehouse"],
    database= st.secrets["database"],
    schema= st.secrets["schema"]
)
 

query = 'select * from CUSTOMER_LIFE'

@st.cache_data()
def get_data_from_snowflake(query):
    cur = conn.cursor()
    cur.execute(query)
    results = cur.fetchall()
    df = pd.DataFrame(results, columns=[i[0] for i in cur.description])
    return df




# Set page title and description

st.title("Customer Lifetime Value Calculation")
st.write("In this section, we are using customer transaction data to calculate individual customer lifetime value (CLV). By estimating the total revenue a business can expect from a single customer over their lifetime, we can determine the value of each customer and make informed decisions about marketing, sales, and customer service strategies to increase profitability.")

# Add some padding and a separator

st.write("---")

df = get_data_from_snowflake(query)



discount_rate = st.sidebar.slider('Discount rate', 0, 100, 10, 5)
discount_rate=discount_rate/100

profit_margin = st.sidebar.slider('Profit margin', 0, 100, 10, 5)
profit_margin=profit_margin/100

churn_rate = st.sidebar.slider('Churn Rate', 0, 100, 10, 5)
churn_rate= churn_rate/100

years=st.sidebar.slider('Years', 0, 25, 10, 1)

# Define the constants
churn_rate = Decimal(churn_rate)
profit_margin = Decimal(profit_margin)
discount_rate = Decimal(discount_rate)
years = Decimal(years)

# Define a function to calculate the individual CLV
def calculate_clv(total_spend, num_purchases):
    asp = total_spend / num_purchases
    pf = num_purchases / 1  # assuming only one customer
    cl = 1 / churn_rate
    cv = asp * pf
    individual_clv = cv * cl * profit_margin * (1 - discount_rate) * years
    return round(individual_clv,2)

# Calculate the individual CLV for each customer
results = []
for index, row in df.iterrows():
    total_spend = Decimal(row['TOTAL'])
    num_purchases = Decimal(row['QUANTITY'])
    individual_clv = calculate_clv(total_spend, num_purchases)
    results.append({'customer_id': row['ID'], 'individual_clv': individual_clv})

# Create a DataFrame to store the results
results_df = pd.DataFrame(results)



# Join the dataframes on the "customer_id" column
joined_df = pd.merge(df, results_df,left_on="ID", right_on="customer_id")

# Print the joined dataframe
with st.beta_expander("Click to expand details"):
    st.write(joined_df.head())

# Assume df is your original dataframe with many columns
columns_to_keep = ['TOTAL', 'individual_clv']
df = joined_df[columns_to_keep]
df = df.astype(int)

sum_df = pd.DataFrame({'total_current_purchases': [df['TOTAL'].sum()],
                       'total_predicted_spend': [df['individual_clv'].sum()]})
# melt the data
melted_df = pd.melt(sum_df, var_name='category', value_name='value')





# Create the stacked bar chart
chart = alt.Chart(melted_df).mark_bar().encode(
    x='category:N',
    y='value:Q',
    color='category:N'
)

# Add axis labels and a title
chart = chart.properties(
    title='Total vs. Predicted Sum',
    width=800,
    height=600
).configure_axis(
    labelFontSize=14,
    titleFontSize=16
)

# Show the chart in Streamlit
st.altair_chart(chart)



