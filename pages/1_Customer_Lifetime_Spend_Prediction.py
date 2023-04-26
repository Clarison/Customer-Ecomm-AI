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
 
    
with st.beta_expander("Old Work"):    
    # Define a SQL query to fetch data from a table
    query = 'select * from CLV where ss_sales_price > 20 limit 10000'

    # Execute the query and fetch the results into a DataFrame
    @st.cache_data()
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
    #df= df[['O_CUSTKEY','O_ORDERKEY','O_TOTALPRICE','O_ORDERDATE']]
    @st.cache_data()
    def get_data_cleaned(query):
        # Drop Null values
        df.dropna(subset=['SS_CUSTOMER_SK'], inplace=True)




        # Sort by Age in ascending order
        df_orders = df.sort_values('SS_CUSTOMER_SK')

        # Display the results in Streamlit
        #st.write(df_orders)

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

        df_summary = df_orders.reset_index().groupby('SS_CUSTOMER_SK').agg({
                    'SS_SALES_PRICE': [min, max, sum, groupby_mean, groupby_count],
                    'D_DATE': [min, max, purchase_duration, avg_frequency]
                     })
        df_summary.columns = ['_'.join(col).lower() for col in df_summary.columns]
        #df_summary = df_summary.loc[df_summary['invoicedate_purchase_duration'] > 0]

        df_summary = df_summary.sort_values('ss_sales_price_count', ascending=False)
        return df_summary

    df_summary=get_data_cleaned(df)
    # Display the results in Streamlit
    st.write(df_summary.head())



    # Group data by a column ('sales_count' in this example)
    grouped_data = df_summary.groupby('ss_sales_price_count').size().reset_index(name='count')

    #filtering only for greater than 1
    grouped_data = grouped_data[grouped_data['ss_sales_price_count'] > 1]

    # Create an Altair bar chart
    chart = alt.Chart(grouped_data).mark_bar().encode(
        x=alt.X('ss_sales_price_count:O', title='Sales Count'),
        y=alt.Y('count:Q', title='Total Instances')
    ).properties(
        width=600,
        height=400,
        title='Sales Count vs. Total Instances'
    )

    # Display the chart in Streamlit
    st.altair_chart(chart)
    df_summary_hist = df_summary[df_summary['d_date_purchase_duration']>1]

    # Create a histogram of duration in seconds
    chart = alt.Chart(df_summary_hist).mark_bar().encode(
        x=alt.X('d_date_purchase_duration:Q', bin=alt.Bin(step=10), title='Duration (days)'),
        y=alt.Y('count()', title='Number of Occurrences')
    ).properties(
        width=600,
        height=400,
        title='Duration Histogram'
    )



    # rename columns
    df = df.rename(columns={'SS_CUSTOMER_SK': 'CustomerID', 'D_DATE': 'InvoiceDate','SS_SALES_PRICE': 'Sales'})
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    # Display the chart in Streamlit
    st.altair_chart(chart)

    @st.cache_data()
    def get_data_predict(query):

        def groupby_mean(x):
            return x.mean()

        def groupby_count(x):
            return x.count()

        clv_freq = '3M'

        df_data = df.reset_index().groupby([
                    'CustomerID',
                    pd.Grouper(key='InvoiceDate', freq=clv_freq)
                    ]).agg({'Sales': [sum, groupby_mean, groupby_count],})
        df_data.columns = ['_'.join(col).lower() for col in df_data.columns]
        df_data = df_data.reset_index()
        df_data=df_data.rename(columns={'sales_groupby_mean':'sales_avg','sales_groupby_count':'sales_count'})
        map_date_month = {str(x)[:10]: 'M_%s' % (i+1) for i, x in enumerate(
                            sorted(df_data.reset_index()['InvoiceDate'].unique(), reverse=True))}
        df_data['M'] = df_data['InvoiceDate'].apply(lambda x: map_date_month[str(x)[:10]])
        df_features = pd.pivot_table(
                        df_data.loc[df_data['M'] != 'M_1'], 
                        values=['sales_sum', 'sales_avg', 'sales_count'], 
                        columns='M', 
                        index='CustomerID')
        df_features.reset_index()
        df_features.columns = ['_'.join(col) for col in df_features.columns]
        df_features.reset_index(level=0, inplace=True)

        # Let us fill in the Null values with 0
        df_features.fillna(0, inplace=True)

        # Select the target
        df_target = df_data.loc[df_data['M'] == 'M_1', ['CustomerID', 'sales_sum']]
        df_target.columns = ['CustomerID', 'CLV_'+clv_freq]

        df_sample_set = df_features.merge(
                        df_target, 
                        left_on='CustomerID', 
                        right_on='CustomerID',
                        how='left')
        df_sample_set.fillna(0, inplace=True)
        df_sample_set = df_sample_set.sort_values('CLV_3M', ascending=False)

        return df_sample_set

    df_sample_set=get_data_predict(df)

    st.write(df_sample_set.head(100))

st.write('New Beginings')

query = 'select * from CUSTOMER_LIFE'

df = get_data_from_snowflake(query)

    # Display the results in Streamlit
st.write(df.head())


discount_ratee = st.sidebar.slider('Discount rate', 0, 100, 10, 5)
dis=discount_ratee/100

profit_margin = st.sidebar.slider('Profit margin', 0, 100, 10, 5)
profit_margin=profit_margin/100
# Define the constants
churn_rate = Decimal('0.2')
profit_margin = Decimal(profit_margin)
discount_rate = Decimal(dis)
years = Decimal('5')

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

# Print the DataFrame
st.write(results_df)
