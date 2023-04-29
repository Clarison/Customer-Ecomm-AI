import streamlit as st
import plotly.graph_objs as go
import pandas as pd
import numpy as np
import datetime as dt
import snowflake.connector as sf
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import matplotlib.pyplot as plt
import humanize
import altair as alt
import pandas as pd
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import multiprocessing
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score
import matplotlib.pyplot as plt

st.set_page_config(page_title="Customer Churn Forecast", page_icon=":bar_chart:", layout="wide")

st.title("Customer Churn Forecast")
#########################################################################################################
#trying chemy
engine = create_engine(URL(
    account = 'dl84836.us-east-2.aws',
    user = 'alekyakastury',
    password = '@Noon1240',
    database = 'CUSTOMER',
    schema = 'PUBLIC',
    warehouse = 'compute_wh'
))

#####################################################################################################

# Define a function to be executed in parallel
@st.cache_data
def execute_query(query):
    engine = create_engine(URL(
    account = 'dl84836.us-east-2.aws',
    user = 'alekyakastury',
    password = '@Noon1240',
    database = 'CUSTOMER',
    schema = 'PUBLIC',
    warehouse = 'compute_wh'))
    df = pd.read_sql_query(query, engine)
    return df


######################################################################################################
# Define your SQL queries
query =  """SELECT * FROM CUSTOMER_DEMO_VIEW;"""
df_customer_demo=execute_query(query)

query="""SELECT * FROM CUSTOMER_INCOME;"""
df_customer_income=execute_query(query)

query= """SELECT * FROM INCOME_VIEW;"""
df_income_view=execute_query(query)
#########################################################################################
#### Data Preparation
df_customer_demo=df_customer_demo.dropna()
#########################################################################################
###### Label encoding

# Import label encoder 
from sklearn import preprocessing 
  
# label_encoder object knows how to understand word labels. 
label_encoder = preprocessing.LabelEncoder()

df_customer_demo['cd_gender']= label_encoder.fit_transform(df_customer_demo['cd_gender'])
df_customer_demo['cd_education_status']= label_encoder.fit_transform(df_customer_demo['cd_education_status'])
df_customer_demo['cd_credit_rating']= label_encoder.fit_transform(df_customer_demo['cd_credit_rating'])
df_customer_demo['cd_marital_status']= label_encoder.fit_transform(df_customer_demo['cd_marital_status'])

###############################################################################3

X = df_customer_demo.drop(columns=['c_customer_sk','c_first_name','c_last_name','customer_status_i'], axis = 1)
y = df_customer_demo['customer_status_i']

from imblearn.over_sampling import SMOTE
smote = SMOTE()
X_resampled, y_resampled = smote.fit_resample(X,y)

from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier

X_train, X_test, y_train, y_test = train_test_split(X_resampled, y_resampled, test_size = 0.2, random_state = 42)

random = RandomForestClassifier(n_estimators = 200, max_depth=200, random_state = 0) 
random.fit(X_train , y_train) 
print('Random_forest_score :',random.score(X_test, y_test))

y_pred=random.predict(X_test)

X_test['customer_status_i']=y_pred
customer_demo_df=X_test
# combine 3 columns into 1 column

# replace 'Male' with 1 and 'Female' with 0 in the 'Gender' column
customer_demo_df['cd_gender'] = customer_demo_df['cd_gender'].replace({1:'Male',0:'Female'})
age_bins = [0, 30, 50,100]
age_labels = ['0','30', '50']
customer_demo_df['age_agg'] = pd.cut(customer_demo_df['age'], bins=age_bins, labels=age_labels, include_lowest=True)


customer_demo_df['Segment'] = customer_demo_df['age_agg'].astype(str) + '_' + customer_demo_df['cd_gender'].astype(str)

# create labels for the bins
segment_labels = {'0_Male':'Boy','0_Female':'Girl','30_Male':'Young Adult Male', '30_Female':'Young Adult Female', '50_Male':'Old Male', '50_Female':'Old Female'}

customer_demo_df['Segmented']=customer_demo_df['Segment'].map(segment_labels)

# segment customers based on the combined column
#customer_demo_df['Segmented'] = pd.cut(customer_demo_df['Segment'], bins=segment_bins, labels=segment_labels)

risky_customers=X_test[X_test['customer_status_i']==1].shape[0]
retention_rate=round(X_test[X_test['customer_status_i']==2].shape[0]*100/X_test['customer_status_i'].shape[0],2)
###############################################################################
query=""" SELECT CUSTOMER_STATUS,COUNT(C_CUSTOMER_SK) AS COUNT_OF_CUSTOMERS FROM ACTIVE_CUSTOMERS GROUP BY CUSTOMER_STATUS LIMIT 10000;"""
df_status=execute_query(query)

################################# CUSTOMER INCOME #################################################3

query="""SELECT * FROM CUSTOMER_INCOME;"""
df_customer_income=execute_query(query)

X = df_customer_income.drop(columns=['c_customer_sk','customer_status_i'], axis = 1)
y = df_customer_income['customer_status_i']

from imblearn.over_sampling import SMOTE
smote = SMOTE()
X_resampled, y_resampled = smote.fit_resample(X,y)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 42)

random = RandomForestClassifier(n_estimators = 200, max_depth=100, random_state = 0) 
random.fit(X_train , y_train) 

y_pred=random.predict(X_test)

X_test['customer_status_i']=y_pred
cust_income_df=X_test
# filter the DataFrame based on a condition
filtered_df = X_test.loc[X_test['customer_status_i'] == 0]

# calculate the mean of column 'B' in the filtered DataFrame
mean_b = filtered_df['income'].mean()


############################################## Dashboard #############################################3
# Create a container for the metrics
with st.beta_container():
    # Create two columns for the metrics
    col1, col2, col3 = st.beta_columns(3)
    with col1:
        st.metric(label="Risky Customers", value=risky_customers)
    with col2:
        st.metric('Income of Risky Customers', mean_b)
    with col3:
        st.metric('Retention Rate', str(retention_rate)+'%')

c1 = alt.Chart(df_status,title='Customers by status').mark_bar().encode(x='customer_status', y='count_of_customers')
c1 = c1.properties(width=800, height=400)
st.altair_chart(c1)

# create a bar chart of the value counts of income in X_test
fig, ax = plt.subplots()
cust_income_df.income.value_counts().plot(kind='bar', ax=ax)

# add labels and title
ax.set_xlabel('Income')
ax.set_ylabel('Count')
ax.set_title('Value Counts of Income')

# display the chart on Streamlit
st.pyplot(fig)

# group the DataFrame by the 'Segmented' and 'Status' columns and count the number of customers in each group
segment_status_counts = customer_demo_df.groupby(['Segmented', 'customer_status_i']).size().reset_index(name='Count')

# pivot the DataFrame to create a stacked bar chart
segment_status_pivot = segment_status_counts.pivot(index='customer_status_i', columns='Segmented', values='Count')

# plot the stacked bar chart
fig, ax = plt.subplots()
segment_status_pivot.plot(kind='bar', stacked=True, ax=ax)

# set the plot title and axis labels
plt.title('Customer Status by Segment')
plt.xlabel('Segment')
plt.ylabel('Number of Customers')

# display the plot on Streamlit
st.pyplot(fig)






