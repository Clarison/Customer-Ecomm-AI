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
from sklearn import preprocessing 
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier

st.set_page_config(page_title="Customer Churn Forecast", page_icon=":bar_chart:", layout="wide")

st.title("Customer Churn Forecast")
#########################################################################################################
#trying chemy
engine = create_engine(URL(
    account = st.secrets["account"],
    user = st.secrets["user"],
    password = st.secrets["password"],
    database = st.secrets["database"],
    schema = st.secrets["schema"],
    warehouse = st.secrets["warehouse"]
))

#####################################################################################################

# Define a function to be executed in parallel
@st.cache_data
def execute_query(query):
    df = pd.read_sql_query(query, engine)
    return df


######################################################################################################
# Define your SQL queries
query1 =  """SELECT * FROM CUSTOMER_DEMO_VIEW;"""
@st.cache_data
def exec_cust_demo(query):
    df_customer_demo=pd.read_sql_query(query, engine)
    return df_customer_demo
df_customer_demo=exec_cust_demo(query1)

query2="""SELECT * FROM CUSTOMER_INCOME;"""
@st.cache_data
def exec_cust_income(query):
    df_customer_income=pd.read_sql_query(query, engine)
    return df_customer_income
df_customer_income=exec_cust_income(query2)

query3= """SELECT * FROM INCOME_VIEW;"""
@st.cache_data
def exec_cust_income_view(query):
    df_income_view=pd.read_sql_query(query, engine)
    return df_income_view
df_income_view=exec_cust_income_view(query3)
#########################################################################################
#### Data Preparation
df_customer_demo=df_customer_demo.dropna()
#########################################################################################
###### Label encoding


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

@st.cache_data
def run_model():
    XX_resampled, y_resampled = smote.fit_resample(X,y)
    XX_train, XX_test, y_train, y_test = train_test_split(XX_resampled, y_resampled, test_size = 0.2, random_state = 42)
    random = RandomForestClassifier(n_estimators = 200, max_depth=200, random_state = 0) 
    random.fit(XX_train , y_train) 
    y_pred=random.predict(XX_test)
    XX_test['customer_status_i']=y_pred
    return XX_test

customer_demo_df=run_model()

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

risky_customers=customer_demo_df[customer_demo_df['customer_status_i']==1].shape[0]
retention_rate=round(customer_demo_df[customer_demo_df['customer_status_i']==2].shape[0]*100/customer_demo_df['customer_status_i'].shape[0],2)
###############################################################################
query4=""" SELECT CUSTOMER_STATUS,COUNT(C_CUSTOMER_SK) AS COUNT_OF_CUSTOMERS FROM ACTIVE_CUSTOMERS GROUP BY CUSTOMER_STATUS;"""

@st.cache_data
def exec_status(query):
    df_status=pd.read_sql_query(query, engine)
    return df_status
df_status=exec_status(query4)
################################# CUSTOMER INCOME #################################################3

query5="""SELECT * FROM CUSTOMER_INCOME;"""
@st.cache_data
def exec_cust_inc(query):
    df_customer_income=pd.read_sql_query(query, engine)
    return df_customer_income

df_customer_income=exec_cust_inc(query5)

@st.cache_data
def run_model1():
    X = df_customer_income.drop(columns=['c_customer_sk','customer_status_i'], axis = 1)
    y = df_customer_income['customer_status_i']

    smote = SMOTE()
    X_resampled, y_resampled = smote.fit_resample(X,y)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 42)

    random = RandomForestClassifier(n_estimators = 200, max_depth=100, random_state = 0) 
    random.fit(X_train , y_train) 

    y_pred=random.predict(X_test)

    X_test['customer_status_i']=y_pred
    return X_test

X_test=run_model1()
# filter the DataFrame based on a condition
filtered_df = X_test.loc[X_test['customer_status_i'] == 0]

# calculate the mean of column 'B' in the filtered DataFrame
mean_b = filtered_df['income'].mean()
#############################################################
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
############################################ PRODUCT ANALYSIS #######################################

