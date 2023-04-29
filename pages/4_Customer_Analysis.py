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


st.set_page_config(page_title="Customer Analysis Dashboard", page_icon=":bar_chart:", layout="wide")

st.title("Customer Analysis Dashboard")
#########################################################################################################
#trying chemy
engine = create_engine(URL(
    account = 'dl84836.us-east-2.aws',
    user =  st.secrets["user"],
    password = '@Noon1240',
    database = 'CUSTOMER',
    schema = 'PUBLIC',
    warehouse = 'compute_wh'
))

############################################################################################

# create a dropdown for the year parameter with the distinct state values
year = st.sidebar.selectbox('Year', [1998,1999,2000,2001,2002])

# create a dropdown for the year parameter with the distinct state values
month = st.sidebar.selectbox('Month', [1,2,3,4,5,6,7,8,9,10,11,12])

def shorten_num(number):    
    if number >= 1000000000:
        shortened_num = str(round(number/1000000000, 1)) + "B"
    elif number >= 1000000:
        shortened_num = str(round(number/1000000, 1)) + "M"
    elif number >= 1000:
        shortened_num = str(round(number/1000, 1)) + "K"
    else:
        shortened_num = str(number)
    return shortened_num

#####################################################BLOCK 1##############################################
@st.cache_data
def run_query(query,col):
    df=pd.read_sql_query(query,engine)
    res=df[col][0]
    return res

query="""SELECT SUM(SS_NET_PAID) as sales FROM STORE_SALES SS INNER JOIN DATE_DIM DD ON
SS.SS_SOLD_DATE_SK=DD.D_DATE_SK WHERE 
DD.D_YEAR={} and DD.D_MOY={} group by 
DD.D_YEAR, DD.D_MOY""".format(year,month)

revenue_current=run_query(query,'sales')

if year==1998 and month==1:
    percentage=100
elif month==1:
    prev_year=year-1
    prev_month=12
    
    query="""SELECT SUM(SS_NET_PAID) as sales FROM 
    STORE_SALES SS INNER JOIN DATE_DIM DD
    ON SS.SS_SOLD_DATE_SK=DD.D_DATE_SK
    WHERE DD.D_YEAR={} and DD.D_MOY={}
    group by DD.D_YEAR, DD.D_MOY""".format(prev_year,prev_month)
    
else:
    prev_month=month-1
    
    query="""SELECT SUM(SS_NET_PAID) as sales FROM 
    STORE_SALES SS INNER JOIN DATE_DIM DD
    ON SS.SS_SOLD_DATE_SK=DD.D_DATE_SK
    WHERE DD.D_YEAR={} and DD.D_MOY={}
    group by DD.D_YEAR, DD.D_MOY""".format(year,prev_month)

revenue_prev=run_query(query,'sales')

percentage=((revenue_current-revenue_prev)/revenue_prev)*100


###########################################################################################################


#################################BLOCK 2##################################################
query="""SELECT count(distinct SS_CUSTOMER_SK) as no_of_customers
FROM STORE_SALES SS INNER JOIN DATE_DIM DD ON
SS.SS_SOLD_DATE_SK=DD.D_DATE_SK 
WHERE DD.D_YEAR={} AND DD.D_MOY={}
group by DD.D_YEAR, DD.D_MOY;""".format(year,month)

no_of_customers=run_query(query,'no_of_customers')

if year==1998 and month==1:
    percentage_cust=100
elif month==1:
    prev_year=year-1
    prev_month=12
    query="""SELECT count(distinct SS_CUSTOMER_SK) as no_of_customers
    FROM STORE_SALES SS INNER JOIN DATE_DIM DD ON
    SS.SS_SOLD_DATE_SK=DD.D_DATE_SK 
    WHERE DD.D_YEAR={} AND DD.D_MOY={}
    group by DD.D_YEAR, DD.D_MOY;""".format(prev_year,prev_month)
    
else:
    prev_month=month-1
    query="""SELECT count(distinct SS_CUSTOMER_SK) as no_of_customers
    FROM STORE_SALES SS INNER JOIN DATE_DIM DD ON
    SS.SS_SOLD_DATE_SK=DD.D_DATE_SK 
    WHERE DD.D_YEAR={} AND DD.D_MOY={}
    group by DD.D_YEAR, DD.D_MOY;""".format(year,prev_month)

no_of_customers_prev=run_query(query,'no_of_customers')

percentage_cust=((no_of_customers-no_of_customers_prev)/no_of_customers_prev)*100

###########################################################################################
##############################BLOCK 3####################################################
query="""SELECT count(SS_CUSTOMER_SK) AS ret
FROM (
  SELECT SS_CUSTOMER_SK, COUNT(*) AS count
  FROM STORE_SALES SS INNER JOIN DATE_DIM DD
 ON SS.SS_SOLD_DATE_SK=DD.D_DATE_SK
   WHERE DD.D_YEAR={} and DD.D_MOY={}
  GROUP BY SS_CUSTOMER_SK
) S WHERE count>1 ;""".format(year,month)

ret_customers=run_query(query,'ret')         

query="""SELECT count(SS_CUSTOMER_SK) as TOTAL
  FROM STORE_SALES SS INNER JOIN DATE_DIM DD
 ON SS.SS_SOLD_DATE_SK=DD.D_DATE_SK
   WHERE DD.D_YEAR={} and DD.D_MOY={};""".format(year,month)

total_customers=run_query(query,'total')

percentage_ret_customers=ret_customers*100/total_customers
#########################################################################################
#############################BLOCK 4#####################################################

query="""SELECT count(SS_NET_PAID) as count_sales FROM STORE_SALES SS INNER JOIN DATE_DIM DD ON
SS.SS_SOLD_DATE_SK=DD.D_DATE_SK WHERE 
DD.D_YEAR={} and DD.D_MOY={} group by 
DD.D_YEAR, DD.D_MOY;""".format(year,month)

count_sales=run_query(query,'count_sales')
average=revenue_current /count_sales

if year==1998 and month==1:
    percentage_avg_inc=100
elif month==1:
    prev_year=year-1
    prev_month=12
    
    query="""SELECT count(SS_NET_PAID) as prev_count_sales FROM 
    STORE_SALES SS INNER JOIN DATE_DIM DD
    ON SS.SS_SOLD_DATE_SK=DD.D_DATE_SK
    WHERE DD.D_YEAR={} and DD.D_MOY={}
    group by DD.D_YEAR, DD.D_MOY""".format(prev_year,prev_month)
    prev_count=run_query(query,'prev_count_sales')
    prev_avg=revenue_prev/prev_count
    percentage_avg_inc=(average-prev_avg)*100/prev_avg    
else:
    prev_month=month-1
    
    query="""SELECT count(SS_NET_PAID) as prev_count_sales FROM 
    STORE_SALES SS INNER JOIN DATE_DIM DD
    ON SS.SS_SOLD_DATE_SK=DD.D_DATE_SK
    WHERE DD.D_YEAR={} and DD.D_MOY={}
    group by DD.D_YEAR, DD.D_MOY""".format(year,prev_month)

    prev_count=run_query(query,'prev_count_sales')
    prev_avg=revenue_prev/prev_count
    percentage_avg_inc=(average-prev_avg)*100/prev_avg

#########################################################################################
# Create a container for the metrics
with st.beta_container():
    # Create two columns for the metrics
    col1, col2, col3,col4 = st.beta_columns(4)
    with col1:
        st.metric(label="Revenue", value=shorten_num(revenue_current),delta=str(round(percentage,1))+'%')
    with col2:
        st.metric('Number of Customers', shorten_num(no_of_customers),delta=str(round(percentage_cust,1))+'%')
    with col3:
        st.metric('Returning customers', str(round(percentage_ret_customers,1))+'%') 
    with col4:
        st.metric('Average Order Value',round(average,0),delta=str(round(percentage_avg_inc,1))+'%')

#######################################################################################################################
query1="""SELECT CAST(dd.d_year AS INTEGER) as YEAR,COUNT(SS_CUSTOMER_SK) AS COUNT_OF_CUSTOMERS
FROM STORE_SALES SS INNER JOIN DATE_DIM DD
ON SS.SS_SOLD_DATE_SK=DD.D_DATE_SK
WHERE DD.D_MOY={} 
group by DD.D_YEAR;""".format(month)

#@st.cache_data
#def run_query_plot_1(query):
df=pd.read_sql_query(query1,engine)
c1 = alt.Chart(df,title='Yearly customer count of a month').mark_bar().encode(x=alt.X('year',scale=alt.Scale(type='linear',domain=[1998,2003])), y='count_of_customers')
c1 = c1.properties(width=800, height=400)
 
 
 
#########################################################################################################################
query2="""SELECT dd.d_moy as MONTH,COUNT(SS_CUSTOMER_SK) AS COUNT_OF_CUSTOMERS
FROM STORE_SALES SS INNER JOIN DATE_DIM DD
ON SS.SS_SOLD_DATE_SK=DD.D_DATE_SK
WHERE DD.D_YEAR={}
group by DD.D_MOY;""".format(year)

#@st.cache_data
#def run_query_plot_2(query):
df=pd.read_sql_query(query2,engine)
c2 = alt.Chart(df,title='Monthly customer count per year').mark_line().encode(x='month', y='count_of_customers')
c2 = c2.properties(width=800, height=400)
##################################################################################
query="""SELECT AGE,COUNT(1) AS COUNT FROM (SELECT D_YEAR-C_BIRTH_YEAR AS AGE FROM CUSTOMER C INNER JOIN STORE_SALES SS
ON C.C_CUSTOMER_SK=SS.SS_CUSTOMER_SK INNER JOIN DATE_DIM DD
ON SS.SS_SOLD_DATE_SK=DD.D_DATE_SK WHERE DD.D_YEAR={} AND DD.D_MOY={})S group by AGE;""".format(year,month)

@st.cache_data
def run_query_3(query):
    df=pd.read_sql_query(query,engine)
    return df

age_df=run_query_3(query)
c1 = alt.Chart(age_df,title='Age Distribution of Customers').mark_bar().encode(x='Age', y='count')
c1 = c1.properties(width=800, height=400)
#############################################################################################################################

 

#################################################################################
tab1, tab2, tab3 = st.tabs(["Monthly Customer Count per Year","Customer count analysis of the Month","Age distribution of Customers"])

with tab1:
    st.altair_chart(c2)

with tab2:   
    st.altair_chart(c1)
with tab3:
    st.altair_chart(age_df)
    
##########################################################################################################################
st.sidebar.title ('Revenue per Demographic') 

query="""SELECT sum(SS_NET_PAID) as REVENUE_MEN FROM STORE_SALES SS INNER JOIN DATE_DIM DD ON SS_SOLD_DATE_SK=D_DATE_SK
WHERE DD.D_YEAR={} AND DD.D_MOY={} and SS_CUSTOMER_SK IN 
(SELECT DISTINCT CD_DEMO_SK FROM CUSTOMER_DEMOGRAPHICS WHERE CD_GENDER='M');""".format(year,month)

total_revenue_men=run_query(query,'revenue_men')
st.sidebar.markdown('MEN')                     
st.sidebar.markdown('$'+str(shorten_num(total_revenue_men)))  

###########################################################################################################################
query="""SELECT sum(SS_NET_PAID) as REVENUE_WOMEN FROM STORE_SALES SS INNER JOIN DATE_DIM DD ON SS_SOLD_DATE_SK=D_DATE_SK
WHERE  DD.D_YEAR={} AND DD.D_MOY={} and SS_CUSTOMER_SK IN 
(SELECT DISTINCT CD_DEMO_SK FROM CUSTOMER_DEMOGRAPHICS WHERE CD_GENDER='F');""".format(year,month)

total_revenue_women=run_query(query,'revenue_women')
st.sidebar.markdown('WOMEN')                     
st.sidebar.markdown('$'+str(shorten_num(total_revenue_women)))  

###########################################################################################################################






