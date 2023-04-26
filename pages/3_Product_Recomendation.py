import streamlit as st
import snowflake.connector
import pandas as pd
import openai
import altair as alt
from openai.embeddings_utils import get_embedding, cosine_similarity
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

st.write("<h1 style='text-align: center;'>Recomendations for Customers</h1>", unsafe_allow_html=True)


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

#st.write(df.head())


df_grouped = df.groupby('SS_TICKET_NUMBER')['SS_ITEM_SK'].agg(lambda x: ','.join(map(str, x))).reset_index()

# transform dataset into a one-hot encoded format
df_encoded = df_grouped['SS_ITEM_SK'].str.get_dummies(',')

# apply Apriori algorithm to identify frequent itemsets
frequent_itemsets = apriori(df_encoded, min_support=0.04, use_colnames=True)

st.write("Colaborative Recommendation")
with st.beta_expander("Expand to see pairs of products brought together"):
    st.write('Recomended Products Pairs')

    # check if there are any frequent itemsets
    if len(frequent_itemsets) > 0:
        # generate association rules from frequent itemsets
        rules = association_rules(frequent_itemsets, metric='lift', min_threshold=1)
        # create dictionary of antecedents and consequents
        antecedent_consequent_dict = dict(zip(rules['antecedents'], rules['consequents']))
        # convert frozen sets to regular sets
        rules[['antecedents', 'consequents']] = rules[['antecedents', 'consequents']].applymap(set)
        # rename the columns
        rules.rename(columns={'antecedents': 'Product A', 'consequents': 'Product B'}, inplace=True)
        # select only the desired columns
        rules = rules.loc[:, ['Product A', 'Product B', 'confidence']]

        # print the resulting association rules
        st.write(rules.head())
    else:
        st.write("No frequent itemsets found with the given minimum support value.")


# Create the dropdown select
product = st.selectbox('Select a product you would like to buy :', rules['Product A'])
product_a_str ='(' + ','.join(map(str, product)) + ')'

# find the corresponding value(s) of Product B in antecedent_consequent_dict
product_b = antecedent_consequent_dict.get(frozenset(product), None)

st.write("The Details For the product you like to buy are  :")

query = f"SELECT I_ITEM_ID,I_PRODUCT_NAME,I_CLASS,I_CATEGORY,I_ITEM_DESC FROM Item WHERE i_item_sk in {product_a_str}"

# Execute the query
df = get_data_from_snowflake(query)

# Display the result
st.write(df)
clas =df['I_CLASS']
st.write(clas)

product_b_str ='(' + ','.join(map(str, product_b)) + ')'
#st.write(product_b_str)
st.write("You may also like to look at before you complete your purchase :")

query = f"SELECT I_ITEM_ID,I_PRODUCT_NAME,I_CLASS,I_CATEGORY,I_ITEM_DESC FROM Item WHERE i_item_sk in {product_b_str}"

# Execute the query
df = get_data_from_snowflake(query)
# Display the result
st.write(df)



st.write("Context Based Recomendations")
openai.api_key = st.secrets["api_key"]

query = 'select i_item_sk,i_class,i_category,i_color from item limit 20'

# Execute the query
@st.cache(allow_output_mutation=True)
def load_product_data():
    product_data_df = get_data_from_snowflake(query)
    product_data_df['combined'] = product_data_df.apply(lambda row: f"{row['I_CLASS']}, {row['I_CATEGORY']}, {row['I_COLOR']}", axis=1)
    product_data_df['text_embedding'] = product_data_df.combined.apply(lambda x: get_embedding(x, engine='text-embedding-ada-002'))
    return product_data_df

# Load the product data
product_data_df = load_product_data()


    
customer_input = st.text_input("Hi! How can I help you Today ?" )

response = openai.Embedding.create(
    input=customer_input,
    model="text-embedding-ada-002"
)
embeddings_customer_question = response['data'][0]['embedding']

product_data_df['search_products'] = product_data_df.text_embedding.apply(lambda x: cosine_similarity(x, embeddings_customer_question))
product_data_df = product_data_df.sort_values('search_products', ascending=False)

top_3_products_df=product_data_df.head(3)
st.write("Here are some Products you might be Intrested in:")
st.write(top_3_products_df)
