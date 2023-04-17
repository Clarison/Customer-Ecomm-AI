import streamlit as st

# Add a title
st.title("Hello!")

# Add some text
st.write("Welcome to my Streamlit app! Say hello below.")

# Add an input field for the user to enter their name
name = st.text_input("Enter your name")

# Add a button to submit the name
if st.button("Submit"):
    st.write(f"Hello, {name}!")
