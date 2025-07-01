import streamlit as st

# Hide the footer and menu
hide_streamlit_style = """
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {visibility: hidden;}
    </style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

# Your app content
st.title("My Clean Streamlit App")
st.write("This app has no footer or help message!")
