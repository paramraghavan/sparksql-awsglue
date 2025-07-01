import streamlit as st
import pandas as pd
import plotly.express as px

# This creates a web page with these elements
st.title("My Dashboard")
st.write("Hello World!")

# Interactive widgets
name = st.text_input("Enter your name:")
st.write(f"Hello {name}!")

# Data visualization
df = pd.DataFrame({'x': [1, 2, 3, 4], 'y': [10, 20, 30, 40]})
fig = px.line(df, x='x', y='y')
st.plotly_chart(fig)