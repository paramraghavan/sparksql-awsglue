import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
from datetime import datetime, date
import time

# Page Configuration
st.set_page_config(
    page_title="Streamlit Components Guide",
    page_icon="🚀",
    layout="wide",  # or "centered"
    initial_sidebar_state="expanded"
)

st.title("🚀 Streamlit Components Reference Guide")
st.markdown("A comprehensive guide to commonly used Streamlit components")

# ==============================================================================
# 1. TEXT AND MARKDOWN COMPONENTS
# ==============================================================================
st.header("1. Text and Markdown Components")

# Basic text
st.text("This is plain text")
st.markdown("This is **bold** and *italic* text")
st.markdown("### This is a markdown header")

# Code display
st.code("""
def hello():
    print("Hello, Streamlit!")
""", language='python')

# LaTeX
st.latex(r'''
    e^{i\pi} + 1 = 0
''')

# Colored text and alerts
st.success("✅ Success message")
st.info("ℹ️ Info message")
st.warning("⚠️ Warning message")
st.error("❌ Error message")
st.exception(RuntimeError("This is an exception"))

# ==============================================================================
# 2. INPUT WIDGETS
# ==============================================================================
st.header("2. Input Widgets")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Text Inputs")

    # Text input
    name = st.text_input("Enter your name", value="John Doe")
    st.write(f"Hello, {name}!")

    # Text area
    feedback = st.text_area("Your feedback", height=100)

    # Password
    password = st.text_input("Password", type="password")

    # Number inputs
    age = st.number_input("Age", min_value=0, max_value=120, value=25)
    price = st.number_input("Price", min_value=0.0, value=10.5, step=0.1)

with col2:
    st.subheader("Selection Widgets")

    # Selectbox (dropdown)
    country = st.selectbox("Choose country", ["USA", "Canada", "UK", "Germany"])

    # Multiselect
    colors = st.multiselect("Choose colors", ["Red", "Green", "Blue", "Yellow"])

    # Radio buttons
    gender = st.radio("Gender", ["Male", "Female", "Other"])

    # Checkbox
    agree = st.checkbox("I agree to terms and conditions")

    # Slider
    temperature = st.slider("Temperature", -10, 40, 20)

    # Select slider
    size = st.select_slider("Size", options=["XS", "S", "M", "L", "XL"])

# Date and time inputs
st.subheader("Date and Time Inputs")
col1, col2, col3 = st.columns(3)
with col1:
    birth_date = st.date_input("Birth date", value=date(1990, 1, 1))
with col2:
    appointment_time = st.time_input("Appointment time")
with col3:
    event_datetime = st.datetime_input("Event date and time")

# File uploader
st.subheader("File Upload")
uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)
    st.write("Uploaded data preview:")
    st.dataframe(df.head())

# ==============================================================================
# 3. DISPLAY COMPONENTS
# ==============================================================================
st.header("3. Display Components")

# Create sample data
sample_data = pd.DataFrame({
    'Name': ['Alice', 'Bob', 'Charlie', 'Diana'],
    'Age': [25, 30, 35, 28],
    'City': ['New York', 'London', 'Tokyo', 'Paris'],
    'Salary': [50000, 60000, 75000, 55000]
})

# Dataframe
st.subheader("DataFrames and Tables")
st.dataframe(sample_data)  # Interactive dataframe
st.table(sample_data)  # Static table

# Metrics
st.subheader("Metrics")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Users", "1,234", "12%")
col2.metric("Revenue", "$45,678", "-2%")
col3.metric("Conversion Rate", "3.2%", "0.5%")
col4.metric("Avg Session", "4m 32s", "1m 2s")

# JSON display
st.subheader("JSON Display")
sample_json = {
    "name": "John",
    "age": 30,
    "skills": ["Python", "Streamlit", "Data Science"]
}
st.json(sample_json)

# ==============================================================================
# 4. CHARTS AND VISUALIZATION
# ==============================================================================
st.header("4. Charts and Visualization")

# Generate sample data for charts
chart_data = pd.DataFrame(
    np.random.randn(20, 3),
    columns=['A', 'B', 'C']
)

col1, col2 = st.columns(2)

with col1:
    st.subheader("Line Chart")
    st.line_chart(chart_data)

    st.subheader("Bar Chart")
    st.bar_chart(chart_data)

with col2:
    st.subheader("Area Chart")
    st.area_chart(chart_data)

    st.subheader("Scatter Chart")
    scatter_data = pd.DataFrame({
        'x': np.random.randn(100),
        'y': np.random.randn(100)
    })
    st.scatter_chart(scatter_data)

# Plotly chart
st.subheader("Plotly Chart")
fig = px.scatter(sample_data, x="Age", y="Salary", color="City", size="Age")
st.plotly_chart(fig, use_container_width=True)

# Matplotlib chart
st.subheader("Matplotlib Chart")
fig, ax = plt.subplots()
ax.hist(np.random.normal(0, 1, 1000), bins=30)
ax.set_title("Normal Distribution")
st.pyplot(fig)

# Map
st.subheader("Map")
map_data = pd.DataFrame({
    'lat': [37.76, 37.77, 37.78],
    'lon': [-122.4, -122.41, -122.42]
})
st.map(map_data)

# ==============================================================================
# 5. LAYOUT COMPONENTS
# ==============================================================================
st.header("5. Layout Components")

# Columns
st.subheader("Columns")
col1, col2, col3 = st.columns([1, 2, 1])  # Different widths
with col1:
    st.write("Column 1")
    st.button("Button 1")
with col2:
    st.write("Column 2 (wider)")
    st.slider("Slider", 0, 100)
with col3:
    st.write("Column 3")
    st.checkbox("Check me")

# Tabs
st.subheader("Tabs")
tab1, tab2, tab3 = st.tabs(["📈 Chart", "🗃 Data", "⚙️ Config"])
with tab1:
    st.line_chart(chart_data)
with tab2:
    st.dataframe(sample_data)
with tab3:
    st.slider("Configuration value", 0, 100, 50)

# Expandable sections
st.subheader("Expandable Sections")
with st.expander("Click to expand"):
    st.write("This content is hidden until expanded")
    st.image("https://static.streamlit.io/examples/cat.jpg", width=200)

# Container
st.subheader("Containers")
container = st.container()
with container:
    st.write("This is inside a container")

# Empty placeholder
placeholder = st.empty()
placeholder.text("This will be replaced...")

# ==============================================================================
# 6. SIDEBAR
# ==============================================================================
st.sidebar.header("Sidebar Components")
st.sidebar.selectbox("Navigation", ["Home", "Data", "Charts", "Settings"])
st.sidebar.slider("Sidebar slider", 0, 100, 50)
st.sidebar.text_input("Sidebar input")

# ==============================================================================
# 7. INTERACTIVE COMPONENTS
# ==============================================================================
st.header("7. Interactive Components")

# Buttons
col1, col2, col3 = st.columns(3)
with col1:
    if st.button("Primary Button"):
        st.success("Primary button clicked!")

with col2:
    if st.button("Secondary Button", type="secondary"):
        st.info("Secondary button clicked!")

with col3:
    if st.download_button(
            label="Download CSV",
            data=sample_data.to_csv(index=False),
            file_name="sample_data.csv",
            mime="text/csv"
    ):
        st.success("Download started!")

# Forms
st.subheader("Forms")
with st.form("my_form"):
    st.write("Inside the form")
    form_name = st.text_input("Name in form")
    form_age = st.number_input("Age in form", min_value=0, max_value=120)

    # Every form must have a submit button
    submitted = st.form_submit_button("Submit")
    if submitted:
        st.write(f"Submitted: {form_name}, {form_age}")

# ==============================================================================
# 8. ADVANCED COMPONENTS
# ==============================================================================
st.header("8. Advanced Components")

# Progress bar
st.subheader("Progress Bar")
if st.button("Start Progress"):
    progress_bar = st.progress(0)
    status_text = st.empty()

    for i in range(100):
        progress_bar.progress(i + 1)
        status_text.text(f"Progress: {i + 1}%")
        time.sleep(0.01)

    status_text.text("Complete!")

# Spinner
st.subheader("Spinner")
if st.button("Show Spinner"):
    with st.spinner("Loading..."):
        time.sleep(2)
    st.success("Done!")

# Balloons and snow
col1, col2 = st.columns(2)
with col1:
    if st.button("🎈 Balloons"):
        st.balloons()
with col2:
    if st.button("❄️ Snow"):
        st.snow()

# ==============================================================================
# 9. STATE MANAGEMENT
# ==============================================================================
st.header("9. State Management")

# Session state
if 'counter' not in st.session_state:
    st.session_state.counter = 0

col1, col2, col3 = st.columns(3)
with col1:
    if st.button("Increment"):
        st.session_state.counter += 1
with col2:
    if st.button("Decrement"):
        st.session_state.counter -= 1
with col3:
    if st.button("Reset"):
        st.session_state.counter = 0

st.write(f"Counter value: {st.session_state.counter}")

# ==============================================================================
# 10. CACHING
# ==============================================================================
st.header("10. Caching")


@st.cache_data  # New caching decorator
def load_large_data():
    # Simulate expensive computation
    time.sleep(1)
    return pd.DataFrame(np.random.randn(1000, 5))


if st.button("Load Cached Data"):
    with st.spinner("Loading data..."):
        data = load_large_data()
    st.dataframe(data.head())
    st.success("Data loaded (cached for subsequent runs)")

# ==============================================================================
# 11. ERROR HANDLING AND DEBUGGING
# ==============================================================================
st.header("11. Error Handling and Debugging")

# Stop execution
if st.checkbox("Show stop example"):
    st.write("This will show")
    st.stop()  # Everything below this won't execute
    st.write("This won't show")

# Debug info
if st.checkbox("Show debug info"):
    st.write("Session state:", st.session_state)
    st.write("Query params:", st.experimental_get_query_params())

# ==============================================================================
# 12. CUSTOM STYLING
# ==============================================================================
st.header("12. Custom Styling")

# HTML and CSS
st.markdown("""
<style>
.custom-text {
    color: red;
    font-size: 20px;
    font-weight: bold;
}
</style>
<div class="custom-text">
    This is custom styled text!
</div>
""", unsafe_allow_html=True)

# Colored metrics
st.markdown("""
<style>
[data-testid="metric-container"] {
   background-color: rgba(28, 131, 225, 0.1);
   border: 1px solid rgba(28, 131, 225, 0.1);
   padding: 5% 5% 5% 10%;
   border-radius: 5px;
   color: rgb(30, 103, 119);
   overflow-wrap: break-word;
}
</style>
""", unsafe_allow_html=True)

# ==============================================================================
# EXAMPLE: REAL-WORLD DASHBOARD
# ==============================================================================
st.header("13. Real-World Example: Sales Dashboard")

# Sample sales data
sales_data = pd.DataFrame({
    'Date': pd.date_range('2024-01-01', periods=30),
    'Sales': np.random.randint(1000, 5000, 30),
    'Region': np.random.choice(['North', 'South', 'East', 'West'], 30),
    'Product': np.random.choice(['A', 'B', 'C'], 30)
})

# Sidebar filters
st.sidebar.subheader("Dashboard Filters")
selected_region = st.sidebar.multiselect("Select Region", sales_data['Region'].unique())
date_range = st.sidebar.date_input("Date Range",
                                   value=[sales_data['Date'].min(), sales_data['Date'].max()],
                                   min_value=sales_data['Date'].min(),
                                   max_value=sales_data['Date'].max())

# Filter data
if selected_region:
    filtered_data = sales_data[sales_data['Region'].isin(selected_region)]
else:
    filtered_data = sales_data

# Dashboard metrics
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Sales", f"${filtered_data['Sales'].sum():,}")
col2.metric("Avg Daily Sales", f"${filtered_data['Sales'].mean():.0f}")
col3.metric("Best Day", f"${filtered_data['Sales'].max():,}")
col4.metric("Total Days", len(filtered_data))

# Charts
col1, col2 = st.columns(2)
with col1:
    st.subheader("Sales Trend")
    st.line_chart(filtered_data.set_index('Date')['Sales'])

with col2:
    st.subheader("Sales by Region")
    region_sales = filtered_data.groupby('Region')['Sales'].sum()
    st.bar_chart(region_sales)

# Data table
st.subheader("Raw Data")
st.dataframe(filtered_data, use_container_width=True)

# Download button
csv_data = filtered_data.to_csv(index=False)
st.download_button(
    label="Download filtered data as CSV",
    data=csv_data,
    file_name="sales_data.csv",
    mime="text/csv"
)

st.markdown("---")
st.markdown("### 🎉 That's a comprehensive overview of Streamlit components!")
st.markdown("Use these components to build interactive data applications quickly and easily.")