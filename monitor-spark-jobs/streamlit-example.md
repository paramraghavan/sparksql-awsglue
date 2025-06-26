**Streamlit** is a Python library that lets you create interactive web applications quickly and easily, especially
popular for data science and machine learning projects.

## What makes Streamlit special:

**No web development skills needed** - You write pure Python, and Streamlit automatically creates the web interface.

## Quick Example:

```python
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

# Run with: streamlit run app.py
```

This creates a full web app with:

- Title and text
- Input box
- Interactive chart
- Automatic layout and styling

## Key Features:

**Widgets**: Sliders, buttons, selectboxes, file uploaders

```python
age = st.slider("Age", 0, 100, 25)
uploaded_file = st.file_uploader("Choose CSV")
option = st.selectbox("Pick one", ["A", "B", "C"])
```

**Charts**: Built-in support for Plotly, Matplotlib, Altair

```python
st.line_chart(data)
st.bar_chart(data)
st.map(locations)
```

**Layouts**: Columns, sidebars, tabs

```python
col1, col2 = st.columns(2)
with col1:
    st.write("Left column")
with col2:
    st.write("Right column")
```

## Why it's perfect for EMR monitoring:

1. **Rapid prototyping** - Build dashboards in minutes
2. **Real-time updates** - Auto-refreshes when data changes
3. **Interactive** - Users can filter, drill down, explore data
4. **Easy deployment** - Single command to run
5. **Python ecosystem** - Works with pandas, requests, etc.

## Alternatives to Streamlit:

If you prefer other approaches:

**Jupyter Notebooks with Voila:**

```bash
pip install voila
voila your_notebook.ipynb --port=8866
```

**Dash by Plotly:**

```python
import dash
from dash import dcc, html

app = dash.Dash(__name__)
app.layout = html.Div([
    dcc.Graph(figure=fig)
])
app.run_server()
```

**Flask (more complex):**

```python
from flask import Flask, render_template

app = Flask(__name__)


@app.route('/')
def dashboard():
    return render_template('dashboard.html', data=data)
```

**Simple HTML + JavaScript:**

```html

<script>
    fetch('/api/cluster-metrics')
    .then(response => response.json())
    .then(data => updateCharts(data));
</script>
```

## For your EMR monitoring use case:

Streamlit is ideal because:

- You can quickly build charts showing resource usage
- Add filters for date ranges, users, job types
- Create real-time refreshing dashboards
- Team members can interact with the data
- No need to learn HTML/CSS/JavaScript

**Bottom line:** Streamlit turns your Python data analysis scripts into shareable web apps with minimal effort. Perfect
for internal tools like EMR monitoring dashboards.