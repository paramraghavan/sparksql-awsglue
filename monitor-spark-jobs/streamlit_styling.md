Ways to hide the Streamlit footer and branding

## Method 1: CSS Injection (Most Common)

```python
import streamlit as st

# Hide Streamlit footer
hide_streamlit_style = """
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}
</style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True)
```

## Method 2: More Comprehensive Footer Hiding

```python
import streamlit as st

# Hide all Streamlit branding and menus
hide_st_style = """
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}
.stDeployButton {display:none;}
.stActionButton {display:none;}
#stDecoration {display:none;}
</style>
"""
st.markdown(hide_st_style, unsafe_allow_html=True)
```

## Method 3: Target Specific Elements

```python
import streamlit as st

# Hide specific Streamlit elements
hide_elements = """
<style>
/* Hide the main menu */
#MainMenu {visibility: hidden;}

/* Hide footer */
footer {visibility: hidden;}

/* Hide header */
header {visibility: hidden;}

/* Hide "Deploy" button */
.stDeployButton {display: none;}

/* Hide hamburger menu */
.css-14xtw13.e8zbici0 {display: none;}

/* Hide "Made with Streamlit" */
footer:after {
    content:''; 
    visibility: hidden;
    display: block;
}
</style>
"""
st.markdown(hide_elements, unsafe_allow_html=True)
```

## Method 4: Complete Branding Removal

```python
import streamlit as st

# Remove all Streamlit branding
no_streamlit = """
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}
.stApp > footer {visibility: hidden;}
.stApp > header {visibility: hidden;}
.css-18e3th9 {padding-top: 0rem;}
.css-hxt7ib {padding-top: 2rem;}
.stDeployButton {display:none;}
#stDecoration {display:none;}
.reportview-container .main footer {visibility: hidden;}
.reportview-container .main footer:after {
    content:''; 
    visibility: hidden;
    display: block;
}
</style>
"""
st.markdown(no_streamlit, unsafe_allow_html=True)
```

## Method 5: Function to Apply Styling

```python
import streamlit as st

def hide_streamlit_branding():
    """Hide Streamlit footer and branding"""
    hide_style = """
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    .stDeployButton {display:none;}
    .stActionButton {display:none;}
    </style>
    """
    st.markdown(hide_style, unsafe_allow_html=True)

# Usage
hide_streamlit_branding()
```

## Method 6: Config-based Approach

You can also modify the Streamlit config:

```python
# In your .streamlit/config.toml file
[theme]
base = "light"
primaryColor = "#your_color"

[browser]
gatherUsageStats = false

[server]
enableCORS = false
enableXsrfProtection = false
```

## For Your EMR Monitor Dashboard

Add this to your existing Streamlit code:

```python
import streamlit as st

def apply_custom_styling():
    """Apply custom styling to hide Streamlit branding"""
    st.markdown("""
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    .stDeployButton {display:none;}
    .stActionButton {display:none;}
    
    /* Custom app styling */
    .main-header {
        padding: 1rem 0;
        border-bottom: 2px solid #f0f0f0;
        margin-bottom: 2rem;
    }
    </style>
    """, unsafe_allow_html=True)

# At the top of your main() function:
def main():
    st.set_page_config(page_title="EMR Job Resource Monitor", layout="wide")
    apply_custom_styling()  # Add this line
    st.title("EMR Job Resource Monitor - Tabular View")
    # ... rest of your code
```

## Method 7: Environment Variable

For production deployments, you can also set:

```bash
export STREAMLIT_THEME_BASE="light"
export STREAMLIT_BROWSER_GATHER_USAGE_STATS=false
```

## Best Practice

The most reliable method is **Method 1** or **Method 2** as they work across different Streamlit versions. Add it right after your `st.set_page_config()` call:

```python
st.set_page_config(page_title="EMR Job Resource Monitor", layout="wide")

# Hide Streamlit branding
hide_streamlit_style = """
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}
</style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True)
```