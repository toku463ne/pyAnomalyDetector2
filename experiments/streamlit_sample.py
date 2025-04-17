import streamlit as st
import pandas as pd
import numpy as np
import pdb

# Generate sample data
def generate_sample_data():
    time = pd.date_range(start='2023-01-01', periods=100, freq='D')
    data = {f'Data{i}': np.random.randn(100).cumsum() for i in range(1, 13)}
    data['Time'] = time
    return pd.DataFrame(data)

# Create tabs
tab1, tab2, tab3 = st.tabs(["Tab 1", "Tab 2", "Tab 3"])

# Generate data for charts
data = generate_sample_data()
data.set_index('Time', inplace=True)

# Function to display 6 charts in a tab (2x3 layout)
def display_charts(tab, data):
    #pdb.set_trace()
    with tab:
        for row in range(2):  # 2 rows
            cols = st.columns(3)  # 3 columns
            for col, i in zip(cols, range(row * 3 + 1, row * 3 + 4)):
                if f'Data{i}' in data.columns:
                    with col:
                        st.write(f"Timeline Chart {i}")
                        st.line_chart(data[f'Data{i}'])

# Display charts in each tab
display_charts(tab1, data)
display_charts(tab2, data)
display_charts(tab3, data)
