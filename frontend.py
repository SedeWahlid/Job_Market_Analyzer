import streamlit as st
from supabase import create_client
import pandas as pd

#################################################
# STREAMLIT VISUALIZATION + DATA ANALYSIS
#################################################

def get_tech_stack_percentages_from_db():
    supabase_key = st.secrets["SUPABASE_KEY"]
    supabase_url = st.secrets["PROJECT_URL"]
    supabase = create_client(supabase_url, supabase_key)

    # Query the tech stack data from the database
    response = supabase.from_("tech_stack").select("job_id", "technology").execute()
    tech_data = response.data

    # Create a DataFrame from the fetched data
    tech_df = pd.DataFrame(tech_data)
    
    # Count occurrences of each technology
    tech_counts = tech_df['technology'].value_counts()

    # Calculate unique job counts
    total_unique_jobs = len(tech_df['job_id'].unique())

    # Calculate percentages
    tech_percentages = (tech_counts / total_unique_jobs) * 100

    return tech_percentages

def display_tech_stack(tech_percentages):
    st.subheader('Tech Stack Distribution')
    tech_df = pd.DataFrame(list(tech_percentages.items()), columns=['Tech Stack', 'Total Count'])
    st.bar_chart(tech_df.set_index('Tech Stack'))

# Retrieve tech stack percentages from the database
tech_percentages = get_tech_stack_percentages_from_db()

# Display tech stacks in one chart
st.title("Job market Analyzer")
st.set_page_config(layout="wide")
display_tech_stack(tech_percentages)
