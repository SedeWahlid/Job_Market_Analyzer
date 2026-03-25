import streamlit as st
from supabase import create_client
import pandas as pd

st.set_page_config(layout="wide")

#################################################
# STREAMLIT VISUALIZATION + DATA ANALYSIS
#################################################

def get_tech_stack_percentages_from_db():
    try:
        supabase_key = st.secrets["SUPABASE_KEY"]
        supabase_url = st.secrets["PROJECT_URL"]
        supabase = create_client(supabase_url, supabase_key)
        all_tech_data = []
        start = 0
        chunk_size = 1000 # Fetch 1000 rows at a time
        while True:
            response = (
                supabase.from_("tech_stack")
                .select("job_id", "technology")
                .range(start, start + chunk_size - 1)
                .execute()
            )
            
            data = response.data
            
            # If no more data is returned, break the loop
            if not data:
                break
                
            all_tech_data.extend(data)
            start += chunk_size

        # Create a DataFrame from ALL the fetched data
        tech_df = pd.DataFrame(all_tech_data)
        
        # Count occurrences of each technology
        tech_counts = tech_df['technology'].value_counts()

        # Calculate unique job counts
        total_unique_jobs = len(tech_df['job_id'].unique())

        # Calculate percentages
        tech_percentages = (tech_counts / total_unique_jobs) * 100
        
        return tech_percentages, total_unique_jobs
        
    except Exception as e:
        st.error(f"Could not fetch the data with error : {e}")
        return None, 0

def display_tech_stack(tech_percentages):
    st.subheader('Tech Stack Distribution in Percentages')
    # Convert to DataFrame for Streamlit bar chart
    tech_df = pd.DataFrame(list(tech_percentages.items()), columns=['Tech Stack', 'Percentage'])
    st.bar_chart(tech_df.set_index('Tech Stack'))

#################################################
# MAIN
#################################################

st.title("Job Market Analyzer")

# Retrieve tech stack percentages from the database
tech_percentages, unique_jobs = get_tech_stack_percentages_from_db()

# Display results if data was successfully fetched
if tech_percentages is not None:
    display_tech_stack(tech_percentages)
    st.success(f"Successfully analyzed {len(tech_percentages)} unique technologies across {unique_jobs} total jobs!\n")