# Job Market Analyzer

## Overview

The Job Market Analyzer is an automated tool designed to scrape job listings from LinkedIn and Indeed, extract relevant technical stacks, and store the data in a Supabase database. It also provides a Streamlit-based visualization dashboard to display the distribution of different technologies used in job descriptions. Note: The location is striclty set to Germany !

## Features

- **Data Scraping**: Utilizes `jobspy` to gather job postings based on specified search terms and locations.
- **Tech Stack Extraction**: Identifies and categorizes technical stacks from job descriptions using predefined keywords.
- **Database Storage**: Stores job data and extracted tech stacks in a Supabase database for easy retrieval and analysis.
- **Visualization Dashboard**: Uses Streamlit to create an interactive dashboard displaying the distribution of technologies used in job postings.

## Components

### extractor.py
This Python script performs the core functionality:

1. **Data Scraping**: Collects job listings from LinkedIn and Indeed.
2. **Tech Stack Extraction**: Analyzes job descriptions to identify relevant technical stacks using predefined keywords.
3. **Database Insertion**: Inserts job data and tech stack information into a Supabase database.
4. **Streamlit Visualization**: Retrieves data from the database and displays it using Streamlit charts.

### run_weekly.yml
This GitHub Actions workflow file schedules the `extractor.py` script to run weekly:

- **Schedule**: Runs at 6:00 AM UTC every Monday.
- **Steps**:
  - Checks out the code repository.
  - Sets up Python and installs dependencies.
  - Executes the `extractor.py` script.

### requirements.txt
This file lists all the Python packages required for running the project:

- dotenv: For managing environment variables.
- supabase: For connecting to Supabase database.
- pandas: For data manipulation and analysis.
- python-jobspy: For scraping job listings from LinkedIn and Indeed.
- streamlit: For building interactive web applications.

## Setup

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/SedeWahlid/Job_Market_Analyzer.git
   cd job_market_analyzer
   ```

2. **Install Dependencies**:
   Ensure you have Python 3.x installed, then run:
   ```bash
   pip install -r requirements.txt
   ```

3. **Environment Variables**:
   Create a `.env` file in the root directory and add your Supabase API key and URL:
   ```
   SUPABASE_KEY=your-supabase-key
   PROJECT_URL=your-supabase-url
   ```

4. **Run the Script Manually with no Visuals**:
   ```bash
   python extractor.py
   ```

5. **Deploy Streamlit App** (Optional):
   You can deploy the Streamlit app on platforms like Streamlit Cloud or other hosting services.

## Usage

1. **Viewing Data**:
   You could also use the streamlit interface which is limited to just visualizing the data:
   ```bash
   streamlit run extractor.py
   ```

2. **Scheduling Runs**:
   The GitHub Actions workflow (`run_weekly.yml`) ensures that the script runs automatically every Monday at 6:00 AM UTC.

## Contributing

Contributions are welcome! Feel free to open issues and pull requests for improvements or new features.

## License

This project is licensed under the APACHE License - see the [LICENSE](LICENSE) file for details.