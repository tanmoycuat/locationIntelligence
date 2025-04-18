# Newsec Location Intelligence

A Streamlit application that provides location intelligence based on address data from the Synapse database and Newsec website.

## Features

- Interactive map view of properties with color-coded markers
- Data table with search and export functionality
- Analytics dashboard with charts and key metrics
- Excel export of data and summary reports
- Multiple data sources with automatic fallback

## Installation

1. Clone this repository
2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

3. Configure environment variables in a `.env` file:

```
DB_SERVER=your_server_name
DB_DATABASE=Synapse
DB_USERNAME=your_username
DB_PASSWORD=your_password
DB_DRIVER="ODBC Driver 17 for SQL Server"
```

## Usage

Run the Streamlit app:

```bash
streamlit run app.py
```

## Data Sources

1. **Synapse Database**: Primary source for property data
2. **Newsec Website**: Secondary source when database data is unavailable
3. **Sample Data**: Generated for testing purposes if neither source is available

## Files

- `app.py`: Main Streamlit application
- `database.py`: Database connection and queries
- `export.py`: Excel export functionality
- `scraper.py`: Web scraping functions for Newsec website
- `requirements.txt`: Required Python packages

## Dependencies

- Streamlit
- Pandas
- Folium
- Plotly
- SQLAlchemy
- PyODBC
- BeautifulSoup4
- Geopy

## License

Copyright (c) 2025 Newsec
