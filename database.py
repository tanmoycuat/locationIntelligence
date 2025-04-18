import pandas as pd
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine, text
import requests
from bs4 import BeautifulSoup
import logging
import os
from dotenv import load_dotenv
from scraper import scrape_newsec_website, web_search_property_info

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Database connection parameters
DB_SERVER = os.getenv("DB_SERVER", "your_server_name")
DB_DATABASE = os.getenv("DB_DATABASE", "Synapse")
DB_USERNAME = os.getenv("DB_USERNAME", "your_username")
DB_PASSWORD = os.getenv("DB_PASSWORD", "your_password")
DB_DRIVER = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")

def get_db_connection():
    """
    Establish a connection to the Synapse database
    """
    try:
        connection_string = f"DRIVER={{{DB_DRIVER}}};SERVER={DB_SERVER};DATABASE={DB_DATABASE};UID={DB_USERNAME};PWD={DB_PASSWORD}"
        conn = pyodbc.connect(connection_string)
        logger.info("Successfully connected to the Synapse database")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        return None

def get_sqlalchemy_engine():
    """
    Create a SQLAlchemy engine for the Synapse database
    """
    try:
        connection_url = f"mssql+pyodbc://{DB_USERNAME}:{DB_PASSWORD}@{DB_SERVER}/{DB_DATABASE}?driver={DB_DRIVER.replace(' ', '+')}"
        engine = create_engine(connection_url)
        logger.info("Successfully created SQLAlchemy engine")
        return engine
    except Exception as e:
        logger.error(f"Error creating SQLAlchemy engine: {str(e)}")
        return None

def get_location_data(filters=None):
    """
    Retrieve location data from the Synapse database based on filters
    If data is not available in the database, fall back to web scraping and web search
    
    Parameters:
    filters (dict): Dictionary containing filter criteria
    
    Returns:
    pandas.DataFrame: DataFrame containing location data
    """
    # Default empty DataFrame with expected columns
    columns = [
        'property_id', 'property_name', 'property_type', 'address', 'city', 
        'country', 'postal_code', 'latitude', 'longitude', 'size', 
        'year_built', 'last_renovation', 'data_source', 'last_updated'
    ]
    empty_df = pd.DataFrame(columns=columns)
    
    # Try to get data from database first
    db_data = get_data_from_database(filters)
    
    # If database data is empty or insufficient, try web scraping
    if db_data is None or db_data.empty or len(db_data) < 5:  # Arbitrary threshold
        logger.info("Insufficient data from database, attempting to scrape website")
        web_data = scrape_newsec_website(filters)
        
        # If web scraped data is still insufficient, try general web search
        if web_data is None or web_data.empty or len(web_data) < 5:
            logger.info("Insufficient data from Newsec website, attempting general web search")
            
            # Construct search query from filters
            search_query = "commercial property"
            if filters:
                if filters.get('property_type'):
                    search_query += f" {filters['property_type']}"
                if filters.get('city'):
                    search_query += f" {filters['city']}"
            
            # Add country context if not specified
            if filters and not any(country in search_query.lower() for country in ['sweden', 'denmark', 'norway', 'finland']):
                search_query += " Sweden"  # Default to Sweden
            
            # Perform web search
            websearch_data = web_search_property_info(search_query, max_results=10)
            
            # Combine all available data sources
            data_sources = []
            
            if db_data is not None and not db_data.empty:
                data_sources.append(db_data)
                
            if web_data is not None and not web_data.empty:
                data_sources.append(web_data)
                
            if websearch_data is not None and not websearch_data.empty:
                data_sources.append(websearch_data)
            
            if data_sources:
                # Combine all available data sources and remove duplicates
                combined_data = pd.concat(data_sources).drop_duplicates(subset=['property_id'])
                logger.info(f"Combined data from multiple sources: {len(combined_data)} total properties")
                return combined_data
            else:
                logger.warning("No data available from any source (database, website, or web search)")
                return empty_df
        else:
            # Combine data if both database and Newsec website have data
            if db_data is not None and not db_data.empty:
                # Combine and remove duplicates based on property_id
                combined_data = pd.concat([db_data, web_data]).drop_duplicates(subset=['property_id'])
                return combined_data
            else:
                return web_data
    else:
        return db_data

def get_data_from_database(filters=None):
    """
    Query the Synapse database for location data based on filters
    
    Parameters:
    filters (dict): Dictionary containing filter criteria
    
    Returns:
    pandas.DataFrame: DataFrame containing location data from database
    """
    try:
        # Get database connection
        engine = get_sqlalchemy_engine()
        if engine is None:
            logger.error("Failed to create database engine")
            return None
        
        # Build SQL query based on filters
        query = """
        SELECT 
            p.PropertyID as property_id,
            p.PropertyName as property_name,
            p.PropertyType as property_type,
            a.AddressLine1 + ISNULL(', ' + a.AddressLine2, '') as address,
            a.City as city,
            a.Country as country,
            a.PostalCode as postal_code,
            a.Latitude as latitude,
            a.Longitude as longitude,
            p.Size as size,
            p.YearBuilt as year_built,
            p.LastRenovation as last_renovation,
            'Synapse Database' as data_source,
            p.LastUpdated as last_updated
        FROM 
            Properties p
        JOIN 
            Addresses a ON p.AddressID = a.AddressID
        WHERE 
            1=1
        """
        
        # Add filter conditions if provided
        params = {}
        if filters:
            if filters.get('property_type'):
                query += " AND p.PropertyType = :property_type"
                params['property_type'] = filters['property_type']
            
            if filters.get('city'):
                query += " AND a.City = :city"
                params['city'] = filters['city']
            
            if filters.get('start_date') and filters.get('end_date'):
                query += " AND p.LastUpdated BETWEEN :start_date AND :end_date"
                params['start_date'] = filters['start_date']
                params['end_date'] = filters['end_date']
        
        # Execute query
        with engine.connect() as connection:
            result = connection.execute(text(query), params)
            data = pd.DataFrame(result.fetchall())
            
            # Set column names if data is not empty
            if not data.empty:
                data.columns = result.keys()
        
        logger.info(f"Retrieved {len(data) if data is not None else 0} records from database")
        return data
    
    except Exception as e:
        logger.error(f"Error retrieving data from database: {str(e)}")
        return None

# For testing/development - sample data generator
def generate_sample_data(num_records=50):
    """
    Generate sample location data for testing/development
    
    Parameters:
    num_records (int): Number of sample records to generate
    
    Returns:
    pandas.DataFrame: DataFrame containing sample location data
    """
    import numpy as np
    from datetime import datetime, timedelta
    
    # Sample property types
    property_types = ["Office", "Retail", "Industrial", "Residential"]
    
    # Sample cities with approximate coordinates
    cities = {
        "Stockholm": (59.3293, 18.0686),
        "Gothenburg": (57.7089, 11.9746),
        "Malmö": (55.6050, 13.0038),
        "Copenhagen": (55.6761, 12.5683),
        "Helsinki": (60.1699, 24.9384),
        "Oslo": (59.9139, 10.7522)
    }
    
    # Generate random data
    data = []
    for i in range(1, num_records + 1):
        # Select random city and get base coordinates
        city = np.random.choice(list(cities.keys()))
        base_lat, base_lon = cities[city]
        
        # Add small random offset to coordinates
        lat = base_lat + np.random.uniform(-0.05, 0.05)
        lon = base_lon + np.random.uniform(-0.05, 0.05)
        
        # Generate random dates
        built_year = np.random.randint(1950, 2020)
        renovation_year = np.random.randint(built_year, 2023) if np.random.random() > 0.3 else None
        last_updated = datetime.now() - timedelta(days=np.random.randint(1, 365))
        
        # Create record
        record = {
            'property_id': i,
            'property_name': f"Property {i}",
            'property_type': np.random.choice(property_types),
            'address': f"Street {i}, {np.random.randint(1, 100)}",
            'city': city,
            'country': "Sweden" if city in ["Stockholm", "Gothenburg", "Malmö"] else 
                      "Denmark" if city == "Copenhagen" else 
                      "Finland" if city == "Helsinki" else "Norway",
            'postal_code': f"{np.random.randint(10000, 99999)}",
            'latitude': lat,
            'longitude': lon,
            'size': np.random.randint(100, 10000),
            'year_built': built_year,
            'last_renovation': renovation_year,
            'data_source': 'Sample Data',
            'last_updated': last_updated
        }
        data.append(record)
    
    return pd.DataFrame(data)

# Uncomment for testing with sample data
# def get_location_data(filters=None):
#     """
#     For testing/development - returns sample data
#     """
#     data = generate_sample_data(50)
#     
#     # Apply filters if provided
#     if filters:
#         if filters.get('property_type'):
#             data = data[data['property_type'] == filters['property_type']]
#         
#         if filters.get('city'):
#             data = data[data['city'] == filters['city']]
#         
#         if filters.get('start_date') and filters.get('end_date'):
#             data = data[(data['last_updated'] >= filters['start_date']) & 
#                         (data['last_updated'] <= filters['end_date'])]
#     
#     return data
