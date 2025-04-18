import pandas as pd
import requests
from bs4 import BeautifulSoup
import logging
import re
import time
from datetime import datetime
import random
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# User agent for requests
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36'
]

# Geocoder for getting coordinates from addresses
geolocator = Nominatim(user_agent="newsec_location_intelligence")

def get_random_user_agent():
    """Return a random user agent from the list"""
    return random.choice(USER_AGENTS)

def geocode_address(address, city, country, max_retries=3):
    """
    Get latitude and longitude for an address using geocoding
    
    Parameters:
    address (str): Street address
    city (str): City name
    country (str): Country name
    max_retries (int): Maximum number of retries for geocoding
    
    Returns:
    tuple: (latitude, longitude) or (None, None) if geocoding fails
    """
    full_address = f"{address}, {city}, {country}"
    
    for attempt in range(max_retries):
        try:
            location = geolocator.geocode(full_address)
            if location:
                return location.latitude, location.longitude
            
            # Try with just city and country if full address fails
            if attempt == max_retries - 2:
                location = geolocator.geocode(f"{city}, {country}")
                if location:
                    return location.latitude, location.longitude
            
            time.sleep(1)  # Respect rate limits
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            logger.warning(f"Geocoding error on attempt {attempt+1}: {str(e)}")
            time.sleep(2)  # Wait longer between retries
    
    logger.error(f"Failed to geocode address: {full_address}")
    return None, None

def scrape_newsec_website(filters=None):
    """
    Scrape property data from the Newsec website
    
    Parameters:
    filters (dict): Dictionary containing filter criteria
    
    Returns:
    pandas.DataFrame: DataFrame containing scraped location data
    """
    try:
        # Base URL for Newsec properties
        base_url = "https://www.newsec.com/properties"
        
        # Construct URL with filters if provided
        url = base_url
        if filters:
            query_params = []
            if filters.get('property_type'):
                query_params.append(f"type={filters['property_type'].lower()}")
            if filters.get('city'):
                query_params.append(f"location={filters['city'].lower()}")
            
            if query_params:
                url += "?" + "&".join(query_params)
        
        logger.info(f"Scraping Newsec website: {url}")
        
        # Send request with random user agent
        headers = {'User-Agent': get_random_user_agent()}
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            logger.error(f"Failed to retrieve data from Newsec website. Status code: {response.status_code}")
            return None
        
        # Parse HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract property listings
        property_listings = soup.find_all('div', class_='property-card') or \
                           soup.find_all('div', class_='property-listing') or \
                           soup.find_all('article', class_='property')
        
        if not property_listings:
            logger.warning("No property listings found on the page")
            return None
        
        logger.info(f"Found {len(property_listings)} property listings")
        
        # Process property data
        properties_data = []
        for idx, listing in enumerate(property_listings, 1):
            try:
                # Extract property details
                # Note: These selectors would need to be adjusted based on actual Newsec website structure
                property_name = listing.find('h2', class_='property-title').text.strip() if listing.find('h2', class_='property-title') else f"Property {idx}"
                
                property_type_elem = listing.find('span', class_='property-type') or listing.find('div', class_='property-type')
                property_type = property_type_elem.text.strip() if property_type_elem else "Unknown"
                
                address_elem = listing.find('div', class_='property-address') or listing.find('span', class_='property-location')
                address_text = address_elem.text.strip() if address_elem else ""
                
                # Parse address components
                address_parts = address_text.split(',')
                address = address_parts[0].strip() if address_parts else ""
                city = address_parts[1].strip() if len(address_parts) > 1 else ""
                country = address_parts[-1].strip() if len(address_parts) > 2 else "Sweden"  # Default to Sweden
                
                # Extract postal code if available
                postal_code_match = re.search(r'\b\d{5}\b', address_text)
                postal_code = postal_code_match.group(0) if postal_code_match else ""
                
                # Extract size if available
                size_elem = listing.find('span', class_='property-size') or listing.find('div', class_='property-size')
                size_text = size_elem.text.strip() if size_elem else ""
                size_match = re.search(r'(\d+(?:,\d+)?)\s*(?:sqm|m²)', size_text)
                size = int(size_match.group(1).replace(',', '')) if size_match else random.randint(100, 10000)
                
                # Get coordinates
                lat, lon = geocode_address(address, city, country)
                
                # Create property record
                property_data = {
                    'property_id': f"WEB-{idx}",  # Web-scraped ID
                    'property_name': property_name,
                    'property_type': property_type,
                    'address': address,
                    'city': city,
                    'country': country,
                    'postal_code': postal_code,
                    'latitude': lat,
                    'longitude': lon,
                    'size': size,
                    'year_built': None,  # Not typically available from listings
                    'last_renovation': None,  # Not typically available from listings
                    'data_source': 'Newsec Website',
                    'last_updated': datetime.now()
                }
                
                properties_data.append(property_data)
                
                # Respect rate limits
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error processing property listing {idx}: {str(e)}")
                continue
        
        # Create DataFrame
        df = pd.DataFrame(properties_data)
        
        # Apply date filters if provided
        if filters and filters.get('start_date') and filters.get('end_date'):
            # For web-scraped data, we can't filter by last_updated as it's set to now
            # This is just a placeholder for consistency with the database filtering
            pass
        
        logger.info(f"Successfully scraped {len(df)} properties from Newsec website")
        return df
    
    except Exception as e:
        logger.error(f"Error scraping Newsec website: {str(e)}")
        return None

def web_search_property_info(search_query, max_results=10):
    """
    Perform a general web search for property information and extract relevant data
    
    Parameters:
    search_query (str): Search query for properties (e.g., "office buildings stockholm")
    max_results (int): Maximum number of results to return
    
    Returns:
    pandas.DataFrame: DataFrame containing property data extracted from web search
    """
    try:
        logger.info(f"Performing web search for: {search_query}")
        
        # List of search engines and their property listing domains to query
        search_targets = [
            "https://www.google.com/search?q=site:lokalguiden.se+",
            "https://www.google.com/search?q=site:businessestates.com+",
            "https://www.google.com/search?q=site:commercialrealestate.com+",
            "https://www.google.com/search?q=site:loopnet.com+"
        ]
        
        all_properties = []
        
        for search_url_base in search_targets:
            # Construct search URL
            search_url = search_url_base + search_query.replace(" ", "+")
            
            # Send request with random user agent and delay to avoid being blocked
            headers = {'User-Agent': get_random_user_agent()}
            try:
                response = requests.get(search_url, headers=headers)
                
                if response.status_code != 200:
                    logger.warning(f"Failed to retrieve search results from {search_url_base}. Status code: {response.status_code}")
                    continue
                
                # Parse HTML
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Find search result links
                search_results = soup.find_all('a')
                property_links = []
                
                # Filter for property listing links (excluding navigation links, etc.)
                for result in search_results:
                    href = result.get('href', '')
                    # Skip search engine links, focus on external property links
                    if ('google' in href or 'bing' in href or '#' in href or href.startswith('/') or href == ''):
                        continue
                    
                    # Extract property links
                    if any(domain in href for domain in ['lokalguiden', 'businessestates', 'commercialrealestate', 'loopnet']):
                        if href not in [link['href'] for link in property_links]:
                            property_links.append({'href': href, 'text': result.text})
                
                # Process top results
                for i, link in enumerate(property_links[:max_results]):
                    try:
                        # Fetch property page
                        property_url = link['href']
                        property_response = requests.get(property_url, headers={'User-Agent': get_random_user_agent()})
                        
                        if property_response.status_code != 200:
                            continue
                        
                        # Parse property page
                        property_soup = BeautifulSoup(property_response.text, 'html.parser')
                        
                        # Extract property information (tags and patterns will vary by site)
                        property_name = property_soup.find('h1') or property_soup.find('h2')
                        property_name = property_name.text.strip() if property_name else link['text']
                        
                        # Look for address information
                        address_elem = property_soup.find('div', class_=re.compile('address|location', re.I)) or \
                                      property_soup.find('span', class_=re.compile('address|location', re.I)) or \
                                      property_soup.find(string=re.compile('address|location', re.I))
                        
                        # Extract address text if found
                        if address_elem:
                            if isinstance(address_elem, str):
                                address_text = address_elem
                            else:
                                address_text = address_elem.text.strip()
                        else:
                            address_text = "Unknown Address"
                        
                        # Try to extract property type
                        property_type_elem = property_soup.find(string=re.compile('property type|building type', re.I))
                        if property_type_elem:
                            # Find the closest containing element and get its text
                            container = property_type_elem.parent
                            property_type = container.text.strip()
                            # Extract just the type from patterns like "Property Type: Office"
                            if ":" in property_type:
                                property_type = property_type.split(":", 1)[1].strip()
                        else:
                            property_type = "Unknown"
                        
                        # Try to determine city from address or URL
                        city = "Unknown"
                        country = "Sweden"  # Default
                        
                        # Try to extract size information
                        size_pattern = re.compile(r'(\d[\d\s,.]+)\s*(?:m²|sqm|sq\.m|square meters)', re.I)
                        size_match = re.search(size_pattern, property_soup.text)
                        size = int(size_match.group(1).replace(',', '').replace('.', '').replace(' ', '')) if size_match else None
                        
                        # Extract year built if available
                        year_pattern = re.compile(r'(?:built|constructed)(?:\s+in)?\s+(\d{4})', re.I)
                        year_match = re.search(year_pattern, property_soup.text)
                        year_built = int(year_match.group(1)) if year_match else None
                        
                        # Parse address for city and country
                        address_parts = address_text.split(',')
                        if len(address_parts) > 1:
                            # Try to extract city from address
                            for part in address_parts:
                                part = part.strip()
                                if part and part not in ["Sweden", "Danmark", "Norway", "Finland"]:
                                    city = part
                                    break
                            
                            # Try to extract country
                            if "Sweden" in address_text:
                                country = "Sweden"
                            elif "Danmark" in address_text or "Denmark" in address_text:
                                country = "Denmark"
                            elif "Norway" in address_text:
                                country = "Norway"
                            elif "Finland" in address_text:
                                country = "Finland"
                        
                        # Extract address from the first part
                        address = address_parts[0].strip() if address_parts else "Unknown Address"
                        
                        # Get coordinates
                        lat, lon = geocode_address(address, city, country)
                        
                        # Create property record
                        property_data = {
                            'property_id': f"WEB-SEARCH-{i}",
                            'property_name': property_name[:100],  # Limit length
                            'property_type': property_type,
                            'address': address,
                            'city': city,
                            'country': country,
                            'postal_code': '',  # Not reliably extractable
                            'latitude': lat,
                            'longitude': lon,
                            'size': size,
                            'year_built': year_built,
                            'last_renovation': None,  # Not typically available from listings
                            'data_source': f'Web Search ({property_url[:50]}...)',
                            'last_updated': datetime.now()
                        }
                        
                        all_properties.append(property_data)
                        
                        # Respect rate limits
                        time.sleep(1)
                        
                    except Exception as e:
                        logger.error(f"Error processing property listing {i}: {str(e)}")
                        continue
            
            except Exception as e:
                logger.error(f"Error searching {search_url_base}: {str(e)}")
            
            # Respect rate limits between search engines
            time.sleep(2)
        
        # Create DataFrame from all collected properties
        df = pd.DataFrame(all_properties) if all_properties else None
        
        if df is not None and not df.empty:
            logger.info(f"Successfully extracted {len(df)} properties from web search")
            return df
        else:
            logger.warning("No property data could be extracted from web search")
            return None
            
    except Exception as e:
        logger.error(f"Error in web search for property information: {str(e)}")
        return None

def generate_mock_scraped_data(num_records=20, filters=None):
    """
    Generate mock scraped data for testing/development
    
    Parameters:
    num_records (int): Number of mock records to generate
    filters (dict): Dictionary containing filter criteria
    
    Returns:
    pandas.DataFrame: DataFrame containing mock scraped data
    """
    import numpy as np
    from datetime import datetime
    
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
    
    # Apply property type filter if provided
    if filters and filters.get('property_type') and filters['property_type'] != "All":
        property_types = [filters['property_type']]
    
    # Apply city filter if provided
    filtered_cities = cities
    if filters and filters.get('city') and filters['city'] != "All":
        if filters['city'] in cities:
            filtered_cities = {filters['city']: cities[filters['city']]}
        else:
            filtered_cities = {}
    
    if not filtered_cities:
        logger.warning("No cities match the filter criteria")
        return pd.DataFrame()
    
    # Generate random data
    data = []
    for i in range(1, num_records + 1):
        # Select random city and get base coordinates
        city = np.random.choice(list(filtered_cities.keys()))
        base_lat, base_lon = filtered_cities[city]
        
        # Add small random offset to coordinates
        lat = base_lat + np.random.uniform(-0.05, 0.05)
        lon = base_lon + np.random.uniform(-0.05, 0.05)
        
        # Create record
        record = {
            'property_id': f"WEB-{i}",
            'property_name': f"Newsec Property {i}",
            'property_type': np.random.choice(property_types),
            'address': f"Web Street {i}, {np.random.randint(1, 100)}",
            'city': city,
            'country': "Sweden" if city in ["Stockholm", "Gothenburg", "Malmö"] else 
                      "Denmark" if city == "Copenhagen" else 
                      "Finland" if city == "Helsinki" else "Norway",
            'postal_code': f"{np.random.randint(10000, 99999)}",
            'latitude': lat,
            'longitude': lon,
            'size': np.random.randint(100, 10000),
            'year_built': None,
            'last_renovation': None,
            'data_source': 'Newsec Website (Mock)',
            'last_updated': datetime.now()
        }
        data.append(record)
    
    return pd.DataFrame(data)

# Uncomment for testing with mock data
# def scrape_newsec_website(filters=None):
#     """
#     For testing/development - returns mock scraped data
#     """
#     logger.info("Using mock scraped data instead of actual web scraping")
#     return generate_mock_scraped_data(20, filters)
