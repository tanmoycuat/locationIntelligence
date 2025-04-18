import streamlit as st
import pandas as pd
import folium
import plotly.express as px
import os
import logging
import sys
from database import get_location_data, generate_sample_data
from export import export_to_excel, export_summary_report
from scraper import generate_mock_scraped_data
import time
import traceback
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Try to import streamlit_folium, but provide fallback if not available
try:
    from streamlit_folium import folium_static
    FOLIUM_AVAILABLE = True
except ImportError:
    logger.warning("streamlit_folium not installed. Map view will be limited.")
    FOLIUM_AVAILABLE = False
    st.warning("For full map functionality, install streamlit-folium: `pip install streamlit-folium`")

# Set page configuration
st.set_page_config(
    page_title="Newsec Location Intelligence",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Title and description
st.title("Newsec Location Intelligence")
st.markdown("""
This application provides location intelligence based on address data from the Synapse database 
and Newsec website. Use the filters to explore properties and export the data to Excel.
""")

# Add information about data sources
with st.expander("About Data Sources"):
    st.markdown("""
    This application uses the following data sources:
    
    1. **Synapse Database**: Primary source for property data
    2. **Newsec Website**: Secondary source when database data is unavailable
    
    For testing purposes, sample data is generated if neither source is available.
    """)

def load_data(filters):
    """
    Load data with progress bar and proper error handling
    
    Parameters:
    filters (dict): Dictionary containing filter criteria
    
    Returns:
    pandas.DataFrame: DataFrame containing location data
    """
    try:
        # Show loading progress bar
        with st.spinner("Loading property data..."):
            # Add progress bar
            progress_bar = st.progress(0)
            
            # Try to get data from database or website
            try:
                # Simulate progress updates
                for percent_complete in range(0, 101, 20):
                    progress_bar.progress(percent_complete)
                    time.sleep(0.1)  # Small delay for user experience
                
                data = get_location_data(filters)
                
                # If data is empty, generate sample data
                if data is None or data.empty:
                    st.warning("No data available from database or website. Using sample data for demonstration.")
                    data = generate_sample_data(50)
                    
            except Exception as e:
                st.error(f"Error retrieving data: {str(e)}")
                logger.error(f"Data retrieval error: {traceback.format_exc()}")
                st.info("Using sample data for demonstration.")
                data = generate_sample_data(50)
            
            # Complete progress
            progress_bar.progress(100)
            time.sleep(0.5)  # Small delay before removing progress bar
            progress_bar.empty()
        
        return data
    
    except Exception as e:
        st.error(f"Unexpected error during data loading: {str(e)}")
        logger.error(f"Unexpected error: {traceback.format_exc()}")
        return pd.DataFrame()

def apply_custom_css():
    """Apply custom CSS to improve the app's appearance"""
    st.markdown("""
    <style>
        .main .block-container {
            padding-top: 2rem;
            padding-bottom: 2rem;
        }
        h1, h2, h3 {
            color: #1E3A8A;
        }
        .stExpander {
            border: none;
            box-shadow: 0 1px 3px rgba(0,0,0,0.12), 0 1px 2px rgba(0,0,0,0.24);
            margin-bottom: 1rem;
        }
        .reportview-container .sidebar-content {
            padding-top: 1rem;
        }
        .stSidebar .sidebar-content {
            background-color: #f8f9fa;
        }
        /* Improve filter styles */
        .stSelectbox label, .stDateInput label {
            font-weight: bold;
            color: #444;
        }
        /* Add more breathing room */
        .stTabs [data-baseweb="tab-panel"] {
            padding-top: 1rem;
        }
    </style>
    """, unsafe_allow_html=True)

def display_map(data):
    """Display interactive map with property markers"""
    if not data.empty:
        # Center map on average coordinates
        center_lat = data['latitude'].mean()
        center_lon = data['longitude'].mean()
        
        m = folium.Map(location=[center_lat, center_lon], zoom_start=10)
        
        # Add markers with different colors based on property type
        property_types = data['property_type'].unique()
        color_map = {
            'Office': 'blue',
            'Retail': 'red',
            'Industrial': 'purple',
            'Residential': 'green',
            # Default for any other types
            'default': 'gray'
        }
        
        # Create a legend
        legend_html = '''
        <div style="position: fixed; bottom: 50px; left: 50px; z-index: 1000; background-color: white; 
                   padding: 10px; border: 2px solid grey; border-radius: 5px">
        <h4>Property Types</h4>
        '''
        
        for prop_type in property_types:
            color = color_map.get(prop_type, color_map['default'])
            legend_html += f'''
            <div>
                <span style="background-color: {color}; width: 15px; height: 15px; 
                             border-radius: 50%; display: inline-block"></span>
                {prop_type}
            </div>
            '''
        
        legend_html += '</div>'
        
        # Add markers for each property
        for idx, row in data.iterrows():
            color = color_map.get(row['property_type'], color_map['default'])
            
            popup_text = f"""
            <b>{row['property_name']}</b><br>
            Type: {row['property_type']}<br>
            Address: {row['address']}<br>
            City: {row['city']}<br>
            Size: {row['size']} sqm<br>
            """
            
            # Add year built and renovation if available
            if row['year_built']:
                popup_text += f"Year built: {row['year_built']}<br>"
            if row['last_renovation']:
                popup_text += f"Last renovation: {row['last_renovation']}<br>"
            
            popup_text += f"Data source: {row['data_source']}"
            
            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup=folium.Popup(popup_text, max_width=300),
                tooltip=row['property_name'],
                icon=folium.Icon(color=color, icon='building', prefix='fa')
            ).add_to(m)
        
        # Add marker clusters if there are many points
        if len(data) > 20:
            from folium.plugins import MarkerCluster
            marker_cluster = MarkerCluster().add_to(m)
            
            for idx, row in data.iterrows():
                color = color_map.get(row['property_type'], color_map['default'])
                
                folium.Marker(
                    location=[row['latitude'], row['longitude']],
                    popup=row['property_name'],
                    icon=folium.Icon(color=color, icon='building', prefix='fa')
                ).add_to(marker_cluster)
        
        # Add the legend
        m.get_root().html.add_child(folium.Element(legend_html))
        
        # Display map
        if FOLIUM_AVAILABLE:
            folium_static(m, width=1200, height=600)
        else:
            # Fallback if streamlit_folium is not available
            st.warning("Map view requires streamlit-folium package. Install with: `pip install streamlit-folium`")
            # Display a simple table of coordinates instead
            st.dataframe(data[['property_name', 'address', 'city', 'latitude', 'longitude']])
    else:
        st.info("No properties found with the selected filters.")

def display_data_table(data):
    """Display data table with search and export functionality"""
    if not data.empty:
        # Add search functionality
        search_term = st.text_input("Search in data table:", "")
        
        if search_term:
            # Filter data based on search term
            filtered_data = data[
                data.astype(str).apply(
                    lambda row: row.str.contains(search_term, case=False).any(), 
                    axis=1
                )
            ]
            st.write(f"Found {len(filtered_data)} matching records")
            display_data = filtered_data
        else:
            display_data = data
        
        # Add number of records indicator
        st.write(f"Displaying {len(display_data)} properties")
        
        # Display data table with pagination
        st.dataframe(display_data, use_container_width=True)
        
        # Export options
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("Export to Excel"):
                export_path = export_to_excel(display_data)
                st.success(f"Data exported successfully")
                
                # Provide download link
                with open(export_path, "rb") as file:
                    st.download_button(
                        label="Download Excel File",
                        data=file,
                        file_name="newsec_location_data.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
        
        with col2:
            if st.button("Export Summary Report"):
                export_path = export_summary_report(display_data)
                st.success(f"Summary report exported successfully")
                
                # Provide download link
                with open(export_path, "rb") as file:
                    st.download_button(
                        label="Download Summary Report",
                        data=file,
                        file_name="newsec_summary_report.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
    else:
        st.info("No properties found with the selected filters.")

def display_analytics(data):
    """Display analytics charts and insights"""
    if not data.empty:
        # Property type distribution
        st.subheader("Property Type Distribution")
        fig1 = px.pie(data, names='property_type', title='Distribution by Property Type')
        st.plotly_chart(fig1, use_container_width=True)
        
        # City distribution
        st.subheader("City Distribution")
        city_counts = data['city'].value_counts().reset_index()
        city_counts.columns = ['City', 'Count']
        fig2 = px.bar(city_counts, x='City', y='Count', title='Properties by City')
        st.plotly_chart(fig2, use_container_width=True)
        
        # Size distribution
        st.subheader("Property Size Distribution")
        fig3 = px.histogram(data, x='size', title='Property Size Distribution (sqm)',
                           labels={'size': 'Size (sqm)', 'count': 'Number of Properties'})
        fig3.update_layout(bargap=0.1)
        st.plotly_chart(fig3, use_container_width=True)
        
        # Data sources
        if 'data_source' in data.columns:
            st.subheader("Data Sources")
            source_counts = data['data_source'].value_counts().reset_index()
            source_counts.columns = ['Source', 'Count']
            fig4 = px.pie(source_counts, names='Source', values='Count', title='Data Source Distribution')
            st.plotly_chart(fig4, use_container_width=True)
        
        # Add key statistics/insights
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Properties", len(data))
        
        with col2:
            total_size = f"{int(data['size'].sum()):,} sqm"
            st.metric("Total Area", total_size)
        
        with col3:
            avg_size = f"{int(data['size'].mean()):,} sqm"
            st.metric("Average Size", avg_size)
        
        with col4:
            property_types_count = len(data['property_type'].unique())
            st.metric("Property Types", property_types_count)
    else:
        st.info("No properties found with the selected filters.")

def main():
    """Main application function"""
    try:
        # Apply custom CSS for better appearance
        apply_custom_css()
        
        # Sidebar filters
        st.sidebar.header("Filters")

        # Property type filter
        property_types = ["All", "Office", "Retail", "Industrial", "Residential"]
        selected_property_type = st.sidebar.selectbox("Property Type", property_types)

        # City filter
        cities = ["All", "Stockholm", "Gothenburg", "Malmö", "Copenhagen", "Helsinki", "Oslo"]
        selected_city = st.sidebar.selectbox("City", cities)

        # Date range filter
        date_range = st.sidebar.date_input(
            "Date Range", 
            [pd.to_datetime("2023-01-01"), pd.to_datetime("today")]
        )

        # Additional filters
        with st.sidebar.expander("Advanced Filters"):
            min_size = st.number_input("Minimum Size (sqm)", min_value=0, value=0)
            max_size = st.number_input("Maximum Size (sqm)", min_value=0, value=100000)
            
            if 'year_built' in locals():
                min_year = st.number_input("Built After Year", min_value=1900, max_value=datetime.now().year, value=1900)
        
        # Reset filters button
        if st.sidebar.button("Reset Filters"):
            st.experimental_rerun()
        
        # Display data refresh timestamp
        st.sidebar.markdown("---")
        st.sidebar.text(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Apply filters and get data
        filters = {
            "property_type": None if selected_property_type == "All" else selected_property_type,
            "city": None if selected_city == "All" else selected_city,
            "start_date": date_range[0],
            "end_date": date_range[1] if len(date_range) > 1 else date_range[0],
            "min_size": min_size,
            "max_size": max_size
        }
        
        # Load data with progress indicator
        data = load_data(filters)
        
        # Apply additional filters that couldn't be applied in the database/API query
        if not data.empty:
            # Apply size filter
            if min_size > 0 or max_size < 100000:
                data = data[(data['size'] >= min_size) & (data['size'] <= max_size)]
            
            # Check if we have any data after filtering
            if data.empty:
                st.warning("No properties match your filter criteria. Try adjusting the filters.")
                return
        
        # Main content area with tabs
        tab1, tab2, tab3 = st.tabs(["Map View", "Data Table", "Analytics"])

        with tab1:
            st.header("Property Locations")
            display_map(data)

        with tab2:
            st.header("Property Data")
            display_data_table(data)

        with tab3:
            st.header("Analytics")
            display_analytics(data)
            
    except Exception as e:
        st.error(f"An unexpected error occurred: {str(e)}")
        logger.error(f"Unexpected error in main application: {traceback.format_exc()}")
        st.info("Try refreshing the page or contact support if the problem persists.")

if __name__ == "__main__":
    main()
