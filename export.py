import pandas as pd
import os
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def export_to_excel(data, filename=None, include_timestamp=True, export_dir="exports"):
    """
    Export DataFrame to Excel file
    
    Parameters:
    data (pandas.DataFrame): DataFrame to export
    filename (str): Base filename (without extension)
    include_timestamp (bool): Whether to include timestamp in filename
    export_dir (str): Directory to save exports
    
    Returns:
    str: Path to the exported Excel file
    """
    try:
        # Create exports directory if it doesn't exist
        if not os.path.exists(export_dir):
            os.makedirs(export_dir)
            logger.info(f"Created exports directory: {export_dir}")
        
        # Generate filename
        if filename is None:
            filename = "newsec_location_data"
        
        if include_timestamp:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            full_filename = f"{filename}_{timestamp}.xlsx"
        else:
            full_filename = f"{filename}.xlsx"
        
        file_path = os.path.join(export_dir, full_filename)
        
        # Create Excel writer
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            # Write data to Excel
            data.to_excel(writer, sheet_name='Property Data', index=False)
            
            # Access workbook and worksheet
            workbook = writer.book
            worksheet = writer.sheets['Property Data']
            
            # Format headers
            header_format = {
                'bold': True,
                'font_size': 12,
                'bg_color': '#4F81BD',
                'font_color': '#FFFFFF',
                'border': 1
            }
            
            # Apply formatting
            for col_num, column in enumerate(data.columns, 1):
                # Set column width based on content
                max_length = max(
                    data[column].astype(str).map(len).max(),
                    len(str(column))
                ) + 2
                
                # Limit column width to a reasonable maximum
                col_width = min(max_length, 30)
                
                # Set column width
                col_letter = get_column_letter(col_num)
                worksheet.column_dimensions[col_letter].width = col_width
            
            # Add filters to header row
            worksheet.auto_filter.ref = worksheet.dimensions
        
        logger.info(f"Data successfully exported to {file_path}")
        return file_path
    
    except Exception as e:
        logger.error(f"Error exporting data to Excel: {str(e)}")
        return None

def get_column_letter(col_num):
    """
    Convert column number to Excel column letter (A, B, C, ..., AA, AB, etc.)
    
    Parameters:
    col_num (int): Column number (1-based)
    
    Returns:
    str: Excel column letter
    """
    col_letter = ''
    while col_num > 0:
        col_num, remainder = divmod(col_num - 1, 26)
        col_letter = chr(65 + remainder) + col_letter
    return col_letter

def export_filtered_data(data, filters, filename=None):
    """
    Export filtered data to Excel with filter information
    
    Parameters:
    data (pandas.DataFrame): DataFrame to export
    filters (dict): Dictionary containing filter criteria
    filename (str): Base filename (without extension)
    
    Returns:
    str: Path to the exported Excel file
    """
    try:
        # Create a copy of the data
        export_data = data.copy()
        
        # Generate filename based on filters
        if filename is None:
            parts = ["newsec_location_data"]
            
            if filters:
                if filters.get('property_type') and filters['property_type'] != "All":
                    parts.append(filters['property_type'].lower())
                
                if filters.get('city') and filters['city'] != "All":
                    parts.append(filters['city'].lower())
            
            filename = "_".join(parts)
        
        # Export to Excel
        file_path = export_to_excel(export_data, filename)
        
        return file_path
    
    except Exception as e:
        logger.error(f"Error exporting filtered data: {str(e)}")
        return None

def export_summary_report(data, filename="newsec_summary_report"):
    """
    Export a summary report with aggregated data
    
    Parameters:
    data (pandas.DataFrame): DataFrame to export
    filename (str): Base filename (without extension)
    
    Returns:
    str: Path to the exported Excel file
    """
    try:
        # Create exports directory if it doesn't exist
        export_dir = "exports"
        if not os.path.exists(export_dir):
            os.makedirs(export_dir)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        full_filename = f"{filename}_{timestamp}.xlsx"
        file_path = os.path.join(export_dir, full_filename)
        
        # Create Excel writer
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            # Write main data
            data.to_excel(writer, sheet_name='Property Data', index=False)
            
            # Create summary by property type
            if 'property_type' in data.columns:
                property_type_summary = data.groupby('property_type').agg({
                    'property_id': 'count',
                    'size': ['sum', 'mean', 'min', 'max']
                })
                property_type_summary.columns = ['Count', 'Total Size (sqm)', 'Avg Size (sqm)', 'Min Size (sqm)', 'Max Size (sqm)']
                property_type_summary.reset_index().to_excel(writer, sheet_name='By Property Type', index=False)
            
            # Create summary by city
            if 'city' in data.columns:
                city_summary = data.groupby('city').agg({
                    'property_id': 'count',
                    'size': ['sum', 'mean']
                })
                city_summary.columns = ['Count', 'Total Size (sqm)', 'Avg Size (sqm)']
                city_summary.reset_index().to_excel(writer, sheet_name='By City', index=False)
            
            # Create summary by data source
            if 'data_source' in data.columns:
                source_summary = data.groupby('data_source').size().reset_index(name='Count')
                source_summary.to_excel(writer, sheet_name='By Data Source', index=False)
        
        logger.info(f"Summary report successfully exported to {file_path}")
        return file_path
    
    except Exception as e:
        logger.error(f"Error exporting summary report: {str(e)}")
        return None
