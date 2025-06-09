"""
Google Earth Engine client for rainfall data extraction
"""

import ee
import json
import os
from google.oauth2 import service_account
from datetime import datetime, timedelta
from config import Config

def initialize_earth_engine():
    """Initialize Google Earth Engine authentication"""
    try:
        # Try service account file first
        if Config.GOOGLE_APPLICATION_CREDENTIALS and os.path.exists(Config.GOOGLE_APPLICATION_CREDENTIALS):
            credentials = service_account.Credentials.from_service_account_file(
                Config.GOOGLE_APPLICATION_CREDENTIALS,
                scopes=['https://www.googleapis.com/auth/earthengine.readonly']
            )
            ee.Initialize(credentials=credentials, opt_url='https://earthengine-highvolume.googleapis.com')
            return True
        
        # Try JSON credentials from environment
        elif Config.GEE_SERVICE_ACCOUNT_CREDENTIALS_JSON:
            creds_json = json.loads(Config.GEE_SERVICE_ACCOUNT_CREDENTIALS_JSON)
            credentials = service_account.Credentials.from_service_account_info(
                creds_json,
                scopes=['https://www.googleapis.com/auth/earthengine.readonly']
            )
            ee.Initialize(credentials=credentials, opt_url='https://earthengine-highvolume.googleapis.com')
            return True
        
        else:
            raise EnvironmentError("No valid GEE credentials found")
            
    except Exception as e:
        print(f"Failed to initialize Earth Engine: {e}")
        raise

class RainfallExtractor:
    """Handles rainfall data extraction from CHIRPS via Google Earth Engine"""
    
    def __init__(self):
        self.collection_id = Config.CHIRPS_COLLECTION_ID
        self.scale = Config.CHIRPS_SCALE
    
    def get_daily_rainfall(self, geometry, start_date, end_date):
        """
        Extract daily rainfall data for a geometry and date range
        
        Args:
            geometry: ee.Geometry object
            start_date: str in 'YYYY-MM-DD' format
            end_date: str in 'YYYY-MM-DD' format
            
        Returns:
            list: Daily rainfall values
        """
        try:
            collection = ee.ImageCollection(self.collection_id) \
                .filterBounds(geometry) \
                .filterDate(start_date, end_date) \
                .select('precipitation')
            
            # Get daily images
            def extract_daily_value(image):
                value = image.reduceRegion(
                    reducer=ee.Reducer.mean(),
                    geometry=geometry,
                    scale=self.scale,
                    maxPixels=1e13
                ).get('precipitation')
                
                return ee.Feature(None, {
                    'date': image.date().format('YYYY-MM-dd'),
                    'rainfall': value
                })
            
            features = collection.map(extract_daily_value)
            feature_collection = ee.FeatureCollection(features)
            
            # Get the data
            data = feature_collection.getInfo()
            
            # Extract values
            daily_data = []
            for feature in data['features']:
                props = feature['properties']
                daily_data.append({
                    'date': props['date'],
                    'rainfall': props['rainfall'] if props['rainfall'] is not None else 0
                })
            
            return sorted(daily_data, key=lambda x: x['date'])
            
        except Exception as e:
            print(f"Error extracting daily rainfall: {e}")
            return []
    
    def get_period_rainfall(self, geometry, start_date, end_date):
        """
        Get total rainfall for a period
        
        Args:
            geometry: ee.Geometry object
            start_date: str in 'YYYY-MM-DD' format  
            end_date: str in 'YYYY-MM-DD' format
            
        Returns:
            float: Total rainfall in mm
        """
        try:
            collection = ee.ImageCollection(self.collection_id) \
                .filterBounds(geometry) \
                .filterDate(start_date, end_date) \
                .select('precipitation')
            
            total_rainfall = collection.sum().reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=geometry,
                scale=self.scale,
                maxPixels=1e13
            ).get('precipitation')
            
            result = total_rainfall.getInfo()
            return result if result is not None else 0
            
        except Exception as e:
            print(f"Error extracting period rainfall: {e}")
            return 0
    
    def detect_planting_date(self, geometry, year, crop_type="maize"):
        """
        Detect planting date based on rainfall patterns
        
        Args:
            geometry: ee.Geometry object
            year: int
            crop_type: str
            
        Returns:
            tuple: (planting_date_str, description)
        """
        try:
            # Define search windows
            early_start = f"{year}-11-01"
            early_end = f"{year}-12-15"
            late_start = f"{year}-12-01" 
            late_end = f"{year+1}-01-31"
            
            # Check early window first
            planting_date = self._check_planting_window(geometry, early_start, early_end, "early")
            if planting_date:
                return planting_date
            
            # Check late window
            planting_date = self._check_planting_window(geometry, late_start, late_end, "late")
            if planting_date:
                return planting_date
            
            # Fallback to default date
            from core.crops import get_crop_config
            crop_config = get_crop_config(crop_type)
            default_date = f"{year}-{crop_config.get('default_planting_date', '11-15')}"
            
            return default_date, f"Default planting date (no adequate rainfall trigger detected)"
            
        except Exception as e:
            print(f"Error detecting planting date: {e}")
            default_date = f"{year}-11-15"
            return default_date, f"Default planting date (error: {str(e)})"
    
    def _check_planting_window(self, geometry, start_date, end_date, window_name):
        """Check for planting triggers in a specific window"""
        try:
            # Get daily rainfall for the window
            daily_data = self.get_daily_rainfall(geometry, start_date, end_date)
            
            if len(daily_data) < 7:
                return None
            
            # Check 7-day rolling windows
            for i in range(len(daily_data) - 6):
                seven_day_total = sum(day['rainfall'] for day in daily_data[i:i+7])
                rain_days = sum(1 for day in daily_data[i:i+7] if day['rainfall'] >= 3)
                
                if seven_day_total >= Config.PLANTING_TRIGGER_RAINFALL and rain_days >= Config.PLANTING_MIN_RAIN_DAYS:
                    planting_date = daily_data[i+1]['date']  # Plant day after first day of 7-day period
                    description = f"Detected in {window_name} window: {seven_day_total:.1f}mm over 7 days with {rain_days} rain days"
                    return planting_date, description
            
            return None
            
        except Exception as e:
            print(f"Error checking planting window {window_name}: {e}")
            return None
    
    def get_rolling_window_analysis(self, geometry, start_date, phase_days, window_days=10):
        """
        Perform rolling window analysis for a crop phase
        
        Args:
            geometry: ee.Geometry object
            start_date: str - phase start date
            phase_days: int - length of phase in days
            window_days: int - rolling window size
            
        Returns:
            list: Rolling window rainfall totals
        """
        try:
            # Calculate phase end date
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = start_dt + timedelta(days=phase_days)
            end_date = end_dt.strftime('%Y-%m-%d')
            
            # Get daily rainfall
            daily_data = self.get_daily_rainfall(geometry, start_date, end_date)
            
            if len(daily_data) < window_days:
                return []
            
            # Calculate rolling windows
            rolling_totals = []
            for i in range(len(daily_data) - window_days + 1):
                window_total = sum(day['rainfall'] for day in daily_data[i:i+window_days])
                window_start = daily_data[i]['date']
                window_end = daily_data[i+window_days-1]['date']
                
                rolling_totals.append({
                    'period': i + 1,
                    'start_date': window_start,
                    'end_date': window_end,
                    'rainfall_mm': round(window_total, 1)
                })
            
            return rolling_totals
            
        except Exception as e:
            print(f"Error in rolling window analysis: {e}")
            return []
