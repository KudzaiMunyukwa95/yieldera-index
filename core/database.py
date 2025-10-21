"""
Database management - OPTIMIZED with Connection Pooling
Fixed for high-latency connections (Singapore -> Amsterdam)
"""

import mysql.connector
from mysql.connector import pooling, Error
import json
from datetime import datetime, date
from typing import Dict, List, Optional, Any
from contextlib import contextmanager
from config import Config
import decimal
import traceback

class DatabaseManager:
    """Manages database connections with connection pooling for performance"""
    
    # Class-level connection pool (shared across all instances)
    _pool = None
    _pool_initialized = False
    
    def __init__(self):
        """Initialize connection pool on first instantiation"""
        if not DatabaseManager._pool_initialized:
            self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize MySQL connection pool for connection reuse"""
        try:
            DatabaseManager._pool = pooling.MySQLConnectionPool(
                pool_name="yieldera_pool",
                pool_size=10,  # Maximum 10 concurrent connections
                pool_reset_session=True,
                host=Config.DB_HOST,
                port=Config.DB_PORT,
                database=Config.DB_NAME,
                user=Config.DB_USER,
                password=Config.DB_PASSWORD,
                autocommit=True,
                charset='utf8mb4',
                use_unicode=True,
                connect_timeout=60,      # Increased for high latency
                read_timeout=120,        # Added for long-running queries
                write_timeout=120,       # Added for long-running queries
                sql_mode='TRADITIONAL'
            )
            DatabaseManager._pool_initialized = True
            print("✅ Database connection pool initialized successfully")
            print(f"   Pool size: 10 connections")
            print(f"   Host: {Config.DB_HOST}")
            print(f"   Database: {Config.DB_NAME}")
            print(f"   Timeouts: connect=60s, read=120s, write=120s")
        except Error as e:
            print(f"❌ Failed to initialize connection pool: {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections - now uses connection pooling
        Connections are reused instead of creating new TCP connections
        """
        connection = None
        try:
            # Get connection from pool (fast - reuses existing connection)
            connection = DatabaseManager._pool.get_connection()
            yield connection
        except Error as e:
            print(f"Database error: {e}")
            raise
        finally:
            if connection and connection.is_connected():
                # Close returns connection to pool, doesn't close TCP connection
                connection.close()
    
    def test_connection(self) -> bool:
        """Test database connectivity using pooled connection"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                cursor.close()
                return result[0] == 1
        except Exception as e:
            print(f"Database connection test failed: {e}")
            return False
    
    def get_pool_stats(self) -> Dict[str, Any]:
        """Get connection pool statistics for monitoring"""
        if DatabaseManager._pool:
            return {
                "pool_name": DatabaseManager._pool.pool_name,
                "pool_size": DatabaseManager._pool.pool_size,
                "pool_initialized": DatabaseManager._pool_initialized
            }
        return {"pool_initialized": False}

def clean_database_value(value):
    """Clean database values for JSON serialization - ENHANCED"""
    if value is None:
        return None
    
    # Handle bytearray (common in some MySQL configurations)
    if isinstance(value, bytearray):
        try:
            # Try to decode as string first
            return value.decode('utf-8')
        except UnicodeDecodeError:
            # If not valid UTF-8, convert to hex string for geometry data
            return f"GEOMETRY_DATA_{len(value)}_bytes"
    
    # Handle bytes
    if isinstance(value, bytes):
        try:
            return value.decode('utf-8')
        except UnicodeDecodeError:
            return f"BINARY_DATA_{len(value)}_bytes"
    
    # Handle decimal.Decimal
    if isinstance(value, decimal.Decimal):
        return float(value)
    
    # Handle datetime/date objects
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    
    # Return as-is for other types
    return value

def safe_numeric_conversion(value, field_name="unknown"):
    """Safely convert database value to numeric type"""
    if value is None:
        return None
    
    # Clean the value first
    cleaned_value = clean_database_value(value)
    
    if cleaned_value is None:
        return None
    
    # If it's already a number, return it
    if isinstance(cleaned_value, (int, float)):
        return float(cleaned_value)
    
    # Try to convert string to float
    if isinstance(cleaned_value, str):
        cleaned_str = cleaned_value.strip()
        if cleaned_str == '' or cleaned_str.lower() in ['null', 'none']:
            return None
        
        # Skip geometry/binary data strings
        if cleaned_str.startswith(('GEOMETRY_DATA_', 'BINARY_DATA_')):
            return None
        
        try:
            return float(cleaned_str)
        except ValueError:
            print(f"Warning: Could not convert {field_name} to float: '{cleaned_str}'")
            return None
    
    # Try to convert other types
    try:
        return float(cleaned_value)
    except (ValueError, TypeError):
        print(f"Warning: Could not convert {field_name} to float: {cleaned_value} (type: {type(cleaned_value)})")
        return None

class FieldsRepository:
    """Repository for field-related database operations with enhanced data cleaning"""
    
    def __init__(self):
        self.db = DatabaseManager()
    
    def get_field_by_id(self, field_id: int) -> Optional[Dict[str, Any]]:
        """
        Get field data by ID with comprehensive data cleaning
        OPTIMIZED: Uses connection pooling for faster queries
        """
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)
                
                query = """
                SELECT 
                    id, farm_id, name, farmer_name, farmer_phone,
                    area_ha, location, created_at, crop, variety,
                    planting_date, irrigated, latitude, longitude,
                    owner_entity_id
                FROM fields 
                WHERE id = %s
                """
                
                cursor.execute(query, (field_id,))
                raw_field_data = cursor.fetchone()
                cursor.close()
                
                if not raw_field_data:
                    return None
                
                # Clean all database values
                field_data = {}
                for key, value in raw_field_data.items():
                    cleaned = clean_database_value(value)
                    field_data[key] = cleaned
                    
                    # Special handling for geometry fields
                    if key == 'location' and isinstance(value, bytearray):
                        field_data[key] = "GEOMETRY_POLYGON"  # Simplified representation
                
                # Validate coordinates with safe conversion
                lat_raw = field_data.get('latitude')
                lng_raw = field_data.get('longitude')
                
                # Convert coordinates safely
                latitude = safe_numeric_conversion(lat_raw, 'latitude')
                longitude = safe_numeric_conversion(lng_raw, 'longitude')
                
                if latitude is None or longitude is None:
                    print(f"Warning: Field {field_id} has NULL/invalid coordinates")
                    return None
                
                # Validate coordinate ranges
                if not (-90 <= latitude <= 90):
                    print(f"Warning: Field {field_id} has invalid latitude: {latitude}")
                    return None
                
                if not (-180 <= longitude <= 180):
                    print(f"Warning: Field {field_id} has invalid longitude: {longitude}")
                    return None
                
                # Update field_data with converted coordinates
                field_data['latitude'] = latitude
                field_data['longitude'] = longitude
                
                # Handle area_ha safely
                area_raw = field_data.get('area_ha')
                area_ha = safe_numeric_conversion(area_raw, 'area_ha')
                
                if area_ha is not None and area_ha <= 0:
                    print(f"Warning: Field {field_id} has non-positive area: {area_ha}")
                    area_ha = None
                
                field_data['area_ha'] = area_ha
                
                return field_data
                
        except Exception as e:
            print(f"Error fetching field {field_id}: {e}")
            print(f"Error traceback: {traceback.format_exc()}")
            return None
    
    def get_fields_batch(self, field_ids: List[int]) -> Dict[int, Dict[str, Any]]:
        """
        NEW: Fetch multiple fields in a single query for performance
        OPTIMIZED: Reduces database round-trips from N to 1
        """
        if not field_ids:
            return {}
        
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)
                
                # Create placeholders for IN clause
                placeholders = ','.join(['%s'] * len(field_ids))
                
                query = f"""
                SELECT 
                    id, farm_id, name, farmer_name, farmer_phone,
                    area_ha, crop, variety, planting_date, irrigated,
                    latitude, longitude, owner_entity_id, created_at
                FROM fields 
                WHERE id IN ({placeholders})
                """
                
                cursor.execute(query, tuple(field_ids))
                raw_fields = cursor.fetchall()
                cursor.close()
                
                # Process and clean all fields
                result = {}
                for raw_field in raw_fields:
                    field_id = raw_field['id']
                    
                    # Clean all values
                    field_data = {}
                    for key, value in raw_field.items():
                        field_data[key] = clean_database_value(value)
                    
                    # Validate coordinates
                    latitude = safe_numeric_conversion(field_data.get('latitude'), 'latitude')
                    longitude = safe_numeric_conversion(field_data.get('longitude'), 'longitude')
                    
                    if latitude is not None and longitude is not None:
                        if -90 <= latitude <= 90 and -180 <= longitude <= 180:
                            field_data['latitude'] = latitude
                            field_data['longitude'] = longitude
                            
                            # Handle area_ha
                            area_ha = safe_numeric_conversion(field_data.get('area_ha'), 'area_ha')
                            field_data['area_ha'] = area_ha
                            
                            result[field_id] = field_data
                
                print(f"✅ Batch fetched {len(result)}/{len(field_ids)} valid fields")
                return result
                
        except Exception as e:
            print(f"Error batch fetching fields: {e}")
            return {}
    
    def get_fields_by_owner(self, owner_entity_id: int, limit: int = 100) -> List[Dict[str, Any]]:
        """Get fields by owner with data cleaning - OPTIMIZED with pooling"""
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)
                
                query = """
                SELECT 
                    id, farm_id, name, farmer_name, area_ha,
                    crop, latitude, longitude, created_at
                FROM fields 
                WHERE owner_entity_id = %s
                ORDER BY created_at DESC
                LIMIT %s
                """
                
                cursor.execute(query, (owner_entity_id, limit))
                raw_fields = cursor.fetchall()
                cursor.close()
                
                # Clean and validate each field
                valid_fields = []
                for raw_field in raw_fields:
                    try:
                        # Clean all values
                        field = {}
                        for key, value in raw_field.items():
                            field[key] = clean_database_value(value)
                        
                        # Validate coordinates
                        latitude = safe_numeric_conversion(field.get('latitude'), 'latitude')
                        longitude = safe_numeric_conversion(field.get('longitude'), 'longitude')
                        
                        if latitude is not None and longitude is not None:
                            if -90 <= latitude <= 90 and -180 <= longitude <= 180:
                                field['latitude'] = latitude
                                field['longitude'] = longitude
                                
                                # Handle area_ha
                                area_ha = safe_numeric_conversion(field.get('area_ha'), 'area_ha')
                                field['area_ha'] = area_ha
                                
                                valid_fields.append(field)
                    except Exception as e:
                        print(f"Error processing field {raw_field.get('id', 'unknown')}: {e}")
                        continue
                
                return valid_fields
                
        except Exception as e:
            print(f"Error fetching fields for owner {owner_entity_id}: {e}")
            return []
    
    def search_fields(self, filters: Dict[str, Any], limit: int = 100) -> List[Dict[str, Any]]:
        """Search fields with filters and data cleaning - OPTIMIZED"""
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)
                
                # Build dynamic query
                where_clauses = []
                params = []
                
                if filters.get('crop'):
                    where_clauses.append("crop = %s")
                    params.append(filters['crop'])
                
                if filters.get('min_area'):
                    where_clauses.append("area_ha >= %s")
                    params.append(filters['min_area'])
                
                if filters.get('max_area'):
                    where_clauses.append("area_ha <= %s")
                    params.append(filters['max_area'])
                
                if filters.get('owner_entity_id'):
                    where_clauses.append("owner_entity_id = %s")
                    params.append(filters['owner_entity_id'])
                
                # Base query - exclude location field to avoid bytearray issues
                query = """
                SELECT 
                    id, name, farmer_name, area_ha, crop,
                    latitude, longitude, created_at
                FROM fields
                """
                
                if where_clauses:
                    query += " WHERE " + " AND ".join(where_clauses)
                
                query += " ORDER BY created_at DESC LIMIT %s"
                params.append(limit)
                
                cursor.execute(query, params)
                raw_fields = cursor.fetchall()
                cursor.close()
                
                # Clean and validate fields
                valid_fields = []
                for raw_field in raw_fields:
                    try:
                        # Clean all values
                        field = {}
                        for key, value in raw_field.items():
                            field[key] = clean_database_value(value)
                        
                        # Validate coordinates
                        latitude = safe_numeric_conversion(field.get('latitude'), 'latitude')
                        longitude = safe_numeric_conversion(field.get('longitude'), 'longitude')
                        
                        if latitude is not None and longitude is not None:
                            if -90 <= latitude <= 90 and -180 <= longitude <= 180:
                                field['latitude'] = latitude
                                field['longitude'] = longitude
                                
                                # Handle area_ha
                                area_ha = safe_numeric_conversion(field.get('area_ha'), 'area_ha')
                                field['area_ha'] = area_ha
                                
                                valid_fields.append(field)
                    except Exception as e:
                        print(f"Error processing field {raw_field.get('id', 'unknown')}: {e}")
                        continue
                
                return valid_fields
                
        except Exception as e:
            print(f"Error searching fields: {e}")
            return []
    
    def create_field(self, field_data: Dict[str, Any]) -> Optional[int]:
        """Create a new field with validation - OPTIMIZED"""
        try:
            # Validate coordinates before insertion
            if 'latitude' in field_data and 'longitude' in field_data:
                latitude = safe_numeric_conversion(field_data['latitude'], 'latitude')
                longitude = safe_numeric_conversion(field_data['longitude'], 'longitude')
                
                if latitude is None or longitude is None:
                    print("Invalid coordinates for new field")
                    return None
                
                if not (-90 <= latitude <= 90) or not (-180 <= longitude <= 180):
                    print(f"Coordinates out of range: lat={latitude}, lng={longitude}")
                    return None
                
                field_data['latitude'] = latitude
                field_data['longitude'] = longitude
            
            # Validate area_ha
            if 'area_ha' in field_data:
                area_ha = safe_numeric_conversion(field_data['area_ha'], 'area_ha')
                if area_ha is not None and area_ha <= 0:
                    area_ha = None
                field_data['area_ha'] = area_ha
            
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                
                query = """
                INSERT INTO fields 
                (farm_id, name, farmer_name, farmer_phone, area_ha, location,
                 crop, variety, planting_date, irrigated, latitude, longitude, owner_entity_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                values = (
                    field_data.get('farm_id'),
                    field_data.get('name'),
                    field_data.get('farmer_name'),
                    field_data.get('farmer_phone'),
                    field_data.get('area_ha'),
                    field_data.get('location'),
                    field_data.get('crop'),
                    field_data.get('variety'),
                    field_data.get('planting_date'),
                    field_data.get('irrigated', 0),
                    field_data.get('latitude'),
                    field_data.get('longitude'),
                    field_data.get('owner_entity_id')
                )
                
                cursor.execute(query, values)
                field_id = cursor.lastrowid
                cursor.close()
                
                return field_id
                
        except Exception as e:
            print(f"Error creating field: {e}")
            return None

class QuotesRepository:
    """Repository for quote-related database operations with data cleaning - OPTIMIZED"""
    
    def __init__(self):
        self.db = DatabaseManager()
    
    def save_quote(self, quote_data: Dict[str, Any]) -> Optional[str]:
        """Save quote to database with enhanced error handling - OPTIMIZED"""
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                
                # Generate quote ID
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                field_suffix = quote_data.get('field_id', 'GEOM')
                quote_id = f"Q{timestamp}_{field_suffix}"
                
                # Ensure quotes table exists
                self._ensure_quotes_table(cursor)
                
                query = """
                INSERT INTO quotes 
                (quote_id, field_id, crop, year, quote_type, sum_insured,
                 gross_premium, premium_rate, payout_index, burning_cost,
                 planting_date, zone, quote_data, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                # Safe extraction with defaults and cleaning
                field_id = quote_data.get('field_id')
                crop = str(quote_data.get('crop', 'unknown'))
                year = int(quote_data.get('year', datetime.now().year))
                quote_type = str(quote_data.get('quote_type', 'unknown'))
                sum_insured = float(quote_data.get('sum_insured', 0))
                gross_premium = float(quote_data.get('gross_premium', 0))
                premium_rate = float(quote_data.get('premium_rate', 0))
                payout_index = float(quote_data.get('total_payout_ratio', 0))
                burning_cost = float(quote_data.get('burning_cost', 0))
                planting_date = quote_data.get('planting_date')
                zone = str(quote_data.get('zone', 'auto_detect'))
                
                # Clean quote_data for JSON serialization
                cleaned_quote_data = {}
                for key, value in quote_data.items():
                    cleaned_quote_data[key] = clean_database_value(value)
                
                values = (
                    quote_id,
                    field_id,
                    crop,
                    year,
                    quote_type,
                    sum_insured,
                    gross_premium,
                    premium_rate,
                    payout_index,
                    burning_cost,
                    planting_date,
                    zone,
                    json.dumps(cleaned_quote_data),
                    datetime.utcnow()
                )
                
                cursor.execute(query, values)
                cursor.close()
                
                return quote_id
                
        except Exception as e:
            print(f"Error saving quote: {e}")
            return None
    
    def save_quotes_batch(self, quotes: List[Dict[str, Any]]) -> List[Optional[str]]:
        """
        NEW: Save multiple quotes in a single transaction
        OPTIMIZED: Reduces database round-trips dramatically
        """
        if not quotes:
            return []
        
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                
                # Ensure table exists
                self._ensure_quotes_table(cursor)
                
                query = """
                INSERT INTO quotes 
                (quote_id, field_id, crop, year, quote_type, sum_insured,
                 gross_premium, premium_rate, payout_index, burning_cost,
                 planting_date, zone, quote_data, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                quote_ids = []
                values_list = []
                
                for quote_data in quotes:
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    field_suffix = quote_data.get('field_id', 'GEOM')
                    quote_id = f"Q{timestamp}_{field_suffix}_{len(quote_ids)}"
                    
                    # Clean quote_data
                    cleaned_quote_data = {}
                    for key, value in quote_data.items():
                        cleaned_quote_data[key] = clean_database_value(value)
                    
                    values = (
                        quote_id,
                        quote_data.get('field_id'),
                        str(quote_data.get('crop', 'unknown')),
                        int(quote_data.get('year', datetime.now().year)),
                        str(quote_data.get('quote_type', 'unknown')),
                        float(quote_data.get('sum_insured', 0)),
                        float(quote_data.get('gross_premium', 0)),
                        float(quote_data.get('premium_rate', 0)),
                        float(quote_data.get('total_payout_ratio', 0)),
                        float(quote_data.get('burning_cost', 0)),
                        quote_data.get('planting_date'),
                        str(quote_data.get('zone', 'auto_detect')),
                        json.dumps(cleaned_quote_data),
                        datetime.utcnow()
                    )
                    
                    values_list.append(values)
                    quote_ids.append(quote_id)
                
                # Execute batch insert
                cursor.executemany(query, values_list)
                cursor.close()
                
                print(f"✅ Batch saved {len(quote_ids)} quotes")
                return quote_ids
                
        except Exception as e:
            print(f"Error batch saving quotes: {e}")
            return [None] * len(quotes)
    
    def _ensure_quotes_table(self, cursor):
        """Ensure quotes table exists"""
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS quotes (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    quote_id VARCHAR(100) UNIQUE NOT NULL,
                    field_id INT NULL,
                    crop VARCHAR(50) NOT NULL,
                    year INT NOT NULL,
                    quote_type ENUM('historical', 'prospective', 'current') NOT NULL,
                    sum_insured DECIMAL(12,2) NOT NULL,
                    gross_premium DECIMAL(12,2) NOT NULL,
                    premium_rate DECIMAL(6,4) NOT NULL,
                    payout_index DECIMAL(6,4) NULL,
                    burning_cost DECIMAL(12,2) NULL,
                    planting_date DATE NULL,
                    zone VARCHAR(50) DEFAULT 'auto_detect',
                    quote_data JSON NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    
                    INDEX idx_field_id (field_id),
                    INDEX idx_crop_year (crop, year),
                    INDEX idx_created_at (created_at),
                    INDEX idx_quote_type (quote_type)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
        except Exception as e:
            print(f"Error ensuring quotes table: {e}")
    
    def get_quote_by_id(self, quote_id: str) -> Optional[Dict[str, Any]]:
        """Get quote by ID with data cleaning - OPTIMIZED"""
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)
                
                query = """
                SELECT quote_id, field_id, crop, year, quote_type,
                       sum_insured, gross_premium, premium_rate, payout_index,
                       burning_cost, planting_date, zone, quote_data, created_at
                FROM quotes
                WHERE quote_id = %s
                """
                
                cursor.execute(query, (quote_id,))
                raw_quote = cursor.fetchone()
                cursor.close()
                
                if not raw_quote:
                    return None
                
                # Clean all values
                quote = {}
                for key, value in raw_quote.items():
                    quote[key] = clean_database_value(value)
                
                # Handle JSON data
                if quote.get('quote_data'):
                    try:
                        if isinstance(quote['quote_data'], str):
                            quote['quote_data'] = json.loads(quote['quote_data'])
                    except json.JSONDecodeError:
                        quote['quote_data'] = {}
                
                return quote
                
        except Exception as e:
            print(f"Error fetching quote {quote_id}: {e}")
            return None
    
    def get_quotes_by_field(self, field_id: int, limit: int = 20) -> List[Dict[str, Any]]:
        """Get quotes for a field with data cleaning - OPTIMIZED"""
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)
                
                query = """
                SELECT quote_id, crop, year, quote_type, sum_insured,
                       gross_premium, premium_rate, payout_index, created_at
                FROM quotes
                WHERE field_id = %s
                ORDER BY created_at DESC
                LIMIT %s
                """
                
                cursor.execute(query, (field_id, limit))
                raw_quotes = cursor.fetchall()
                cursor.close()
                
                # Clean all quotes
                quotes = []
                for raw_quote in raw_quotes:
                    quote = {}
                    for key, value in raw_quote.items():
                        quote[key] = clean_database_value(value)
                    quotes.append(quote)
                
                return quotes
                
        except Exception as e:
            print(f"Error fetching quotes for field {field_id}: {e}")
            return []

def init_database_tables():
    """Initialize database tables if they don't exist"""
    db = DatabaseManager()
    
    try:
        with db.get_connection() as conn:
            cursor = conn.cursor()
            
            # Create quotes table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS quotes (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    quote_id VARCHAR(100) UNIQUE NOT NULL,
                    field_id INT NULL,
                    crop VARCHAR(50) NOT NULL,
                    year INT NOT NULL,
                    quote_type ENUM('historical', 'prospective', 'current') NOT NULL,
                    sum_insured DECIMAL(12,2) NOT NULL,
                    gross_premium DECIMAL(12,2) NOT NULL,
                    premium_rate DECIMAL(6,4) NOT NULL,
                    payout_index DECIMAL(6,4) NULL,
                    burning_cost DECIMAL(12,2) NULL,
                    planting_date DATE NULL,
                    zone VARCHAR(50) DEFAULT 'auto_detect',
                    quote_data JSON NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    
                    INDEX idx_field_id (field_id),
                    INDEX idx_crop_year (crop, year),
                    INDEX idx_created_at (created_at),
                    INDEX idx_quote_type (quote_type)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Add indexes to fields table
            try:
                cursor.execute("CREATE INDEX idx_fields_coordinates ON fields(latitude, longitude)")
            except mysql.connector.Error:
                pass
            
            cursor.close()
            print("✅ Database tables initialized successfully")
            
    except Exception as e:
        print(f"❌ Error initializing database tables: {e}")
        raise
