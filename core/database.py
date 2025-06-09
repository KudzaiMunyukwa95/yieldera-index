"""
Database management for Yieldera Index Insurance Engine
"""

import mysql.connector
from mysql.connector import Error
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
from contextlib import contextmanager
from config import Config

class DatabaseManager:
    """Manages database connections and operations"""
    
    def __init__(self):
        self.config = {
            'host': Config.DB_HOST,
            'port': Config.DB_PORT,
            'database': Config.DB_NAME,
            'user': Config.DB_USER,
            'password': Config.DB_PASSWORD,
            'autocommit': True,
            'charset': 'utf8mb4',
            'use_unicode': True
        }
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        connection = None
        try:
            connection = mysql.connector.connect(**self.config)
            yield connection
        except Error as e:
            print(f"Database error: {e}")
            raise
        finally:
            if connection and connection.is_connected():
                connection.close()
    
    def test_connection(self) -> bool:
        """Test database connectivity"""
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

class FieldsRepository:
    """Repository for field-related database operations"""
    
    def __init__(self):
        self.db = DatabaseManager()
    
    def get_field_by_id(self, field_id: int) -> Optional[Dict[str, Any]]:
        """
        Get field data by ID
        
        Args:
            field_id: Field ID
            
        Returns:
            dict: Field data or None if not found
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
                field_data = cursor.fetchone()
                cursor.close()
                
                # Validate coordinates
                if field_data and (field_data.get('latitude') is None or field_data.get('longitude') is None):
                    print(f"Warning: Field {field_id} has invalid coordinates")
                    return None
                
                return field_data
                
        except Exception as e:
            print(f"Error fetching field {field_id}: {e}")
            return None
    
    def get_fields_by_owner(self, owner_entity_id: int, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get fields by owner entity ID
        
        Args:
            owner_entity_id: Owner entity ID
            limit: Maximum number of fields to return
            
        Returns:
            list: List of field dictionaries
        """
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
                fields = cursor.fetchall()
                cursor.close()
                
                return fields
                
        except Exception as e:
            print(f"Error fetching fields for owner {owner_entity_id}: {e}")
            return []
    
    def search_fields(self, filters: Dict[str, Any], limit: int = 100) -> List[Dict[str, Any]]:
        """
        Search fields with filters
        
        Args:
            filters: Dictionary of search filters
            limit: Maximum results
            
        Returns:
            list: Matching fields
        """
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
                
                # Base query
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
                fields = cursor.fetchall()
                cursor.close()
                
                return fields
                
        except Exception as e:
            print(f"Error searching fields: {e}")
            return []
    
    def create_field(self, field_data: Dict[str, Any]) -> Optional[int]:
        """
        Create a new field
        
        Args:
            field_data: Field information
            
        Returns:
            int: New field ID or None if failed
        """
        try:
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
    """Repository for quote-related database operations"""
    
    def __init__(self):
        self.db = DatabaseManager()
    
    def save_quote(self, quote_data: Dict[str, Any]) -> Optional[str]:
        """
        Save quote to database
        
        Args:
            quote_data: Complete quote information
            
        Returns:
            str: Quote ID or None if failed
        """
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                
                # Generate quote ID
                quote_id = f"Q{datetime.now().strftime('%Y%m%d_%H%M%S')}_{quote_data.get('field_id', 'GEOM')}"
                
                query = """
                INSERT INTO quotes 
                (quote_id, field_id, crop, year, quote_type, sum_insured,
                 gross_premium, premium_rate, payout_index, burning_cost,
                 planting_date, zone, quote_data, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                values = (
                    quote_id,
                    quote_data.get('field_id'),
                    quote_data.get('crop'),
                    quote_data.get('year'),
                    quote_data.get('quote_type'),
                    quote_data.get('sum_insured'),
                    quote_data.get('gross_premium'),
                    quote_data.get('premium_rate'),
                    quote_data.get('total_payout_ratio'),
                    quote_data.get('burning_cost'),
                    quote_data.get('planting_date'),
                    quote_data.get('zone'),
                    json.dumps(quote_data),
                    datetime.utcnow()
                )
                
                cursor.execute(query, values)
                cursor.close()
                
                return quote_id
                
        except Exception as e:
            print(f"Error saving quote: {e}")
            return None
    
    def get_quote_by_id(self, quote_id: str) -> Optional[Dict[str, Any]]:
        """
        Get quote by ID
        
        Args:
            quote_id: Quote ID
            
        Returns:
            dict: Quote data or None if not found
        """
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
                quote = cursor.fetchone()
                cursor.close()
                
                if quote and quote.get('quote_data'):
                    try:
                        quote['quote_data'] = json.loads(quote['quote_data'])
                    except json.JSONDecodeError:
                        quote['quote_data'] = {}
                
                return quote
                
        except Exception as e:
            print(f"Error fetching quote {quote_id}: {e}")
            return None
    
    def get_quotes_by_field(self, field_id: int, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Get quotes for a field
        
        Args:
            field_id: Field ID
            limit: Maximum number of quotes
            
        Returns:
            list: List of quotes
        """
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
                quotes = cursor.fetchall()
                cursor.close()
                
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
            
            # Create quotes table if it doesn't exist
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
            
            # Create quote_metrics table for analytics
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS quote_metrics (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    quote_id VARCHAR(100) NULL,
                    execution_time_ms INT NOT NULL,
                    gee_api_calls INT DEFAULT 0,
                    years_processed INT DEFAULT 0,
                    crop VARCHAR(50) NOT NULL,
                    quote_type VARCHAR(20) NOT NULL,
                    premium_rate DECIMAL(6,4) NULL,
                    payout_index DECIMAL(6,4) NULL,
                    error_message TEXT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    INDEX idx_crop_type (crop, quote_type),
                    INDEX idx_created_at (created_at),
                    FOREIGN KEY (quote_id) REFERENCES quotes(quote_id) ON DELETE SET NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            cursor.close()
            print("✅ Database tables initialized successfully")
            
    except Exception as e:
        print(f"❌ Error initializing database tables: {e}")
        raise
