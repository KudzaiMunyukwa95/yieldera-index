"""
Configuration management for Yieldera Index Insurance Engine
"""

import os
from datetime import datetime

class Config:
    """Base configuration class"""
    
    # Flask settings
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'yieldera-dev-key-change-in-production'
    FLASK_ENV = os.environ.get('FLASK_ENV', 'production')
    
    # Database configuration
    DB_HOST = os.environ.get('DB_HOST')
    DB_PORT = int(os.environ.get('DB_PORT', 3306))
    DB_NAME = os.environ.get('DB_NAME')
    DB_USER = os.environ.get('DB_USER')
    DB_PASSWORD = os.environ.get('DB_PASSWORD')
    
    # Google Earth Engine
    GOOGLE_APPLICATION_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    GEE_SERVICE_ACCOUNT_CREDENTIALS_JSON = os.environ.get('GEE_SERVICE_ACCOUNT_CREDENTIALS_JSON')
    
    # OpenAI
    OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
    OPENAI_MODEL = os.environ.get('OPENAI_MODEL', 'gpt-4')
    
    # CHIRPS/GEE settings
    CHIRPS_COLLECTION_ID = 'UCSB-CHG/CHIRPS/DAILY'
    CHIRPS_SCALE = 5566
    
    # Quote engine defaults
    DEFAULT_DEDUCTIBLE = 0.05  # 5%
    HISTORICAL_YEARS_RANGE = 10
    MIN_VALID_YEARS = 5
    PLANTING_TRIGGER_RAINFALL = 15  # mm over 7 days
    PLANTING_MIN_RAIN_DAYS = 2  # days with 3mm+
    
    # Rate limiting
    RATELIMIT_STORAGE_URL = os.environ.get('REDIS_URL', 'memory://')
    
    @classmethod
    def validate_config(cls):
        """Validate required configuration"""
        errors = []
        
        # Check database config
        required_db = ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
        for key in required_db:
            if not getattr(cls, key):
                errors.append(f"Missing database configuration: {key}")
        
        # Check GEE credentials
        if not cls.GOOGLE_APPLICATION_CREDENTIALS and not cls.GEE_SERVICE_ACCOUNT_CREDENTIALS_JSON:
            errors.append("Missing Google Earth Engine credentials")
        
        # Check OpenAI
        if not cls.OPENAI_API_KEY:
            errors.append("Missing OpenAI API key")
        
        if errors:
            raise ValueError(f"Configuration errors: {', '.join(errors)}")
        
        return True

class DevelopmentConfig(Config):
    """Development configuration"""
    DEBUG = True
    FLASK_ENV = 'development'

class ProductionConfig(Config):
    """Production configuration"""
    DEBUG = False
    FLASK_ENV = 'production'

class TestingConfig(Config):
    """Testing configuration"""
    TESTING = True
    FLASK_ENV = 'testing'
    # Use in-memory database for tests
    DB_HOST = 'localhost'
    DB_NAME = 'test_yieldera'

# Configuration mapping
config_map = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': ProductionConfig
}

def get_config():
    """Get configuration based on environment"""
    env = os.environ.get('FLASK_ENV', 'production')
    return config_map.get(env, config_map['default'])
