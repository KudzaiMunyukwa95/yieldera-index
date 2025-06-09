"""
Health check and system status endpoints
"""

from flask import Blueprint, jsonify
import traceback
from datetime import datetime
import os

from config import Config
from core.database import DatabaseManager
from core.crops import list_supported_crops, get_crop_summary_stats, AGROECOLOGICAL_ZONES
from core.gee_client import initialize_earth_engine

health_bp = Blueprint('health', __name__)

@health_bp.route('/health', methods=['GET'])
def health_check():
    """Comprehensive health check endpoint"""
    try:
        health_status = {
            "status": "healthy",
            "service": "Yieldera Index Insurance Engine",
            "version": "2.0.0",
            "timestamp": datetime.utcnow().isoformat(),
            "environment": os.environ.get('FLASK_ENV', 'production')
        }
        
        # Test database connection
        db = DatabaseManager()
        try:
            db_connected = db.test_
