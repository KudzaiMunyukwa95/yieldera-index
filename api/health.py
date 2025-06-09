"""
Health check and system status endpoints
"""

from flask import Blueprint, jsonify, request
import traceback
from datetime import datetime
import os
import sys

from config import Config

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
        try:
            from core.database import DatabaseManager
            db = DatabaseManager()
            db_connected = db.test_connection()
            health_status["database"] = {
                "status": "connected" if db_connected else "disconnected",
                "host": Config.DB_HOST,
                "database": Config.DB_NAME
            }
        except Exception as e:
            health_status["database"] = {
                "status": "error",
                "error": str(e)
            }
        
        # Test Earth Engine
        try:
            import ee
            test_point = ee.Geometry.Point([30, -17])
            health_status["earth_engine"] = {
                "status": "connected",
                "test_geometry": "success"
            }
        except Exception as e:
            health_status["earth_engine"] = {
                "status": "error",
                "error": str(e)
            }
        
        # OpenAI status
        health_status["openai"] = {
            "status": "configured" if Config.OPENAI_API_KEY else "not_configured",
            "model": Config.OPENAI_MODEL if Config.OPENAI_API_KEY else None
        }
        
        # System capabilities
        try:
            from core.crops import list_supported_crops, AGROECOLOGICAL_ZONES
            health_status["capabilities"] = {
                "supported_crops": len(list_supported_crops()),
                "agroecological_zones": len(AGROECOLOGICAL_ZONES),
                "quote_types": ["historical", "prospective"],
                "ai_summaries": bool(Config.OPENAI_API_KEY),
                "bulk_processing": True,
                "field_management": True
            }
        except Exception as e:
            health_status["capabilities"] = {
                "error": str(e)
            }
        
        # Determine overall status
        critical_systems = [
            health_status["database"]["status"] == "connected",
            health_status["earth_engine"]["status"] == "connected"
        ]
        
        if not all(critical_systems):
            health_status["status"] = "degraded"
            health_status["issues"] = []
            
            if health_status["database"]["status"] != "connected":
                health_status["issues"].append("Database connection failed")
            if health_status["earth_engine"]["status"] != "connected":
                health_status["issues"].append("Earth Engine connection failed")
        
        return jsonify(health_status)
        
    except Exception as e:
        print(f"Health check error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "service": "Yieldera Index Insurance Engine",
            "version": "2.0.0",
            "timestamp": datetime.utcnow().isoformat(),
            "error": str(e)
        }), 500

@health_bp.route('/health/database', methods=['GET'])
def database_health():
    """Detailed database health check"""
    try:
        from core.database import DatabaseManager
        db = DatabaseManager()
        
        # Test basic connection
        connection_test = db.test_connection()
        
        # Test table access
        table_status = {}
        try:
            with db.get_connection() as conn:
                cursor = conn.cursor()
                
                # Check fields table
                cursor.execute("SELECT COUNT(*) FROM fields")
                fields_count = cursor.fetchone()[0]
                table_status["fields"] = {
                    "accessible": True,
                    "record_count": fields_count
                }
                
                # Check quotes table (may not exist yet)
                try:
                    cursor.execute("SELECT COUNT(*) FROM quotes")
                    quotes_count = cursor.fetchone()[0]
                    table_status["quotes"] = {
                        "accessible": True,
                        "record_count": quotes_count
                    }
                except:
                    table_status["quotes"] = {
                        "accessible": False,
                        "note": "Table may not exist yet"
                    }
                
                cursor.close()
                
        except Exception as e:
            table_status["error"] = str(e)
        
        return jsonify({
            "status": "success",
            "database": {
                "connection": "connected" if connection_test else "failed",
                "host": Config.DB_HOST,
                "database": Config.DB_NAME,
                "tables": table_status
            }
        })
        
    except Exception as e:
        print(f"Database health check error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@health_bp.route('/health/gee', methods=['GET'])
def gee_health():
    """Google Earth Engine health check"""
    try:
        import ee
        
        # Test basic GEE operations
        test_results = {}
        
        # Test geometry creation
        try:
            test_point = ee.Geometry.Point([30, -17])
            test_results["geometry_creation"] = "success"
        except Exception as e:
            test_results["geometry_creation"] = f"failed: {str(e)}"
        
        # Test CHIRPS collection access
        try:
            collection = ee.ImageCollection(Config.CHIRPS_COLLECTION_ID)
            size = collection.size().getInfo()
            test_results["chirps_access"] = {
                "status": "success",
                "collection_size": size
            }
        except Exception as e:
            test_results["chirps_access"] = {
                "status": "failed",
                "error": str(e)
            }
        
        # Test simple computation
        try:
            test_date = "2023-01-01"
            test_collection = ee.ImageCollection(Config.CHIRPS_COLLECTION_ID) \
                .filterDate(test_date, "2023-01-02") \
                .first()
            
            test_value = test_collection.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=ee.Geometry.Point([30, -17]),
                scale=Config.CHIRPS_SCALE
            ).getInfo()
            
            test_results["computation"] = {
                "status": "success",
                "test_date": test_date,
                "test_result": test_value
            }
        except Exception as e:
            test_results["computation"] = {
                "status": "failed",
                "error": str(e)
            }
        
        return jsonify({
            "status": "success",
            "earth_engine": {
                "collection_id": Config.CHIRPS_COLLECTION_ID,
                "scale": Config.CHIRPS_SCALE,
                "tests": test_results
            }
        })
        
    except Exception as e:
        print(f"GEE health check error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@health_bp.route('/crops', methods=['GET'])
def list_crops():
    """List all supported crops and their configurations"""
    try:
        from core.crops import list_supported_crops, get_crop_summary_stats, AGROECOLOGICAL_ZONES
        
        crops_info = list_supported_crops()
        summary_stats = get_crop_summary_stats()
        
        return jsonify({
            "status": "success",
            "crops": crops_info,
            "summary": summary_stats,
            "agroecological_zones": AGROECOLOGICAL_ZONES,
            "planting_detection": {
                "trigger_rainfall_mm": Config.PLANTING_TRIGGER_RAINFALL,
                "min_rain_days": Config.PLANTING_MIN_RAIN_DAYS,
                "description": "7-day cumulative rainfall ≥15mm with 2+ days ≥3mm"
            },
            "quote_defaults": {
                "default_deductible": Config.DEFAULT_DEDUCTIBLE,
                "historical_years_range": Config.HISTORICAL_YEARS_RANGE,
                "min_valid_years": Config.MIN_VALID_YEARS
            }
        })
        
    except Exception as e:
        print(f"List crops error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@health_bp.route('/config', methods=['GET'])
def get_configuration():
    """Get system configuration (non-sensitive data only)"""
    try:
        config_info = {
            "system": {
                "version": "2.0.0",
                "environment": os.environ.get('FLASK_ENV', 'production'),
                "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
            },
            "gee": {
                "collection_id": Config.CHIRPS_COLLECTION_ID,
                "scale": Config.CHIRPS_SCALE,
                "credentials_configured": bool(Config.GOOGLE_APPLICATION_CREDENTIALS or Config.GEE_SERVICE_ACCOUNT_CREDENTIALS_JSON)
            },
            "database": {
                "host": Config.DB_HOST,
                "port": Config.DB_PORT,
                "database": Config.DB_NAME,
                "user": Config.DB_USER,
                "password_configured": bool(Config.DB_PASSWORD)
            },
            "openai": {
                "model": Config.OPENAI_MODEL,
                "api_key_configured": bool(Config.OPENAI_API_KEY)
            },
            "quote_engine": {
                "default_deductible": Config.DEFAULT_DEDUCTIBLE,
                "historical_years_range": Config.HISTORICAL_YEARS_RANGE,
                "min_valid_years": Config.MIN_VALID_YEARS,
                "planting_trigger_rainfall": Config.PLANTING_TRIGGER_RAINFALL,
                "planting_min_rain_days": Config.PLANTING_MIN_RAIN_DAYS
            }
        }
        
        return jsonify({
            "status": "success",
            "configuration": config_info
        })
        
    except Exception as e:
        print(f"Get configuration error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@health_bp.route('/endpoints', methods=['GET'])
def list_endpoints():
    """List all available API endpoints"""
    try:
        endpoints = {
            "health": {
                "GET /api/health": "Comprehensive health check",
                "GET /api/health/database": "Database health check",
                "GET /api/health/gee": "Google Earth Engine health check",
                "GET /api/crops": "List supported crops",
                "GET /api/config": "System configuration",
                "GET /api/endpoints": "This endpoint list"
            },
            "quotes": {
                "POST /api/quotes/historical": "Generate historical quote",
                "POST /api/quotes/prospective": "Generate prospective quote", 
                "POST /api/quotes/field/{field_id}": "Generate quote for specific field",
                "POST /api/quotes/bulk": "Generate bulk quotes",
                "GET /api/quotes/{quote_id}": "Get quote by ID",
                "GET /api/quotes/field/{field_id}/history": "Get field quote history",
                "POST /api/quotes/validate": "Validate quote request"
            },
            "fields": {
                "GET /api/fields": "List fields with filtering",
                "GET /api/fields/{field_id}": "Get field details",
                "POST /api/fields": "Create new field",
                "PUT /api/fields/{field_id}": "Update field (not implemented)",
                "DELETE /api/fields/{field_id}": "Delete field (not implemented)",
                "POST /api/fields/validate": "Validate field data",
                "POST /api/fields/import": "Bulk import fields"
            }
        }
        
        return jsonify({
            "status": "success",
            "api_version": "2.0.0",
            "base_url": request.host_url,
            "endpoints": endpoints,
            "authentication": "None (public API)",
            "rate_limiting": "Not implemented",
            "documentation": "Available in endpoint responses"
        })
        
    except Exception as e:
        print(f"List endpoints error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
