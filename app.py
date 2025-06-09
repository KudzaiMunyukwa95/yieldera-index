"""
Yieldera Index Insurance Engine - Refined Application
Enhanced backend with rainfall-only planting detection and year-by-year simulation
"""

from flask import Flask, jsonify
from flask_cors import CORS
import os
import traceback
from datetime import datetime

# Core modules
from config import Config
from core.gee_client import initialize_earth_engine
from api.quotes import quotes_bp  # Updated with refined features
from api.fields import fields_bp
from api.health import health_bp

def create_app():
    """Application factory pattern with refined features"""
    app = Flask(__name__)
    app.config.from_object(Config)
    
    # Enable CORS
    CORS(app, resources={
        r"/api/*": {
            "origins": ["http://localhost:3000", "https://yieldera.com", "https://*.yieldera.com"],
            "methods": ["GET", "POST", "PUT", "DELETE"],
            "allow_headers": ["Content-Type", "Authorization"]
        }
    })
    
    # Initialize Earth Engine
    try:
        initialize_earth_engine()
        print("✅ Earth Engine initialized successfully")
    except Exception as e:
        print(f"❌ Earth Engine initialization failed: {e}")
        # Don't crash the app, but log the error
    
    # Register blueprints
    app.register_blueprint(quotes_bp, url_prefix='/api/quotes')
    app.register_blueprint(fields_bp, url_prefix='/api/fields') 
    app.register_blueprint(health_bp, url_prefix='/api')
    
    # Enhanced root endpoint
    @app.route('/')
    def home():
        return jsonify({
            "service": "Yieldera Index Insurance Engine",
            "version": "2.1.0-Refined", 
            "status": "running",
            "timestamp": datetime.utcnow().isoformat(),
            "message": "Refined agricultural index insurance platform for Southern Africa",
            "refinements": [
                "Rainfall-only planting detection (no NDVI)",
                "Year-by-year premium/payout simulation", 
                "Seasonal validation (Oct-Jan only)",
                "Enhanced agronomic criteria"
            ],
            "endpoints": {
                "health": "/api/health",
                "crops": "/api/health/crops",
                "quotes": {
                    "historical": "POST /api/quotes/historical",
                    "prospective": "POST /api/quotes/prospective", 
                    "field_based": "POST /api/quotes/field/{field_id}",
                    "bulk": "POST /api/quotes/bulk",
                    "detailed_report": "GET /api/quotes/report/{quote_id}",
                    "simulation_details": "GET /api/quotes/simulation/{quote_id}",
                    "test_refined": "POST /api/quotes/test/refined",
                    "validate": "POST /api/quotes/validate"
                },
                "fields": {
                    "list": "GET /api/fields",
                    "get": "GET /api/fields/{field_id}",
                    "create": "POST /api/fields"
                }
            },
            "planting_detection": {
                "method": "rainfall_only",
                "criteria": {
                    "cumulative_threshold": "≥20mm over 7 consecutive days",
                    "daily_threshold": "≥5mm per day",
                    "minimum_qualifying_days": 2,
                    "detection_window": "October 1 - January 31"
                },
                "data_source": "CHIRPS satellite rainfall",
                "no_ndvi": "NDVI logic completely removed"
            },
            "simulation_features": [
                "Individual year premium calculation",
                "Year-by-year payout simulation",
                "Historical drought impact analysis",
                "Net farmer outcome tracking",
                "Loss ratio projections",
                "Critical period identification"
            ],
            "seasonal_validation": {
                "focus": "Summer crops only",
                "planting_window": "October - January",
                "off_season_rejection": "Automatic",
                "agronomic_basis": "Southern African cropping patterns"
            },
            "key_improvements": [
                "Fixed premium rate simulation per year",
                "Seasonal logic for prospective quotes",
                "Rainfall-based planting (20mm/7days)",
                "Accurate agronomic criteria"
            ]
        })
    
    # Enhanced error handlers
    @app.errorhandler(404)
    def not_found(error):
        return jsonify({
            "error": "Not Found",
            "message": "The requested endpoint does not exist",
            "available_endpoints": [
                "GET /",
                "GET /api/health", 
                "POST /api/quotes/historical",
                "POST /api/quotes/prospective",
                "POST /api/quotes/field/{field_id}",
                "GET /api/quotes/simulation/{quote_id}",
                "POST /api/quotes/test/refined",
                "POST /api/quotes/validate",
                "GET /api/fields"
            ],
            "version": "2.1.0-Refined"
        }), 404
    
    @app.errorhandler(500)
    def internal_error(error):
        return jsonify({
            "error": "Internal Server Error",
            "message": "An unexpected error occurred",
            "support": "Contact support@yieldera.com",
            "version": "2.1.0-Refined"
        }), 500
    
    @app.errorhandler(400)
    def bad_request(error):
        return jsonify({
            "error": "Bad Request", 
            "message": "Invalid request format or missing required fields",
            "version": "2.1.0-Refined"
        }), 400
    
    return app

# Create app instance
app = create_app()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    debug = os.environ.get("FLASK_ENV") == "development"
    
    print(f"🚀 Starting Yieldera Index Insurance Engine (Refined)")
    print(f"📍 Port: {port}")
    print(f"🔧 Debug: {debug}")
    print(f"🌍 Environment: {os.environ.get('FLASK_ENV', 'production')}")
    print(f"⭐ Version: 2.1.0-Refined")
    print(f"🌧️ Planting Detection: Rainfall-only (20mm/7days, 2+ days ≥5mm)")
    print(f"📊 Simulation: Year-by-year premium/payout analysis")
    print(f"🗓️ Season Focus: Summer crops (Oct-Jan planting)")
    print(f"🎯 Geographic Focus: Southern Africa")
    print(f"🔧 Key Fixes:")
    print(f"   - Removed all NDVI logic")
    print(f"   - Added individual year premium/payout simulation")
    print(f"   - Fixed seasonal logic for prospective quotes")
    print(f"   - Enhanced agronomic planting criteria")
    
    app.run(host="0.0.0.0", port=port, debug=debug)
