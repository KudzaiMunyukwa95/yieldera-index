"""
Yieldera Index Insurance Engine - Main Application
Next-generation backend for agricultural index insurance
"""

from flask import Flask, jsonify
from flask_cors import CORS
import os
import traceback
from datetime import datetime

# Core modules
from config import Config
from core.gee_client import initialize_earth_engine
from api.quotes import quotes_bp
from api.fields import fields_bp
from api.health import health_bp

def create_app():
    """Application factory pattern"""
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
    
    # Root endpoint
    @app.route('/')
    def home():
        return jsonify({
            "service": "Yieldera Index Insurance Engine",
            "version": "2.0.0",
            "status": "running",
            "timestamp": datetime.utcnow().isoformat(),
            "message": "Next-generation agricultural index insurance platform for Africa",
            "endpoints": {
                "health": "/api/health",
                "crops": "/api/health/crops",
                "quotes": {
                    "historical": "POST /api/quotes/historical",
                    "prospective": "POST /api/quotes/prospective", 
                    "field_based": "POST /api/quotes/field/{field_id}",
                    "bulk": "POST /api/quotes/bulk"
                },
                "fields": {
                    "list": "GET /api/fields",
                    "get": "GET /api/fields/{field_id}",
                    "create": "POST /api/fields"
                }
            },
            "features": [
                "Multi-crop support (9 crops)",
                "Automatic planting detection",
                "Historical & prospective quoting",
                "AI-powered summaries",
                "Zone-based adjustments",
                "Real-time GEE integration",
                "Production-ready error handling"
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
                "GET /api/fields"
            ]
        }), 404
    
    @app.errorhandler(500)
    def internal_error(error):
        return jsonify({
            "error": "Internal Server Error",
            "message": "An unexpected error occurred",
            "support": "Contact support@yieldera.com"
        }), 500
    
    @app.errorhandler(400)
    def bad_request(error):
        return jsonify({
            "error": "Bad Request", 
            "message": "Invalid request format or missing required fields"
        }), 400
    
    return app

# Create app instance
app = create_app()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    debug = os.environ.get("FLASK_ENV") == "development"
    
    print(f"🚀 Starting Yieldera Index Insurance Engine")
    print(f"📍 Port: {port}")
    print(f"🔧 Debug: {debug}")
    print(f"🌍 Environment: {os.environ.get('FLASK_ENV', 'production')}")
    
    app.run(host="0.0.0.0", port=port, debug=debug)
