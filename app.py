"""
Yieldera Index Insurance Engine - Main Application (Enhanced Version)
Next-generation backend for agricultural index insurance with comprehensive reporting
"""

from flask import Flask, jsonify
from flask_cors import CORS
import os
import traceback
from datetime import datetime

# Core modules
from config import Config
from core.gee_client import initialize_earth_engine
from api.quotes import quotes_bp  # Updated with enhanced features
from api.fields import fields_bp
from api.health import health_bp

def create_app():
    """Application factory pattern with enhanced features"""
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
            "version": "2.0.0-Enhanced",
            "status": "running",
            "timestamp": datetime.utcnow().isoformat(),
            "message": "Next-generation agricultural index insurance platform for Africa",
            "enhancement_level": "Comprehensive reporting exceeding ACRE Africa standards",
            "endpoints": {
                "health": "/api/health",
                "crops": "/api/health/crops",
                "quotes": {
                    "historical": "POST /api/quotes/historical",
                    "prospective": "POST /api/quotes/prospective", 
                    "field_based": "POST /api/quotes/field/{field_id}",
                    "bulk": "POST /api/quotes/bulk",
                    "detailed_report": "GET /api/quotes/report/{quote_id}",
                    "test_enhanced": "POST /api/quotes/test/enhanced"
                },
                "fields": {
                    "list": "GET /api/fields",
                    "get": "GET /api/fields/{field_id}",
                    "create": "POST /api/fields"
                }
            },
            "enhanced_features": [
                "Comprehensive quote reports (9 sections)",
                "Executive summary generation",
                "Detailed coverage specifications",
                "Risk analysis with zone adjustments",
                "Payout structure documentation",
                "Enhanced phase breakdown",
                "Financial summary with loadings",
                "Technical specifications",
                "Claims procedure details",
                "Terms and conditions",
                "Portfolio analysis for bulk quotes",
                "Trend analysis for field history"
            ],
            "report_sections": [
                "executive_summary",
                "coverage_details", 
                "risk_analysis",
                "payout_structure",
                "phase_breakdown",
                "financial_summary",
                "technical_specifications",
                "claims_procedure",
                "terms_and_conditions"
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
                "GET /api/quotes/report/{quote_id}",
                "POST /api/quotes/test/enhanced",
                "GET /api/fields"
            ]
        }), 404
    
    @app.errorhandler(500)
    def internal_error(error):
        return jsonify({
            "error": "Internal Server Error",
            "message": "An unexpected error occurred",
            "support": "Contact support@yieldera.com",
            "version": "2.0.0-Enhanced"
        }), 500
    
    @app.errorhandler(400)
    def bad_request(error):
        return jsonify({
            "error": "Bad Request", 
            "message": "Invalid request format or missing required fields",
            "version": "2.0.0-Enhanced"
        }), 400
    
    return app

# Create app instance
app = create_app()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    debug = os.environ.get("FLASK_ENV") == "development"
    
    print(f"🚀 Starting Yieldera Index Insurance Engine (Enhanced)")
    print(f"📍 Port: {port}")
    print(f"🔧 Debug: {debug}")
    print(f"🌍 Environment: {os.environ.get('FLASK_ENV', 'production')}")
    print(f"⭐ Enhancement: Comprehensive reporting system")
    print(f"📊 Report Sections: 9 detailed sections per quote")
    print(f"🎯 Quality Level: Exceeds ACRE Africa standards")
    
    app.run(host="0.0.0.0", port=port, debug=debug)
