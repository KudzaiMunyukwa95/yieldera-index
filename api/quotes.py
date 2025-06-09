"""
Quote API endpoints for Yieldera Index Insurance Engine - COMPLETE FIXED VERSION
"""

from flask import Blueprint, request, jsonify
import traceback
import time
from datetime import datetime
from typing import Dict, List, Any

from core.quote_engine import QuoteEngine
from core.database import FieldsRepository, QuotesRepository
from core.ai_summary import AISummaryGenerator
from core.crops import validate_crop, list_supported_crops

quotes_bp = Blueprint('quotes', __name__)

# Initialize components
quote_engine = QuoteEngine()
fields_repo = FieldsRepository()
quotes_repo = QuotesRepository()
ai_generator = AISummaryGenerator()

@quotes_bp.route('/historical', methods=['POST'])
def historical_quote():
    """
    Generate historical quote for a specific year
    
    Request body:
    {
        "year": 2023,
        "crop": "maize",
        "expected_yield": 5.0,
        "price_per_ton": 300,
        "area_ha": 2.5,
        "latitude": -17.7888,
        "longitude": 30.6015,
        "buffer_radius": 1500,
        "loadings": {"admin": 0.15, "profit": 0.10},
        "zone": "auto_detect"
    }
    """
    try:
        start_time = time.time()
        data = request.get_json()
        
        if not data:
            return jsonify({
                "status": "error",
                "message": "Request body is required"
            }), 400
        
        # Validate required fields
        required_fields = ['year', 'expected_yield', 'price_per_ton']
        for field in required_fields:
            if field not in data:
                return jsonify({
                    "status": "error",
                    "message": f"Missing required field: {field}"
                }), 400
        
        # Execute quote
        quote_result = quote_engine.execute_quote(data)
        
        # Generate AI summary
        try:
            ai_summary = ai_generator.generate_quote_summary(quote_result, data.get('location_info'))
            quote_result['ai_summary'] = ai_summary
        except Exception as e:
            print(f"AI summary generation failed: {e}")
            quote_result['ai_summary'] = "AI summary temporarily unavailable"
        
        # Save quote to database
        try:
            quote_id = quotes_repo.save_quote(quote_result)
            if quote_id:
                quote_result['quote_id'] = quote_id
        except Exception as e:
            print(f"Failed to save quote: {e}")
            # Continue without saving
        
        execution_time = time.time() - start_time
        
        return jsonify({
            "status": "success",
            "quote": quote_result,
            "execution_time_seconds": round(execution_time, 2),
            "version": "2.0.0"
        })
        
    except Exception as e:
        print(f"Historical quote error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }), 500

@quotes_bp.route('/prospective', methods=['POST'])
def prospective_quote():
    """
    Generate prospective quote for future season - OPTIMIZED
    
    Request body:
    {
        "year": 2025,
        "crop": "maize",
        "expected_yield": 5.0,
        "price_per_ton": 300,
        "area_ha": 2.5,
        "latitude": -17.7888,
        "longitude": 30.6015,
        "buffer_radius": 1500,
        "loadings": {"admin": 0.15, "profit": 0.10},
        "zone": "auto_detect"
    }
    """
    try:
        start_time = time.time()
        data = request.get_json()
        
        if not data:
            return jsonify({
                "status": "error",
                "message": "Request body is required"
            }), 400
        
        # Validate required fields
        required_fields = ['expected_yield', 'price_per_ton']
        for field in required_fields:
            if field not in data:
                return jsonify({
                    "status": "error",
                    "message": f"Missing required field: {field}"
                }), 400
        
        # Default to next year if not specified
        if 'year' not in data:
            data['year'] = datetime.now().year + 1
        
        print(f"Starting prospective quote for year {data['year']}")
        
        # Execute quote with timeout protection
        quote_result = quote_engine.execute_quote(data)
        
        # Generate AI summary
        try:
            ai_summary = ai_generator.generate_quote_summary(quote_result, data.get('location_info'))
            quote_result['ai_summary'] = ai_summary
        except Exception as e:
            print(f"AI summary generation failed: {e}")
            quote_result['ai_summary'] = "AI summary temporarily unavailable"
        
        # Save quote to database
        try:
            quote_id = quotes_repo.save_quote(quote_result)
            if quote_id:
                quote_result['quote_id'] = quote_id
        except Exception as e:
            print(f"Failed to save quote: {e}")
            # Continue without saving
        
        execution_time = time.time() - start_time
        print(f"Prospective quote completed in {execution_time:.2f} seconds")
        
        return jsonify({
            "status": "success",
            "quote": quote_result,
            "execution_time_seconds": round(execution_time, 2),
            "version": "2.0.0"
        })
        
    except Exception as e:
        print(f"Prospective quote error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }), 500

@quotes_bp.route('/field/<int:field_id>', methods=['POST'])
def field_based_quote(field_id):
    """
    Generate quote for a specific field from database - ENHANCED ERROR HANDLING
    
    Request body:
    {
        "year": 2024,
        "expected_yield": 5.0,
        "price_per_ton": 300,
        "loadings": {"admin": 0.15, "profit": 0.10},
        "zone": "auto_detect"
    }
    """
    try:
        start_time = time.time()
        data = request.get_json()
        
        if not data:
            return jsonify({
                "status": "error",
                "message": "Request body is required"
            }), 400
        
        # Get field data with enhanced validation
        field_data = fields_repo.get_field_by_id(field_id)
        if not field_data:
            return jsonify({
                "status": "error",
                "message": f"Field {field_id} not found"
            }), 404
        
        # Validate field coordinates
        if not field_data.get('latitude') or not field_data.get('longitude'):
            return jsonify({
                "status": "error",
                "message": f"Field {field_id} has invalid or missing coordinates"
            }), 400
        
        # Validate required fields
        required_fields = ['expected_yield', 'price_per_ton']
        for field in required_fields:
            if field not in data:
                return jsonify({
                    "status": "error",
                    "message": f"Missing required field: {field}"
                }), 400
        
        # Enhanced field data validation with None checks
        try:
            latitude = float(field_data['latitude'])
            longitude = float(field_data['longitude'])
            
            # Validate coordinate ranges
            if not (-90 <= latitude <= 90):
                raise ValueError(f"Invalid latitude: {latitude}")
            if not (-180 <= longitude <= 180):
                raise ValueError(f"Invalid longitude: {longitude}")
                
        except (ValueError, TypeError) as e:
            return jsonify({
                "status": "error",
                "message": f"Field {field_id} has invalid coordinates: {str(e)}"
            }), 400
        
        # Handle area_ha with proper None checking
        area_ha = None
        if field_data.get('area_ha') is not None:
            try:
                area_ha = float(field_data['area_ha'])
                if area_ha <= 0:
                    print(f"Warning: Field {field_id} has invalid area: {area_ha}")
                    area_ha = None
            except (ValueError, TypeError):
                print(f"Warning: Field {field_id} has non-numeric area: {field_data.get('area_ha')}")
                area_ha = None
        
        # Prepare quote request
        quote_request = {
            'latitude': latitude,
            'longitude': longitude,
            'area_ha': area_ha,
            'crop': field_data.get('crop', 'maize'),
            'expected_yield': data['expected_yield'],
            'price_per_ton': data['price_per_ton'],
            'year': data.get('year', datetime.now().year),
            'loadings': data.get('loadings', {}),
            'zone': data.get('zone', 'auto_detect'),
            'deductible_rate': data.get('deductible_rate'),
            'deductible_threshold': data.get('deductible_threshold'),
            'buffer_radius': data.get('buffer_radius', 1500),
            'field_info': {
                'type': 'field',
                'field_id': field_id,
                'name': field_data.get('name', f'Field {field_id}'),
                'farmer_name': field_data.get('farmer_name'),
                'area_ha': area_ha
            }
        }
        
        # Execute quote
        quote_result = quote_engine.execute_quote(quote_request)
        
        # Add field information to result
        quote_result['field_info'] = quote_request['field_info']
        
        # Generate AI summary
        try:
            ai_summary = ai_generator.generate_quote_summary(quote_result, quote_request['field_info'])
            quote_result['ai_summary'] = ai_summary
        except Exception as e:
            print(f"AI summary generation failed: {e}")
            quote_result['ai_summary'] = "AI summary temporarily unavailable"
        
        # Save quote to database
        try:
            quote_result['field_id'] = field_id
            quote_id = quotes_repo.save_quote(quote_result)
            if quote_id:
                quote_result['quote_id'] = quote_id
        except Exception as e:
            print(f"Failed to save quote: {e}")
            # Continue without saving
        
        execution_time = time.time() - start_time
        
        return jsonify({
            "status": "success",
            "quote": quote_result,
            "field_data": {
                "id": field_data['id'],
                "name": field_data.get('name'),
                "farmer_name": field_data.get('farmer_name'),
                "area_ha": area_ha,
                "crop": field_data.get('crop'),
                "latitude": latitude,
                "longitude": longitude
            },
            "execution_time_seconds": round(execution_time, 2),
            "version": "2.0.0"
        })
        
    except Exception as e:
        print(f"Field quote error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }), 500

@quotes_bp.route('/bulk', methods=['POST'])
def bulk_quote():
    """
    Generate quotes for multiple fields or locations
    
    Request body:
    {
        "requests": [
            {
                "field_id": 56,
                "year": 2024,
                "expected_yield": 5.0,
                "price_per_ton": 300
            },
            {
                "latitude": -17.7888,
                "longitude": 30.6015,
                "crop": "sorghum",
                "year": 2024,
                "expected_yield": 4.0,
                "price_per_ton": 250
            }
        ],
        "global_settings": {
            "loadings": {"admin": 0.15, "profit": 0.10},
            "zone": "auto_detect"
        }
    }
    """
    try:
        start_time = time.time()
        data = request.get_json()
        
        if not data or 'requests' not in data:
            return jsonify({
                "status": "error",
                "message": "Missing 'requests' array in request body"
            }), 400
        
        requests = data['requests']
        global_settings = data.get('global_settings', {})
        
        if not requests:
            return jsonify({
                "status": "error",
                "message": "Requests array cannot be empty"
            }), 400
        
        results = []
        successful_quotes = []
        
        for i, req in enumerate(requests):
            try:
                # Merge global settings
                quote_request = {**global_settings, **req}
                
                # Handle field-based request
                if 'field_id' in req:
                    field_data = fields_repo.get_field_by_id(req['field_id'])
                    if field_data:
                        # Validate field data
                        if not field_data.get('latitude') or not field_data.get('longitude'):
                            results.append({
                                "request_index": i,
                                "status": "error",
                                "message": f"Field {req['field_id']} has invalid coordinates"
                            })
                            continue
                        
                        # Handle area_ha safely
                        area_ha = None
                        if field_data.get('area_ha') is not None:
                            try:
                                area_ha = float(field_data['area_ha'])
                                if area_ha <= 0:
                                    area_ha = None
                            except (ValueError, TypeError):
                                area_ha = None
                        
                        quote_request.update({
                            'latitude': float(field_data['latitude']),
                            'longitude': float(field_data['longitude']),
                            'area_ha': area_ha,
                            'crop': field_data.get('crop', quote_request.get('crop', 'maize')),
                            'field_info': {
                                'type': 'field',
                                'field_id': req['field_id'],
                                'name': field_data.get('name', f'Field {req["field_id"]}')
                            }
                        })
                    else:
                        results.append({
                            "request_index": i,
                            "status": "error",
                            "message": f"Field {req['field_id']} not found"
                        })
                        continue
                
                # Execute quote
                quote_result = quote_engine.execute_quote(quote_request)
                successful_quotes.append(quote_result)
                
                # Save to database
                try:
                    if 'field_id' in req:
                        quote_result['field_id'] = req['field_id']
                    quote_id = quotes_repo.save_quote(quote_result)
                    if quote_id:
                        quote_result['quote_id'] = quote_id
                except Exception as e:
                    print(f"Failed to save bulk quote: {e}")
                
                results.append({
                    "request_index": i,
                    "status": "success",
                    "quote": quote_result
                })
                
            except Exception as e:
                results.append({
                    "request_index": i,
                    "status": "error",
                    "message": str(e),
                    "error_type": type(e).__name__
                })
        
        # Generate bulk summary
        bulk_summary = ""
        if successful_quotes:
            try:
                bulk_summary = ai_generator.generate_bulk_summary(successful_quotes)
            except Exception as e:
                print(f"Bulk summary generation failed: {e}")
                bulk_summary = "Bulk summary temporarily unavailable"
        
        execution_time = time.time() - start_time
        successful_count = sum(1 for r in results if r['status'] == 'success')
        
        return jsonify({
            "status": "success",
            "bulk_summary": bulk_summary,
            "total_requests": len(requests),
            "successful_quotes": successful_count,
            "failed_quotes": len(requests) - successful_count,
            "results": results,
            "execution_time_seconds": round(execution_time, 2),
            "version": "2.0.0"
        })
        
    except Exception as e:
        print(f"Bulk quote error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }), 500

@quotes_bp.route('/<quote_id>', methods=['GET'])
def get_quote(quote_id):
    """Get quote by ID"""
    try:
        quote = quotes_repo.get_quote_by_id(quote_id)
        
        if not quote:
            return jsonify({
                "status": "error",
                "message": f"Quote {quote_id} not found"
            }), 404
        
        return jsonify({
            "status": "success",
            "quote": quote
        })
        
    except Exception as e:
        print(f"Get quote error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@quotes_bp.route('/field/<int:field_id>/history', methods=['GET'])
def get_field_quote_history(field_id):
    """Get quote history for a field"""
    try:
        limit = request.args.get('limit', 20, type=int)
        quotes = quotes_repo.get_quotes_by_field(field_id, limit)
        
        return jsonify({
            "status": "success",
            "field_id": field_id,
            "quotes": quotes,
            "total": len(quotes)
        })
        
    except Exception as e:
        print(f"Get field quotes error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@quotes_bp.route('/validate', methods=['POST'])
def validate_quote_request():
    """Validate quote request parameters without executing"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                "status": "error",
                "message": "Request body is required"
            }), 400
        
        validation_errors = []
        
        # Check required fields
        required_fields = ['expected_yield', 'price_per_ton']
        for field in required_fields:
            if field not in data:
                validation_errors.append(f"Missing required field: {field}")
        
        # Validate crop
        if 'crop' in data:
            try:
                validate_crop(data['crop'])
            except ValueError as e:
                validation_errors.append(str(e))
        
        # Validate geometry/coordinates
        has_geometry = 'geometry' in data
        has_coordinates = 'latitude' in data and 'longitude' in data
        has_field_id = 'field_id' in data
        
        if not (has_geometry or has_coordinates or has_field_id):
            validation_errors.append("Must provide either 'geometry', 'latitude'/'longitude', or 'field_id'")
        
        # Validate numeric fields with None checks
        numeric_fields = ['expected_yield', 'price_per_ton', 'area_ha', 'year', 'latitude', 'longitude']
        for field in numeric_fields:
            if field in data and data[field] is not None:
                try:
                    value = float(data[field])
                    if field in ['expected_yield', 'price_per_ton', 'area_ha'] and value <= 0:
                        validation_errors.append(f"Field '{field}' must be positive")
                except (ValueError, TypeError):
                    validation_errors.append(f"Field '{field}' must be a number")
        
        if validation_errors:
            return jsonify({
                "status": "error",
                "validation_errors": validation_errors
            }), 400
        
        return jsonify({
            "status": "success",
            "message": "Quote request is valid",
            "estimated_quote_type": quote_engine._determine_quote_type(data.get('year', datetime.now().year))
        })
        
    except Exception as e:
        print(f"Validation error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
