"""
Quote API endpoints - FINAL FIX for NoneType Error
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

def safe_float_conversion(value, field_name, default=None):
    """Safely convert value to float with proper error handling"""
    if value is None:
        return default
    
    try:
        float_value = float(value)
        return float_value
    except (ValueError, TypeError):
        print(f"Warning: Could not convert {field_name} to float: {value}")
        return default

def validate_field_coordinates(field_data, field_id):
    """Validate field coordinates with comprehensive error checking"""
    
    # Check if latitude exists and is not None
    lat_raw = field_data.get('latitude')
    lng_raw = field_data.get('longitude')
    
    if lat_raw is None:
        return None, None, f"Field {field_id} has no latitude data"
    
    if lng_raw is None:
        return None, None, f"Field {field_id} has no longitude data"
    
    # Try to convert to float
    try:
        latitude = float(lat_raw)
    except (ValueError, TypeError):
        return None, None, f"Field {field_id} has invalid latitude: {lat_raw}"
    
    try:
        longitude = float(lng_raw)
    except (ValueError, TypeError):
        return None, None, f"Field {field_id} has invalid longitude: {lng_raw}"
    
    # Validate coordinate ranges
    if not (-90 <= latitude <= 90):
        return None, None, f"Field {field_id} latitude out of range: {latitude}"
    
    if not (-180 <= longitude <= 180):
        return None, None, f"Field {field_id} longitude out of range: {longitude}"
    
    return latitude, longitude, None

@quotes_bp.route('/historical', methods=['POST'])
def historical_quote():
    """Generate historical quote for a specific year"""
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
    """Generate prospective quote for future season - OPTIMIZED"""
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
    Generate quote for a specific field - ENHANCED NULL HANDLING
    """
    try:
        start_time = time.time()
        data = request.get_json()
        
        if not data:
            return jsonify({
                "status": "error",
                "message": "Request body is required"
            }), 400
        
        # Validate required fields in request
        required_fields = ['expected_yield', 'price_per_ton']
        for field in required_fields:
            if field not in data:
                return jsonify({
                    "status": "error",
                    "message": f"Missing required field: {field}"
                }), 400
        
        # Get field data
        print(f"Fetching field data for field {field_id}")
        field_data = fields_repo.get_field_by_id(field_id)
        
        if not field_data:
            return jsonify({
                "status": "error",
                "message": f"Field {field_id} not found in database"
            }), 404
        
        print(f"Field {field_id} data: {field_data}")
        
        # Validate and extract coordinates using safe conversion
        latitude, longitude, coord_error = validate_field_coordinates(field_data, field_id)
        
        if coord_error:
            return jsonify({
                "status": "error",
                "message": coord_error
            }), 400
        
        print(f"Field {field_id} coordinates validated: lat={latitude}, lng={longitude}")
        
        # Handle area_ha with comprehensive null checking
        area_ha = None
        area_raw = field_data.get('area_ha')
        
        if area_raw is not None:
            area_ha = safe_float_conversion(area_raw, 'area_ha', None)
            if area_ha is not None and area_ha <= 0:
                print(f"Warning: Field {field_id} has non-positive area: {area_ha}, setting to None")
                area_ha = None
        
        print(f"Field {field_id} area_ha: {area_ha}")
        
        # Handle crop field
        crop = field_data.get('crop')
        if not crop or crop.strip() == '':
            crop = 'maize'  # Default crop
        else:
            crop = str(crop).strip()
        
        print(f"Field {field_id} crop: {crop}")
        
        # Prepare quote request with validated data
        quote_request = {
            'latitude': latitude,
            'longitude': longitude,
            'area_ha': area_ha,
            'crop': crop,
            'expected_yield': safe_float_conversion(data['expected_yield'], 'expected_yield'),
            'price_per_ton': safe_float_conversion(data['price_per_ton'], 'price_per_ton'),
            'year': int(data.get('year', datetime.now().year)),
            'loadings': data.get('loadings', {}),
            'zone': data.get('zone', 'auto_detect'),
            'deductible_rate': safe_float_conversion(data.get('deductible_rate'), 'deductible_rate'),
            'deductible_threshold': safe_float_conversion(data.get('deductible_threshold'), 'deductible_threshold', 0.0),
            'buffer_radius': safe_float_conversion(data.get('buffer_radius'), 'buffer_radius', 1500),
            'field_info': {
                'type': 'field',
                'field_id': field_id,
                'name': field_data.get('name', f'Field {field_id}'),
                'farmer_name': field_data.get('farmer_name'),
                'area_ha': area_ha
            }
        }
        
        # Validate essential quote request fields
        if quote_request['expected_yield'] is None or quote_request['expected_yield'] <= 0:
            return jsonify({
                "status": "error",
                "message": "Invalid expected_yield value"
            }), 400
            
        if quote_request['price_per_ton'] is None or quote_request['price_per_ton'] <= 0:
            return jsonify({
                "status": "error",
                "message": "Invalid price_per_ton value"
            }), 400
        
        print(f"Executing quote for field {field_id}")
        
        # Execute quote
        quote_result = quote_engine.execute_quote(quote_request)
        
        # Add field information to result
        quote_result['field_info'] = quote_request['field_info']
        
        print(f"Quote execution completed for field {field_id}")
        
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
        
        execution_time = time.time() - start_time
        
        return jsonify({
            "status": "success",
            "quote": quote_result,
            "field_data": {
                "id": field_data['id'],
                "name": field_data.get('name'),
                "farmer_name": field_data.get('farmer_name'),
                "area_ha": area_ha,
                "crop": crop,
                "latitude": latitude,
                "longitude": longitude
            },
            "execution_time_seconds": round(execution_time, 2),
            "version": "2.0.0"
        })
        
    except Exception as e:
        print(f"Field quote error for field {field_id}: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__,
            "field_id": field_id
        }), 500

@quotes_bp.route('/bulk', methods=['POST'])
def bulk_quote():
    """Generate quotes for multiple fields or locations"""
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
                
                # Handle field-based request with enhanced validation
                if 'field_id' in req:
                    field_data = fields_repo.get_field_by_id(req['field_id'])
                    if field_data:
                        # Validate coordinates
                        latitude, longitude, coord_error = validate_field_coordinates(field_data, req['field_id'])
                        
                        if coord_error:
                            results.append({
                                "request_index": i,
                                "status": "error",
                                "message": coord_error
                            })
                            continue
                        
                        # Handle area_ha safely
                        area_ha = safe_float_conversion(field_data.get('area_ha'), 'area_ha', None)
                        if area_ha is not None and area_ha <= 0:
                            area_ha = None
                        
                        quote_request.update({
                            'latitude': latitude,
                            'longitude': longitude,
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
        
        # Validate numeric fields with safe conversion
        numeric_fields = ['expected_yield', 'price_per_ton', 'area_ha', 'year', 'latitude', 'longitude']
        for field in numeric_fields:
            if field in data and data[field] is not None:
                converted_value = safe_float_conversion(data[field], field)
                if converted_value is None:
                    validation_errors.append(f"Field '{field}' must be a valid number")
                elif field in ['expected_yield', 'price_per_ton', 'area_ha'] and converted_value <= 0:
                    validation_errors.append(f"Field '{field}' must be positive")
        
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
