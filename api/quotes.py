"""
Enhanced Quote API endpoints with comprehensive reporting
Exceeds ACRE Africa term sheet detail level
"""

from flask import Blueprint, request, jsonify
import traceback
import time
from datetime import datetime
from typing import Dict, List, Any

from core.quote_engine import QuoteEngine
from core.database import FieldsRepository, QuotesRepository
from core.ai_summary import EnhancedAISummaryGenerator  # Updated import
from core.crops import validate_crop, list_supported_crops

quotes_bp = Blueprint('quotes', __name__)

# Initialize components with enhanced AI generator
quote_engine = QuoteEngine()
fields_repo = FieldsRepository()
quotes_repo = QuotesRepository()
enhanced_ai_generator = EnhancedAISummaryGenerator()

@quotes_bp.route('/historical', methods=['POST'])
def historical_quote():
    """Generate enhanced historical quote with comprehensive reporting"""
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
        
        # Generate comprehensive report
        try:
            comprehensive_report = enhanced_ai_generator.generate_comprehensive_quote_report(
                quote_result, data.get('location_info')
            )
            quote_result['comprehensive_report'] = comprehensive_report
            
            # Keep backward compatibility
            quote_result['ai_summary'] = comprehensive_report.get('executive_summary', 'Summary unavailable')
        except Exception as e:
            print(f"Enhanced report generation failed: {e}")
            quote_result['ai_summary'] = "Enhanced report temporarily unavailable"
            quote_result['comprehensive_report'] = {"error": "Report generation failed"}
        
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
            "version": "2.0.0-Enhanced",
            "report_type": "comprehensive"
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
    """Generate enhanced prospective quote with comprehensive reporting"""
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
        
        print(f"Starting enhanced prospective quote for year {data['year']}")
        
        # Execute quote
        quote_result = quote_engine.execute_quote(data)
        
        # Generate comprehensive report
        try:
            comprehensive_report = enhanced_ai_generator.generate_comprehensive_quote_report(
                quote_result, data.get('location_info')
            )
            quote_result['comprehensive_report'] = comprehensive_report
            
            # Keep backward compatibility
            quote_result['ai_summary'] = comprehensive_report.get('executive_summary', 'Summary unavailable')
        except Exception as e:
            print(f"Enhanced report generation failed: {e}")
            quote_result['ai_summary'] = "Enhanced report temporarily unavailable"
            quote_result['comprehensive_report'] = {"error": "Report generation failed"}
        
        # Save quote to database
        try:
            quote_id = quotes_repo.save_quote(quote_result)
            if quote_id:
                quote_result['quote_id'] = quote_id
        except Exception as e:
            print(f"Failed to save quote: {e}")
        
        execution_time = time.time() - start_time
        print(f"Enhanced prospective quote completed in {execution_time:.2f} seconds")
        
        return jsonify({
            "status": "success",
            "quote": quote_result,
            "execution_time_seconds": round(execution_time, 2),
            "version": "2.0.0-Enhanced",
            "report_type": "comprehensive"
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
    """Generate enhanced field-based quote with comprehensive reporting"""
    try:
        start_time = time.time()
        data = request.get_json()
        
        print(f"=== ENHANCED FIELD {field_id} QUOTE ===")
        
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
        field_data = fields_repo.get_field_by_id(field_id)
        
        if not field_data:
            return jsonify({
                "status": "error",
                "message": f"Field {field_id} not found in database"
            }), 404
        
        # Validate coordinates
        latitude = field_data.get('latitude')
        longitude = field_data.get('longitude')
        
        if latitude is None or longitude is None:
            return jsonify({
                "status": "error",
                "message": f"Field {field_id} has invalid coordinates"
            }), 400
        
        # Handle area_ha
        area_ha = field_data.get('area_ha')
        if area_ha is not None:
            try:
                area_ha = float(area_ha)
            except (ValueError, TypeError):
                area_ha = None
        
        # Handle crop
        crop = field_data.get('crop', 'maize')
        if not crop or str(crop).strip() == '':
            crop = 'maize'
        else:
            crop = str(crop).strip().lower()
        
        # Validate request data
        try:
            expected_yield = float(data['expected_yield'])
            price_per_ton = float(data['price_per_ton'])
            year = int(data.get('year', datetime.now().year))
        except (ValueError, TypeError) as e:
            return jsonify({
                "status": "error",
                "message": f"Invalid request data: {e}"
            }), 400
        
        # Build enhanced quote request
        quote_request = {
            'latitude': latitude,
            'longitude': longitude, 
            'area_ha': area_ha,
            'crop': crop,
            'expected_yield': expected_yield,
            'price_per_ton': price_per_ton,
            'year': year,
            'loadings': data.get('loadings', {}),
            'zone': data.get('zone', 'auto_detect'),
            'deductible_rate': data.get('deductible_rate', 0.05),
            'deductible_threshold': data.get('deductible_threshold', 0.0),
            'buffer_radius': data.get('buffer_radius', 1500),
            'field_info': {
                'type': 'field',
                'field_id': field_id,
                'name': field_data.get('name', f'Field {field_id}'),
                'farmer_name': field_data.get('farmer_name'),
                'area_ha': area_ha,
                'coordinates': f"{latitude:.6f}, {longitude:.6f}"
            }
        }
        
        # Execute quote
        quote_result = quote_engine.execute_quote(quote_request)
        quote_result['field_info'] = quote_request['field_info']
        
        # Generate comprehensive report
        try:
            comprehensive_report = enhanced_ai_generator.generate_comprehensive_quote_report(
                quote_result, quote_request['field_info']
            )
            quote_result['comprehensive_report'] = comprehensive_report
            
            # Keep backward compatibility
            quote_result['ai_summary'] = comprehensive_report.get('executive_summary', 'Summary unavailable')
        except Exception as e:
            print(f"Enhanced report generation failed: {e}")
            quote_result['ai_summary'] = "Enhanced report temporarily unavailable"
            quote_result['comprehensive_report'] = {"error": "Report generation failed"}
        
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
            "version": "2.0.0-Enhanced",
            "report_type": "comprehensive"
        })
        
    except Exception as e:
        print(f"=== ENHANCED FIELD QUOTE ERROR FOR FIELD {field_id} ===")
        print(f"Error: {e}")
        print(f"Full traceback: {traceback.format_exc()}")
        
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__,
            "field_id": field_id
        }), 500

@quotes_bp.route('/bulk', methods=['POST'])
def bulk_quote():
    """Generate enhanced bulk quotes with portfolio analysis"""
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
                        latitude = field_data['latitude']
                        longitude = field_data['longitude']
                        area_ha = field_data.get('area_ha')
                        
                        quote_request.update({
                            'latitude': latitude,
                            'longitude': longitude,
                            'area_ha': area_ha,
                            'crop': field_data.get('crop', quote_request.get('crop', 'maize')),
                            'field_info': {
                                'type': 'field',
                                'field_id': req['field_id'],
                                'name': field_data.get('name', f'Field {req["field_id"]}'),
                                'farmer_name': field_data.get('farmer_name')
                            }
                        })
                    else:
                        results.append({
                            "request_index": i,
                            "status": "error",
                            "message": f"Field {req['field_id']} not found or has invalid data"
                        })
                        continue
                
                # Execute quote
                quote_result = quote_engine.execute_quote(quote_request)
                successful_quotes.append(quote_result)
                
                # Generate individual comprehensive reports for bulk
                try:
                    comprehensive_report = enhanced_ai_generator.generate_comprehensive_quote_report(
                        quote_result, quote_request.get('field_info')
                    )
                    quote_result['comprehensive_report'] = comprehensive_report
                    quote_result['ai_summary'] = comprehensive_report.get('executive_summary', 'Summary unavailable')
                except Exception as e:
                    print(f"Enhanced report generation failed for bulk item {i}: {e}")
                    quote_result['ai_summary'] = "Enhanced report temporarily unavailable"
                
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
        
        # Generate enhanced bulk portfolio analysis
        portfolio_analysis = {}
        if successful_quotes:
            try:
                portfolio_analysis = enhanced_ai_generator.generate_bulk_summary(successful_quotes)
            except Exception as e:
                print(f"Portfolio analysis generation failed: {e}")
                portfolio_analysis = "Portfolio analysis temporarily unavailable"
        
        execution_time = time.time() - start_time
        successful_count = sum(1 for r in results if r['status'] == 'success')
        
        return jsonify({
            "status": "success",
            "portfolio_analysis": portfolio_analysis,
            "total_requests": len(requests),
            "successful_quotes": successful_count,
            "failed_quotes": len(requests) - successful_count,
            "results": results,
            "execution_time_seconds": round(execution_time, 2),
            "version": "2.0.0-Enhanced",
            "report_type": "comprehensive_bulk"
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
    """Get quote by ID with enhanced display"""
    try:
        quote = quotes_repo.get_quote_by_id(quote_id)
        
        if not quote:
            return jsonify({
                "status": "error",
                "message": f"Quote {quote_id} not found"
            }), 404
        
        # Add enhanced viewing options
        view_mode = request.args.get('view', 'standard')
        
        response_data = {
            "status": "success",
            "quote": quote,
            "view_mode": view_mode
        }
        
        # Add summary statistics if requested
        if view_mode == 'summary':
            if quote.get('quote_data') and isinstance(quote['quote_data'], dict):
                quote_data = quote['quote_data']
                response_data['summary_stats'] = {
                    "sum_insured": quote_data.get('sum_insured', 0),
                    "premium_rate": f"{quote_data.get('premium_rate', 0) * 100:.2f}%",
                    "expected_payout": quote_data.get('expected_payout_ratio', 0) * 100,
                    "phases_covered": len(quote_data.get('phase_breakdown', [])),
                    "quote_type": quote_data.get('quote_type', 'unknown')
                }
        
        return jsonify(response_data)
        
    except Exception as e:
        print(f"Get quote error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@quotes_bp.route('/field/<int:field_id>/history', methods=['GET'])
def get_field_quote_history(field_id):
    """Get enhanced quote history for a field"""
    try:
        limit = request.args.get('limit', 20, type=int)
        quotes = quotes_repo.get_quotes_by_field(field_id, limit)
        
        # Add trend analysis if multiple quotes
        trend_analysis = {}
        if len(quotes) > 1:
            try:
                premium_rates = [q.get('premium_rate', 0) for q in quotes if q.get('premium_rate')]
                if premium_rates:
                    trend_analysis = {
                        "average_premium_rate": f"{sum(premium_rates) / len(premium_rates) * 100:.2f}%",
                        "rate_trend": "increasing" if premium_rates[-1] > premium_rates[0] else "decreasing",
                        "quote_frequency": f"{len(quotes)} quotes generated",
                        "latest_quote_date": quotes[0].get('created_at') if quotes else None
                    }
            except Exception as e:
                print(f"Trend analysis error: {e}")
        
        return jsonify({
            "status": "success",
            "field_id": field_id,
            "quotes": quotes,
            "total": len(quotes),
            "trend_analysis": trend_analysis
        })
        
    except Exception as e:
        print(f"Get field quotes error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@quotes_bp.route('/validate', methods=['POST'])
def validate_quote_request():
    """Enhanced validation with detailed feedback"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                "status": "error",
                "message": "Request body is required"
            }), 400
        
        validation_errors = []
        validation_warnings = []
        
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
        
        # Enhanced numeric validation
        numeric_fields = ['expected_yield', 'price_per_ton', 'area_ha', 'year', 'latitude', 'longitude']
        for field in numeric_fields:
            if field in data and data[field] is not None:
                try:
                    value = float(data[field])
                    if field in ['expected_yield', 'price_per_ton', 'area_ha'] and value <= 0:
                        validation_errors.append(f"Field '{field}' must be positive")
                    elif field == 'expected_yield' and value > 20:
                        validation_warnings.append(f"Expected yield of {value} tons/ha seems high - please verify")
                    elif field == 'price_per_ton' and value > 2000:
                        validation_warnings.append(f"Price of ${value}/ton seems high - please verify")
                except (ValueError, TypeError):
                    validation_errors.append(f"Field '{field}' must be a number")
        
        # Validate year
        if 'year' in data:
            year = data['year']
            current_year = datetime.now().year
            if year < 2015 or year > current_year + 5:
                validation_warnings.append(f"Year {year} is outside typical range (2015-{current_year + 5})")
        
        if validation_errors:
            return jsonify({
                "status": "error",
                "validation_errors": validation_errors,
                "validation_warnings": validation_warnings
            }), 400
        
        # Enhanced response with recommendations
        response = {
            "status": "success",
            "message": "Quote request is valid",
            "estimated_quote_type": quote_engine._determine_quote_type(data.get('year', datetime.now().year)),
            "validation_warnings": validation_warnings
        }
        
        # Add recommendations
        recommendations = []
        if 'area_ha' not in data:
            recommendations.append("Consider adding 'area_ha' for more accurate sum insured calculation")
        if 'zone' not in data:
            recommendations.append("Specify 'zone' for location-specific risk adjustments")
        if 'loadings' not in data:
            recommendations.append("Add 'loadings' for complete premium calculation")
        
        if recommendations:
            response['recommendations'] = recommendations
        
        return jsonify(response)
        
    except Exception as e:
        print(f"Validation error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

# New endpoint for report generation
@quotes_bp.route('/report/<quote_id>', methods=['GET'])
def generate_detailed_report(quote_id):
    """Generate standalone detailed report for a quote"""
    try:
        quote = quotes_repo.get_quote_by_id(quote_id)
        
        if not quote:
            return jsonify({
                "status": "error",
                "message": f"Quote {quote_id} not found"
            }), 404
        
        # Extract quote data
        quote_data = quote.get('quote_data', {})
        if isinstance(quote_data, str):
            import json
            quote_data = json.loads(quote_data)
        
        # Generate comprehensive report
        try:
            comprehensive_report = enhanced_ai_generator.generate_comprehensive_quote_report(
                quote_data, quote_data.get('field_info')
            )
            
            return jsonify({
                "status": "success",
                "quote_id": quote_id,
                "report": comprehensive_report,
                "generated_at": datetime.utcnow().isoformat(),
                "report_version": "2.0.0-Enhanced"
            })
            
        except Exception as e:
            print(f"Report generation failed: {e}")
            return jsonify({
                "status": "error",
                "message": "Report generation failed",
                "error": str(e)
            }), 500
        
    except Exception as e:
        print(f"Detailed report error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

# Testing endpoint for enhanced features
@quotes_bp.route('/test/enhanced', methods=['POST'])
def test_enhanced_features():
    """Test endpoint for enhanced reporting features"""
    try:
        data = request.get_json() or {}
        
        # Default test quote data
        test_quote_data = {
            "crop": data.get("crop", "maize"),
            "year": data.get("year", 2024),
            "sum_insured": data.get("sum_insured", 5000),
            "gross_premium": data.get("gross_premium", 375),
            "premium_rate": data.get("premium_rate", 0.075),
            "expected_payout_ratio": data.get("expected_payout_ratio", 0.15),
            "zone": "aez_3_midlands",
            "zone_name": "AEZ 3 (Midlands)",
            "quote_type": "prospective",
            "phase_breakdown": [
                {
                    "phase_name": "Emergence",
                    "phase_number": 1,
                    "start_day": 0,
                    "end_day": 14,
                    "trigger_mm": 25,
                    "exit_mm": 5,
                    "phase_weight": 0.15,
                    "water_need_mm": 30
                },
                {
                    "phase_name": "Vegetative",
                    "phase_number": 2,
                    "start_day": 15,
                    "end_day": 49,
                    "trigger_mm": 60,
                    "exit_mm": 15,
                    "phase_weight": 0.25,
                    "water_need_mm": 80
                },
                {
                    "phase_name": "Flowering",
                    "phase_number": 3,
                    "start_day": 50,
                    "end_day": 84,
                    "trigger_mm": 80,
                    "exit_mm": 20,
                    "phase_weight": 0.40,
                    "water_need_mm": 100
                },
                {
                    "phase_name": "Grain Fill",
                    "phase_number": 4,
                    "start_day": 85,
                    "end_day": 120,
                    "trigger_mm": 70,
                    "exit_mm": 10,
                    "phase_weight": 0.20,
                    "water_need_mm": 90
                }
            ],
            "historical_years_used": list(range(2018, 2024)),
            "deductible_rate": 0.10,
            "deductible_amount": 500,
            "loadings": {
                "administration": 25,
                "commission": 50,
                "profit": 75
            }
        }
        
        # Generate comprehensive report
        comprehensive_report = enhanced_ai_generator.generate_comprehensive_quote_report(
            test_quote_data, data.get('field_info')
        )
        
        return jsonify({
            "status": "success",
            "message": "Enhanced features test successful",
            "test_quote_data": test_quote_data,
            "comprehensive_report": comprehensive_report,
            "features_tested": [
                "Comprehensive report generation",
                "Enhanced phase breakdown",
                "Financial summary",
                "Technical specifications",
                "Claims procedure",
                "Terms and conditions"
            ]
        })
        
    except Exception as e:
        print(f"Enhanced features test error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }), 500
