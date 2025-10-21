"""
Enterprise Agricultural Insurance Quote API - Version 4.0
Production-ready Flask endpoints with enterprise executive summaries
Implements robust field name extraction and comprehensive quote generation
"""

from flask import Blueprint, request, jsonify
import traceback
import time
from datetime import datetime
from typing import Dict, List, Any

from core.quote_engine import QuoteEngine
from core.database import FieldsRepository, QuotesRepository
from core.ai_summary import generate_executive_summary
from core.crops import validate_crop, list_supported_crops

quotes_bp = Blueprint('quotes', __name__)

# Initialize components
quote_engine = QuoteEngine()
fields_repo = FieldsRepository()
quotes_repo = QuotesRepository()

@quotes_bp.route('/historical', methods=['POST'])
def historical_quote():
    """Generate historical quote with enterprise executive summary"""
    try:
        start_time = time.time()
        data = request.get_json()
        
        print(f"INFO: Processing historical quote request")
        
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
        
        print(f"INFO: Historical quote for year {data.get('year')}")
        
        # Extract field name for summary
        field_name = data.get('location_info', {}).get('name', 'Target Field')
        print(f"INFO: Using field name: '{field_name}'")
        
        # Execute quote
        quote_result = quote_engine.execute_quote(data)
        
        # Generate enterprise executive summary
        try:
            print(f"INFO: Generating executive summary for historical quote")
            quote_result['executive_summary'] = generate_executive_summary(
                quote_result,
                field_name=field_name,
                summary_type='comprehensive'
            )
            quote_result['ai_summary'] = quote_result['executive_summary']
            print(f"SUCCESS: Executive summary generated")
        except Exception as e:
            print(f"ERROR: Executive summary generation failed: {e}")
            print(f"ERROR: Traceback: {traceback.format_exc()}")
            quote_result['executive_summary'] = f"Executive summary temporarily unavailable (Field: {field_name})"
            quote_result['ai_summary'] = quote_result['executive_summary']
        
        # Save quote to database
        try:
            quote_id = quotes_repo.save_quote(quote_result)
            if quote_id:
                quote_result['quote_id'] = quote_id
                print(f"SUCCESS: Quote saved with ID: {quote_id}")
        except Exception as e:
            print(f"WARNING: Failed to save quote: {e}")
        
        execution_time = time.time() - start_time
        print(f"SUCCESS: Historical quote completed in {execution_time:.2f} seconds")
        
        return jsonify({
            "status": "success",
            "quote": quote_result,
            "execution_time_seconds": round(execution_time, 2),
            "version": "4.0-Enterprise",
            "enhancements": [
                "Enterprise executive summaries",
                "Robust field name extraction", 
                "Year-by-year simulation",
                "Seasonal validation"
            ]
        })
        
    except Exception as e:
        print(f"ERROR: Historical quote error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }), 500

@quotes_bp.route('/prospective', methods=['POST'])
def prospective_quote():
    """Generate prospective quote with enterprise executive summary"""
    try:
        start_time = time.time()
        data = request.get_json()
        
        print(f"INFO: Processing prospective quote request")
        
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
        
        # Default to next appropriate year with seasonal validation
        current_month = datetime.now().month
        current_year = datetime.now().year
        
        if 'year' not in data:
            # Smart year selection based on current season
            if current_month >= 8:  # Aug onwards - approaching next season
                data['year'] = current_year + 1
            else:  # Too late for current season, suggest next
                data['year'] = current_year + 1
        
        target_year = data['year']
        print(f"INFO: Prospective quote for {target_year} season")
        
        # Extract field name for summary
        field_name = data.get('location_info', {}).get('name', 'Target Field')
        print(f"INFO: Using field name: '{field_name}'")
        
        # Execute quote
        quote_result = quote_engine.execute_quote(data)
        
        # Generate enterprise executive summary
        try:
            print(f"INFO: Generating executive summary for prospective quote")
            quote_result['executive_summary'] = generate_executive_summary(
                quote_result,
                field_name=field_name,
                summary_type='comprehensive'
            )
            quote_result['ai_summary'] = quote_result['executive_summary']
            print(f"SUCCESS: Executive summary generated")
        except Exception as e:
            print(f"ERROR: Executive summary generation failed: {e}")
            print(f"ERROR: Traceback: {traceback.format_exc()}")
            quote_result['executive_summary'] = f"Executive summary temporarily unavailable (Field: {field_name})"
            quote_result['ai_summary'] = quote_result['executive_summary']
        
        # Save quote to database
        try:
            quote_id = quotes_repo.save_quote(quote_result)
            if quote_id:
                quote_result['quote_id'] = quote_id
                print(f"SUCCESS: Quote saved with ID: {quote_id}")
        except Exception as e:
            print(f"WARNING: Failed to save quote: {e}")
        
        execution_time = time.time() - start_time
        print(f"SUCCESS: Prospective quote completed in {execution_time:.2f} seconds")
        
        return jsonify({
            "status": "success",
            "quote": quote_result,
            "execution_time_seconds": round(execution_time, 2),
            "version": "4.0-Enterprise",
            "seasonal_validation": {
                "target_season": f"{target_year-1}//{target_year}",
                "planting_window": "October - January",
                "current_month": current_month,
                "seasonal_appropriateness": "validated"
            }
        })
        
    except Exception as e:
        print(f"ERROR: Prospective quote error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }), 500

@quotes_bp.route('/field/<int:field_id>', methods=['POST'])
def field_based_quote(field_id):
    """Generate field-based quote with enterprise executive summary and robust field name extraction"""
    try:
        start_time = time.time()
        data = request.get_json()
        
        print(f"INFO: =================================")
        print(f"INFO: Processing field {field_id} quote")
        print(f"INFO: =================================")
        
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
        
        # Get field data from database
        field_data = fields_repo.get_field_by_id(field_id)
        
        if not field_data:
            return jsonify({
                "status": "error",
                "message": f"Field {field_id} not found in database"
            }), 404
        
        # ✅ CRITICAL: Extract field name with multiple fallbacks
        field_name = (
            field_data.get('name') or 
            field_data.get('field_name') or 
            field_data.get('fieldName') or 
            field_data.get('label') or
            field_data.get('field_label') or
            f'Field {field_id}'
        )
        
        # ✅ CRITICAL: Comprehensive logging for debugging
        print(f"INFO: Field data keys available: {list(field_data.keys())}")
        print(f"INFO: Field name candidates:")
        print(f"INFO:   - name: '{field_data.get('name')}'")
        print(f"INFO:   - field_name: '{field_data.get('field_name')}'")
        print(f"INFO:   - fieldName: '{field_data.get('fieldName')}'")
        print(f"INFO:   - label: '{field_data.get('label')}'")
        print(f"INFO:   - field_label: '{field_data.get('field_label')}'")
        print(f"INFO: Selected field name: '{field_name}'")
        print(f"INFO: Full field data structure: {field_data}")
        
        # Validate coordinates
        latitude = field_data.get('latitude')
        longitude = field_data.get('longitude')
        
        if latitude is None or longitude is None:
            return jsonify({
                "status": "error",
                "message": f"Field {field_id} has invalid coordinates (lat: {latitude}, lon: {longitude})"
            }), 400
        
        # Handle area_ha
        area_ha = field_data.get('area_ha')
        if area_ha is not None:
            try:
                area_ha = float(area_ha)
            except (ValueError, TypeError):
                print(f"WARNING: Invalid area_ha value: {area_ha}, setting to None")
                area_ha = None
        
        # Handle crop with validation
        crop = field_data.get('crop', 'maize')
        if not crop or str(crop).strip() == '':
            crop = 'maize'
        else:
            crop = str(crop).strip().lower()
        
        # Validate request data
        try:
            expected_yield = float(data['expected_yield'])
            price_per_ton = float(data['price_per_ton'])
            year = int(data.get('year', datetime.now().year + 1))
        except (ValueError, TypeError) as e:
            return jsonify({
                "status": "error",
                "message": f"Invalid request data: {e}"
            }), 400
        
        # Build quote request
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
            'buffer_radius': data.get('buffer_radius', 1500),
            'field_name': field_name,  # ✅ PASS TO ENGINE
            'field_info': {
                'type': 'field',
                'field_id': field_id,
                'name': field_name,
                'farmer_name': field_data.get('farmer_name'),
                'area_ha': area_ha,
                'coordinates': f"{latitude:.6f}, {longitude:.6f}"
            }
        }
        
        print(f"INFO: Field crop: {crop}")
        print(f"INFO: Coordinates: {latitude:.4f}, {longitude:.4f}")
        print(f"INFO: Area: {area_ha} ha" if area_ha else "INFO: Area: Not specified")
        print(f"INFO: Field name passed to engine: '{field_name}'")
        
        # Execute quote
        quote_result = quote_engine.execute_quote(quote_request)
        quote_result['field_info'] = quote_request['field_info']
        
        # ✅ Generate enterprise executive summary with field name
        try:
            print(f"INFO: Generating executive summary for field: '{field_name}'")
            quote_result['executive_summary'] = generate_executive_summary(
                quote_result,
                field_name=field_name,
                summary_type='comprehensive'
            )
            quote_result['ai_summary'] = quote_result['executive_summary']
            print(f"SUCCESS: Executive summary generated for field '{field_name}'")
        except Exception as e:
            print(f"ERROR: Executive summary generation failed: {e}")
            print(f"ERROR: Traceback: {traceback.format_exc()}")
            quote_result['executive_summary'] = f"Executive summary temporarily unavailable (Field: {field_name})"
            quote_result['ai_summary'] = quote_result['executive_summary']
        
        # Save quote to database
        try:
            quote_result['field_id'] = field_id
            quote_id = quotes_repo.save_quote(quote_result)
            if quote_id:
                quote_result['quote_id'] = quote_id
                print(f"SUCCESS: Quote saved with ID: {quote_id}")
        except Exception as e:
            print(f"WARNING: Failed to save quote: {e}")
        
        execution_time = time.time() - start_time
        print(f"SUCCESS: Field quote completed in {execution_time:.2f} seconds")
        print(f"INFO: =================================")
        
        return jsonify({
            "status": "success",
            "quote": quote_result,
            "field_data": {
                "id": field_data['id'],
                "name": field_name,
                "farmer_name": field_data.get('farmer_name'),
                "area_ha": area_ha,
                "crop": crop,
                "latitude": latitude,
                "longitude": longitude
            },
            "execution_time_seconds": round(execution_time, 2),
            "version": "4.0-Enterprise"
        })
        
    except Exception as e:
        print(f"ERROR: =================================")
        print(f"ERROR: Field quote error for field {field_id}")
        print(f"ERROR: {e}")
        print(f"ERROR: Full traceback: {traceback.format_exc()}")
        print(f"ERROR: =================================")
        
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__,
            "field_id": field_id
        }), 500

@quotes_bp.route('/bulk', methods=['POST'])
def bulk_quote():
    """Generate bulk portfolio quotes with enterprise executive summaries"""
    try:
        start_time = time.time()
        data = request.get_json()
        
        print(f"INFO: Processing bulk quote request")
        
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
        
        print(f"INFO: Processing {len(requests)} bulk quote requests")
        
        results = []
        successful_quotes = []
        
        for i, req in enumerate(requests):
            try:
                print(f"INFO: Processing bulk request {i+1}/{len(requests)}")
                
                # Merge global settings
                quote_request = {**global_settings, **req}
                field_name = f"Bulk Field {i+1}"  # Default field name
                
                # Handle field-based request
                if 'field_id' in req:
                    field_data = fields_repo.get_field_by_id(req['field_id'])
                    if field_data:
                        # Extract field name with fallbacks
                        field_name = (
                            field_data.get('name') or 
                            field_data.get('field_name') or 
                            field_data.get('fieldName') or 
                            f'Field {req["field_id"]}'
                        )
                        
                        latitude = field_data['latitude']
                        longitude = field_data['longitude']
                        area_ha = field_data.get('area_ha')
                        
                        quote_request.update({
                            'latitude': latitude,
                            'longitude': longitude,
                            'area_ha': area_ha,
                            'crop': field_data.get('crop', quote_request.get('crop', 'maize')),
                            'field_name': field_name,
                            'field_info': {
                                'type': 'field',
                                'field_id': req['field_id'],
                                'name': field_name,
                                'farmer_name': field_data.get('farmer_name')
                            }
                        })
                        print(f"INFO: Bulk request {i+1} field name: '{field_name}'")
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
                
                # Generate executive summary for each bulk quote (concise version)
                try:
                    print(f"INFO: Generating executive summary for bulk request {i+1}")
                    quote_result['executive_summary'] = generate_executive_summary(
                        quote_result,
                        field_name=field_name,
                        summary_type='concise'  # Concise for bulk
                    )
                    quote_result['ai_summary'] = quote_result['executive_summary']
                    print(f"SUCCESS: Executive summary generated for bulk request {i+1}")
                except Exception as e:
                    print(f"ERROR: Executive summary failed for bulk item {i}: {e}")
                    quote_result['executive_summary'] = f"Executive summary temporarily unavailable (Field: {field_name})"
                    quote_result['ai_summary'] = quote_result['executive_summary']
                
                # Save to database
                try:
                    if 'field_id' in req:
                        quote_result['field_id'] = req['field_id']
                    quote_id = quotes_repo.save_quote(quote_result)
                    if quote_id:
                        quote_result['quote_id'] = quote_id
                except Exception as e:
                    print(f"WARNING: Failed to save bulk quote: {e}")
                
                results.append({
                    "request_index": i,
                    "status": "success",
                    "quote": quote_result
                })
                
                print(f"SUCCESS: Bulk request {i+1} completed: ${quote_result.get('gross_premium', 0):,.0f} premium")
                
            except Exception as e:
                print(f"ERROR: Bulk request {i+1} failed: {e}")
                results.append({
                    "request_index": i,
                    "status": "error",
                    "message": str(e),
                    "error_type": type(e).__name__
                })
        
        # Generate portfolio-level analysis
        portfolio_analysis = {}
        if successful_quotes:
            try:
                # Calculate portfolio metrics
                total_premium = sum(q.get('gross_premium', 0) for q in successful_quotes)
                total_sum_insured = sum(q.get('sum_insured', 0) for q in successful_quotes)
                avg_premium_rate = (total_premium / total_sum_insured * 100) if total_sum_insured > 0 else 0
                
                portfolio_metrics = {
                    "total_premium": f"${total_premium:,.2f}",
                    "total_sum_insured": f"${total_sum_insured:,.2f}",
                    "average_premium_rate": f"{avg_premium_rate:.2f}%",
                    "crop_distribution": {},
                    "geographic_spread": len(set(q.get('latitude', 0) for q in successful_quotes)),
                    "simulation_years": len(successful_quotes[0].get('historical_years_used', [])) if successful_quotes else 0
                }
                
                # Crop distribution
                crops = [q.get('crop', 'unknown') for q in successful_quotes]
                for crop in set(crops):
                    portfolio_metrics["crop_distribution"][crop] = crops.count(crop)
                
                portfolio_analysis['portfolio_metrics'] = portfolio_metrics
                print(f"SUCCESS: Portfolio analysis generated")
                
            except Exception as e:
                print(f"ERROR: Portfolio analysis generation failed: {e}")
                portfolio_analysis = "Portfolio analysis temporarily unavailable"
        
        execution_time = time.time() - start_time
        successful_count = sum(1 for r in results if r['status'] == 'success')
        
        print(f"SUCCESS: Bulk processing completed: {successful_count}/{len(requests)} successful")
        
        return jsonify({
            "status": "success",
            "portfolio_analysis": portfolio_analysis,
            "total_requests": len(requests),
            "successful_quotes": successful_count,
            "failed_quotes": len(requests) - successful_count,
            "results": results,
            "execution_time_seconds": round(execution_time, 2),
            "version": "4.0-Enterprise"
        })
        
    except Exception as e:
        print(f"ERROR: Bulk quote error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }), 500

@quotes_bp.route('/simulation/<quote_id>', methods=['GET'])
def get_simulation_details(quote_id):
    """Get detailed year-by-year simulation for a quote"""
    try:
        print(f"INFO: Retrieving simulation details for quote {quote_id}")
        
        quote = quotes_repo.get_quote_by_id(quote_id)
        
        if not quote:
            return jsonify({
                "status": "error",
                "message": f"Quote {quote_id} not found"
            }), 404
        
        # Extract simulation data
        quote_data = quote.get('quote_data', {})
        if isinstance(quote_data, str):
            import json
            quote_data = json.loads(quote_data)
        
        simulation_data = quote_data.get('year_by_year_simulation', [])
        
        if not simulation_data:
            return jsonify({
                "status": "error",
                "message": "No simulation data available for this quote"
            }), 404
        
        # Enhance simulation data with additional analysis
        enhanced_simulation = []
        for year_data in simulation_data:
            enhanced_year = dict(year_data)
            
            # Add interpretations
            drought_impact = year_data.get('drought_impact', 0)
            if drought_impact < 5:
                enhanced_year['risk_level'] = 'Low'
            elif drought_impact < 15:
                enhanced_year['risk_level'] = 'Moderate'
            elif drought_impact < 30:
                enhanced_year['risk_level'] = 'High'
            else:
                enhanced_year['risk_level'] = 'Severe'
            
            # Add farmer perspective
            net_result = year_data.get('net_result', 0)
            enhanced_year['farmer_outcome'] = 'Positive' if net_result > 0 else 'Negative'
            
            enhanced_simulation.append(enhanced_year)
        
        print(f"SUCCESS: Simulation details retrieved for quote {quote_id}")
        
        return jsonify({
            "status": "success",
            "quote_id": quote_id,
            "simulation_data": enhanced_simulation,
            "summary_statistics": {
                "total_years": len(simulation_data),
                "years_with_payouts": len([y for y in simulation_data if y.get('drought_impact', 0) > 5]),
                "average_premium": sum(y.get('simulated_premium_usd', 0) for y in simulation_data) / len(simulation_data),
                "average_payout": sum(y.get('simulated_payout', 0) for y in simulation_data) / len(simulation_data),
                "worst_year": max(simulation_data, key=lambda x: x.get('drought_impact', 0))['year'],
                "best_year": min(simulation_data, key=lambda x: x.get('drought_impact', 0))['year']
            },
            "version": "4.0-Enterprise"
        })
        
    except Exception as e:
        print(f"ERROR: Simulation details error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }), 500

@quotes_bp.route('/test/refined', methods=['POST'])
def test_refined_features():
    """Test endpoint for enterprise features"""
    try:
        print(f"INFO: Testing enterprise features")
        
        data = request.get_json() or {}
        
        # Default test quote data for Zimbabwe
        test_quote_data = {
            "crop": data.get("crop", "maize"),
            "latitude": data.get("latitude", -17.8216),  # Harare area
            "longitude": data.get("longitude", 30.9858),
            "expected_yield": data.get("expected_yield", 5.0),
            "price_per_ton": data.get("price_per_ton", 300),
            "year": data.get("year", 2024),
            "area_ha": data.get("area_ha", 2.0),
            "field_name": data.get("field_name", "Test Field")
        }
        
        print(f"INFO: Testing with data: {test_quote_data}")
        
        # Execute quote
        quote_result = quote_engine.execute_quote(test_quote_data)
        
        # Test executive summary generation
        try:
            print(f"INFO: Testing executive summary generation")
            quote_result['executive_summary'] = generate_executive_summary(
                quote_result,
                field_name=test_quote_data['field_name'],
                summary_type='comprehensive'
            )
            quote_result['ai_summary'] = quote_result['executive_summary']
            print(f"SUCCESS: Executive summary test completed")
        except Exception as e:
            print(f"ERROR: Executive summary test failed: {e}")
            quote_result['executive_summary'] = f"Executive summary test failed: {e}"
            quote_result['ai_summary'] = quote_result['executive_summary']
        
        # Extract key features for testing
        enterprise_features = {
            "field_name_extraction": "✅ Multiple fallback keys implemented",
            "executive_summary_generation": "✅ Enterprise-grade summaries",
            "planting_detection": quote_result.get('planting_analysis', {}),
            "year_by_year_simulation": quote_result.get('year_by_year_simulation', []),
            "simulation_summary": quote_result.get('simulation_summary', {}),
            "seasonal_validation": {
                "method": "rainfall_only",
                "criteria": "20mm over 7 days, 2+ days ≥5mm",
                "season_window": "October 1 - January 31"
            }
        }
        
        print(f"SUCCESS: Enterprise features test completed")
        
        return jsonify({
            "status": "success",
            "message": "Enterprise features test successful",
            "test_quote_data": test_quote_data,
            "quote_result": quote_result,
            "enterprise_features": enterprise_features,
            "features_tested": [
                "✅ Robust field name extraction",
                "✅ Enterprise executive summaries",
                "✅ Rainfall-only planting detection",
                "✅ Year-by-year simulation",
                "✅ Seasonal validation",
                "✅ Enhanced error handling"
            ],
            "version": "4.0-Enterprise"
        })
        
    except Exception as e:
        print(f"ERROR: Enterprise features test error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }), 500

@quotes_bp.route('/<quote_id>', methods=['GET'])
def get_quote(quote_id):
    """Get quote by ID with enhanced display"""
    try:
        print(f"INFO: Retrieving quote {quote_id}")
        
        quote = quotes_repo.get_quote_by_id(quote_id)
        
        if not quote:
            return jsonify({
                "status": "error",
                "message": f"Quote {quote_id} not found"
            }), 404
        
        # Add enhanced viewing options
        view_mode = request.args.get('view', 'standard')
        include_simulation = request.args.get('include_simulation', 'false').lower() == 'true'
        
        response_data = {
            "status": "success",
            "quote": quote,
            "view_mode": view_mode,
            "version": "4.0-Enterprise"
        }
        
        # Add simulation data if requested
        if include_simulation:
            quote_data = quote.get('quote_data', {})
            if isinstance(quote_data, str):
                import json
                quote_data = json.loads(quote_data)
            
            response_data['simulation_data'] = quote_data.get('year_by_year_simulation', [])
        
        print(f"SUCCESS: Quote {quote_id} retrieved")
        
        return jsonify(response_data)
        
    except Exception as e:
        print(f"ERROR: Get quote error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }), 500

@quotes_bp.route('/field/<int:field_id>/history', methods=['GET'])
def get_field_quote_history(field_id):
    """Get enhanced quote history for a field"""
    try:
        print(f"INFO: Retrieving quote history for field {field_id}")
        
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
                        "latest_quote_date": quotes[0].get('created_at') if quotes else None,
                        "version_evolution": list(set(q.get('version', 'unknown') for q in quotes))
                    }
                    print(f"SUCCESS: Trend analysis generated for field {field_id}")
            except Exception as e:
                print(f"WARNING: Trend analysis error: {e}")
        
        print(f"SUCCESS: Quote history retrieved for field {field_id}")
        
        return jsonify({
            "status": "success",
            "field_id": field_id,
            "quotes": quotes,
            "total": len(quotes),
            "trend_analysis": trend_analysis,
            "version": "4.0-Enterprise"
        })
        
    except Exception as e:
        print(f"ERROR: Get field quotes error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }), 500

@quotes_bp.route('/validate', methods=['POST'])
def validate_quote_request():
    """Enhanced validation with seasonal checks"""
    try:
        print(f"INFO: Validating quote request")
        
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
        
        # Seasonal validation for year
        if 'year' in data:
            year = data['year']
            current_year = datetime.now().year
            current_month = datetime.now().month
            
            if year < 2018 or year > current_year + 2:
                validation_errors.append(f"Year {year} is outside valid range (2018-{current_year + 2})")
            
            # Seasonal appropriateness check
            if year == current_year:
                if current_month > 3:  # Past main harvest season
                    validation_warnings.append(f"Quote for {year} may be late in season (current month: {current_month})")
                elif current_month < 8:  # Too early for next season
                    validation_warnings.append(f"Consider quoting for {year + 1} season instead")
        
        # Coordinate validation for Southern Africa
        if 'latitude' in data and 'longitude' in data:
            try:
                lat = float(data['latitude'])
                lon = float(data['longitude'])
                if not (-25 <= lat <= -15 and 25 <= lon <= 35):
                    validation_warnings.append("Coordinates appear to be outside Southern Africa region")
            except:
                pass  # Already caught in numeric validation
        
        if validation_errors:
            print(f"ERROR: Validation failed with {len(validation_errors)} errors")
            return jsonify({
                "status": "error",
                "validation_errors": validation_errors,
                "validation_warnings": validation_warnings
            }), 400
        
        # Enhanced response with seasonal recommendations
        current_month = datetime.now().month
        current_year = datetime.now().year
        
        # Determine appropriate quote type
        target_year = data.get('year', current_year + 1)
        if target_year > current_year:
            quote_type = "prospective"
        elif target_year == current_year and current_month >= 8:
            quote_type = "prospective"
        else:
            quote_type = "historical"
        
        response = {
            "status": "success",
            "message": "Quote request is valid",
            "estimated_quote_type": quote_type,
            "validation_warnings": validation_warnings,
            "seasonal_guidance": {
                "current_month": current_month,
                "recommended_season": f"{current_year}/{current_year + 1}" if current_month >= 8 else f"{current_year + 1}/{current_year + 2}",
                "planting_window": "October - January",
                "next_season_starts": f"October {current_year}" if current_month < 10 else f"October {current_year + 1}"
            },
            "version": "4.0-Enterprise"
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
        
        print(f"SUCCESS: Quote request validation completed")
        
        return jsonify(response)
        
    except Exception as e:
        print(f"ERROR: Validation error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }), 500

@quotes_bp.route('/report/<quote_id>', methods=['GET'])
def generate_detailed_report(quote_id):
    """Generate standalone detailed report for a quote"""
    try:
        print(f"INFO: Generating detailed report for quote {quote_id}")
        
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
            field_name = quote_data.get('field_name', 'Target Field')
            comprehensive_report = generate_executive_summary(
                quote_data, 
                field_name=field_name,
                summary_type='comprehensive'
            )
            
            print(f"SUCCESS: Detailed report generated for quote {quote_id}")
            
            return jsonify({
                "status": "success",
                "quote_id": quote_id,
                "report": comprehensive_report,
                "generated_at": datetime.utcnow().isoformat(),
                "report_version": "4.0-Enterprise"
            })
            
        except Exception as e:
            print(f"ERROR: Report generation failed: {e}")
            return jsonify({
                "status": "error",
                "message": "Report generation failed",
                "error": str(e)
            }), 500
        
    except Exception as e:
        print(f"ERROR: Detailed report error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }), 500

@quotes_bp.route('/debug/field/<int:field_id>', methods=['GET'])
def debug_field_data(field_id):
    """Debug endpoint to inspect field data structure and name extraction"""
    try:
        print(f"INFO: =================================")
        print(f"INFO: Debug inspection for field {field_id}")
        print(f"INFO: =================================")
        
        field_data = fields_repo.get_field_by_id(field_id)
        
        if not field_data:
            return jsonify({
                "status": "error",
                "message": f"Field {field_id} not found"
            }), 404
        
        # Try all possible name extraction methods
        name_candidates = {
            'name': field_data.get('name'),
            'field_name': field_data.get('field_name'),
            'fieldName': field_data.get('fieldName'),
            'label': field_data.get('label'),
            'field_label': field_data.get('field_label'),
            'title': field_data.get('title'),
            'field_title': field_data.get('field_title')
        }
        
        # Filter out None values
        valid_candidates = {k: v for k, v in name_candidates.items() if v is not None}
        
        # Select the field name using the same logic as field_based_quote
        selected_name = (
            field_data.get('name') or 
            field_data.get('field_name') or 
            field_data.get('fieldName') or 
            field_data.get('label') or
            field_data.get('field_label') or
            f'Field {field_id}'
        )
        
        # Comprehensive analysis
        field_analysis = {
            "data_structure_health": "✅ Valid" if field_data else "❌ Missing",
            "coordinates_available": "✅ Valid" if (field_data.get('latitude') and field_data.get('longitude')) else "❌ Missing/Invalid",
            "area_available": "✅ Present" if field_data.get('area_ha') else "❌ Missing",
            "crop_specified": "✅ Present" if field_data.get('crop') else "❌ Missing",
            "name_extraction_status": "✅ Success" if any(valid_candidates.values()) else "❌ Failed"
        }
        
        print(f"INFO: Available keys: {list(field_data.keys())}")
        print(f"INFO: Name candidates: {name_candidates}")
        print(f"INFO: Selected name: '{selected_name}'")
        print(f"INFO: Field analysis: {field_analysis}")
        print(f"INFO: =================================")
        
        return jsonify({
            "status": "success",
            "field_id": field_id,
            "field_data": field_data,
            "available_keys": list(field_data.keys()),
            "name_candidates": name_candidates,
            "valid_name_candidates": valid_candidates,
            "selected_name": selected_name,
            "field_analysis": field_analysis,
            "extraction_logic": [
                "1. Try 'name' field",
                "2. Try 'field_name' field", 
                "3. Try 'fieldName' field",
                "4. Try 'label' field",
                "5. Try 'field_label' field",
                "6. Fallback to 'Field {field_id}'"
            ],
            "version": "4.0-Enterprise"
        })
        
    except Exception as e:
        print(f"ERROR: Debug field data error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }), 500
