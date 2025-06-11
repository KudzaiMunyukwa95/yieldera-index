"""
Refined Quote API endpoints with year-by-year simulation
Implements rainfall-only planting detection and detailed analysis
"""

from flask import Blueprint, request, jsonify
import traceback
import time
from datetime import datetime
from typing import Dict, List, Any

from core.quote_engine import QuoteEngine  # Updated to use existing file
from core.database import FieldsRepository, QuotesRepository
from core.ai_summary import EnhancedAISummaryGenerator
from core.crops import validate_crop, list_supported_crops

quotes_bp = Blueprint('quotes', __name__)

# Initialize components with enhanced engine
quote_engine = QuoteEngine()  # Updated to use existing class
fields_repo = FieldsRepository()
quotes_repo = QuotesRepository()
enhanced_ai_generator = EnhancedAISummaryGenerator()

@quotes_bp.route('/historical', methods=['POST'])
def historical_quote():
    """Generate refined historical quote with detailed simulation"""
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
        
        print(f"🔍 Processing refined historical quote for year {data.get('year')}")
        
        # Execute quote with enhanced engine
        quote_result = quote_engine.execute_quote(data)
        
        # Generate comprehensive report
        try:
            comprehensive_report = enhanced_ai_generator.generate_comprehensive_quote_report(
                quote_result, data.get('location_info')
            )
            quote_result['comprehensive_report'] = comprehensive_report
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
            "version": "2.1.0-Refined",
            "enhancements": [
                "Rainfall-only planting detection",
                "Year-by-year simulation", 
                "Seasonal validation",
                "Detailed loss analysis"
            ]
        })
        
    except Exception as e:
        print(f"Refined historical quote error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }), 500

@quotes_bp.route('/prospective', methods=['POST'])
def prospective_quote():
    """Generate refined prospective quote with seasonal validation"""
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
        
        # Validate seasonal appropriateness
        if target_year == current_year and current_month > 3:
            print(f"⚠️ Warning: Late season quote for {target_year} (current month: {current_month})")
        
        print(f"🌱 Processing refined prospective quote for {target_year} season")
        
        # Execute quote with enhanced engine
        quote_result = quote_engine.execute_quote(data)
        
        # Generate comprehensive report
        try:
            comprehensive_report = enhanced_ai_generator.generate_comprehensive_quote_report(
                quote_result, data.get('location_info')
            )
            quote_result['comprehensive_report'] = comprehensive_report
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
        print(f"✅ Refined prospective quote completed in {execution_time:.2f} seconds")
        
        return jsonify({
            "status": "success",
            "quote": quote_result,
            "execution_time_seconds": round(execution_time, 2),
            "version": "2.1.0-Refined",
            "seasonal_validation": {
                "target_season": f"{target_year-1}//{target_year}",
                "planting_window": "October - January",
                "current_month": current_month,
                "seasonal_appropriateness": "validated"
            }
        })
        
    except Exception as e:
        print(f"Refined prospective quote error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }), 500

@quotes_bp.route('/field/<int:field_id>', methods=['POST'])
def field_based_quote(field_id):
    """Generate refined field-based quote with enhanced validation"""
    try:
        start_time = time.time()
        data = request.get_json()
        
        print(f"=== REFINED FIELD {field_id} QUOTE ===")
        
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
        
        # Build refined quote request
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
            'field_info': {
                'type': 'field',
                'field_id': field_id,
                'name': field_data.get('name', f'Field {field_id}'),
                'farmer_name': field_data.get('farmer_name'),
                'area_ha': area_ha,
                'coordinates': f"{latitude:.6f}, {longitude:.6f}"
            }
        }
        
        print(f"🌾 Field crop: {crop}")
        print(f"📍 Coordinates: {latitude:.4f}, {longitude:.4f}")
        print(f"📏 Area: {area_ha} ha" if area_ha else "📏 Area: Not specified")
        
        # Execute quote with enhanced engine
        quote_result = quote_engine.execute_quote(quote_request)
        quote_result['field_info'] = quote_request['field_info']
        
        # Generate comprehensive report
        try:
            comprehensive_report = enhanced_ai_generator.generate_comprehensive_quote_report(
                quote_result, quote_request['field_info']
            )
            quote_result['comprehensive_report'] = comprehensive_report
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
            "version": "2.1.0-Refined"
        })
        
    except Exception as e:
        print(f"=== REFINED FIELD QUOTE ERROR FOR FIELD {field_id} ===")
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
    """Generate refined bulk quotes with enhanced portfolio analysis"""
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
        
        print(f"📊 Processing refined bulk quote: {len(requests)} requests")
        
        results = []
        successful_quotes = []
        
        for i, req in enumerate(requests):
            try:
                print(f"\n🔄 Processing bulk request {i+1}/{len(requests)}")
                
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
                
                # Execute quote with enhanced engine
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
                
                print(f"✅ Bulk request {i+1} completed: ${quote_result.get('gross_premium', 0):,.0f} premium")
                
            except Exception as e:
                print(f"❌ Bulk request {i+1} failed: {e}")
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
                
                # Add refined portfolio metrics
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
                
            except Exception as e:
                print(f"Portfolio analysis generation failed: {e}")
                portfolio_analysis = "Portfolio analysis temporarily unavailable"
        
        execution_time = time.time() - start_time
        successful_count = sum(1 for r in results if r['status'] == 'success')
        
        print(f"📊 Bulk processing completed: {successful_count}/{len(requests)} successful")
        
        return jsonify({
            "status": "success",
            "portfolio_analysis": portfolio_analysis,
            "total_requests": len(requests),
            "successful_quotes": successful_count,
            "failed_quotes": len(requests) - successful_count,
            "results": results,
            "execution_time_seconds": round(execution_time, 2),
            "version": "2.1.0-Refined"
        })
        
    except Exception as e:
        print(f"Refined bulk quote error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }), 500

# New endpoint: Year-by-year simulation details
@quotes_bp.route('/simulation/<quote_id>', methods=['GET'])
def get_simulation_details(quote_id):
    """Get detailed year-by-year simulation for a quote"""
    try:
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
            }
        })
        
    except Exception as e:
        print(f"Simulation details error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

# Enhanced testing endpoint
@quotes_bp.route('/test/refined', methods=['POST'])
def test_refined_features():
    """Test endpoint for refined features"""
    try:
        data = request.get_json() or {}
        
        # Default test quote data for Zimbabwe
        test_quote_data = {
            "crop": data.get("crop", "maize"),
            "latitude": data.get("latitude", -17.8216),  # Harare area
            "longitude": data.get("longitude", 30.9858),
            "expected_yield": data.get("expected_yield", 5.0),
            "price_per_ton": data.get("price_per_ton", 300),
            "year": data.get("year", 2024),
            "area_ha": data.get("area_ha", 2.0)
        }
        
        print(f"🧪 Testing refined features with data: {test_quote_data}")
        
        # Execute refined quote
        quote_result = quote_engine.execute_quote(test_quote_data)
        
        # Extract key refined features for testing
        refined_features = {
            "planting_detection": quote_result.get('planting_analysis', {}),
            "year_by_year_simulation": quote_result.get('year_by_year_simulation', []),
            "simulation_summary": quote_result.get('simulation_summary', {}),
            "seasonal_validation": {
                "method": "rainfall_only",
                "criteria": "20mm over 7 days, 2+ days ≥5mm",
                "season_window": "October 1 - January 31"
            }
        }
        
        return jsonify({
            "status": "success",
            "message": "Refined features test successful",
            "test_quote_data": test_quote_data,
            "quote_result": quote_result,
            "refined_features": refined_features,
            "features_tested": [
                "Rainfall-only planting detection",
                "Year-by-year simulation",
                "Seasonal validation (Oct-Jan only)",
                "Individual year premium/payout calculation",
                "Enhanced phase analysis",
                "Zone-based risk adjustments"
            ]
        })
        
    except Exception as e:
        print(f"Refined features test error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }), 500

# Existing endpoints maintained for backward compatibility
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
        include_simulation = request.args.get('include_simulation', 'false').lower() == 'true'
        
        response_data = {
            "status": "success",
            "quote": quote,
            "view_mode": view_mode
        }
        
        # Add simulation data if requested
        if include_simulation:
            quote_data = quote.get('quote_data', {})
            if isinstance(quote_data, str):
                import json
                quote_data = json.loads(quote_data)
            
            response_data['simulation_data'] = quote_data.get('year_by_year_simulation', [])
        
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
                        "latest_quote_date": quotes[0].get('created_at') if quotes else None,
                        "version_evolution": list(set(q.get('version', 'unknown') for q in quotes))
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
    """Enhanced validation with seasonal checks"""
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
            }
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
                "report_version": "2.1.0-Refined"
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
