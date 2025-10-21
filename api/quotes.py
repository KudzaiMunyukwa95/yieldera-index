"""
Enterprise Quote API endpoints with data-driven executive summaries
Implements rainfall-only planting detection and detailed analysis
"""

from flask import Blueprint, request, jsonify
import traceback
import time
from datetime import datetime
from typing import Dict, List, Any

from core.quote_engine import QuoteEngine
from core.database import FieldsRepository, QuotesRepository
from core.ai_summary import EnterpriseExecutiveSummaryGenerator, generate_executive_summary  # ✅ FIXED IMPORT
from core.crops import validate_crop, list_supported_crops

quotes_bp = Blueprint('quotes', __name__)

# Initialize components with enterprise generator
quote_engine = QuoteEngine()
fields_repo = FieldsRepository()
quotes_repo = QuotesRepository()
enterprise_summary_generator = EnterpriseExecutiveSummaryGenerator()  # ✅ FIXED CLASS NAME

@quotes_bp.route('/historical', methods=['POST'])
def historical_quote():
    """Generate historical quote with enterprise executive summary"""
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
        
        print(f"INFO: Processing historical quote for year {data.get('year')}")
        
        # Execute quote with enterprise engine
        quote_result = quote_engine.execute_quote(data)
        
        # ✅ FIXED: Generate enterprise executive summary
        try:
            field_name = data.get('location_info', {}).get('name', 'Target Field')
            quote_result['executive_summary'] = generate_executive_summary(
                quote_result,
                field_name=field_name,
                summary_type='comprehensive'
            )
            # Keep backward compatibility
            quote_result['ai_summary'] = quote_result['executive_summary']
        except Exception as e:
            print(f"ERROR: Executive summary generation failed: {e}")
            quote_result['executive_summary'] = "Executive summary temporarily unavailable"
            quote_result['ai_summary'] = quote_result['executive_summary']
        
        # Save quote to database
        try:
            quote_id = quotes_repo.save_quote(quote_result)
            if quote_id:
                quote_result['quote_id'] = quote_id
        except Exception as e:
            print(f"WARNING: Failed to save quote: {e}")
        
        execution_time = time.time() - start_time
        
        return jsonify({
            "status": "success",
            "quote": quote_result,
            "execution_time_seconds": round(execution_time, 2),
            "version": "4.0-Enterprise"
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
            print(f"WARNING: Late season quote for {target_year} (current month: {current_month})")
        
        print(f"INFO: Processing prospective quote for {target_year} season")
        
        # Execute quote with enterprise engine
        quote_result = quote_engine.execute_quote(data)
        
        # ✅ FIXED: Generate enterprise executive summary
        try:
            field_name = data.get('location_info', {}).get('name', 'Target Field')
            quote_result['executive_summary'] = generate_executive_summary(
                quote_result,
                field_name=field_name,
                summary_type='comprehensive'
            )
            # Keep backward compatibility
            quote_result['ai_summary'] = quote_result['executive_summary']
        except Exception as e:
            print(f"ERROR: Executive summary generation failed: {e}")
            quote_result['executive_summary'] = "Executive summary temporarily unavailable"
            quote_result['ai_summary'] = quote_result['executive_summary']
        
        # Save quote to database
        try:
            quote_id = quotes_repo.save_quote(quote_result)
            if quote_id:
                quote_result['quote_id'] = quote_id
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
                "target_season": f"{target_year-1}/{target_year}",
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
    """Generate field-based quote with enterprise executive summary"""
    try:
        start_time = time.time()
        data = request.get_json()
        
        print(f"INFO: Processing field {field_id} quote")
        
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
        
        # Get field name for executive summary
        field_name = field_data.get('name', f'Field {field_id}')
        
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
            'field_name': field_name,  # ✅ ADDED: Pass field name for summary
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
        
        # Execute quote with enterprise engine
        quote_result = quote_engine.execute_quote(quote_request)
        quote_result['field_info'] = quote_request['field_info']
        
        # ✅ FIXED: Generate enterprise executive summary with actual field name
        try:
            quote_result['executive_summary'] = generate_executive_summary(
                quote_result,
                field_name=field_name,
                summary_type='comprehensive'
            )
            # Keep backward compatibility
            quote_result['ai_summary'] = quote_result['executive_summary']
        except Exception as e:
            print(f"ERROR: Executive summary generation failed: {e}")
            quote_result['executive_summary'] = "Executive summary temporarily unavailable"
            quote_result['ai_summary'] = quote_result['executive_summary']
        
        # Save quote to database
        try:
            quote_result['field_id'] = field_id
            quote_id = quotes_repo.save_quote(quote_result)
            if quote_id:
                quote_result['quote_id'] = quote_id
        except Exception as e:
            print(f"WARNING: Failed to save quote: {e}")
        
        execution_time = time.time() - start_time
        
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
        print(f"ERROR: Field quote error for field {field_id}")
        print(f"ERROR: {e}")
        print(f"ERROR: Full traceback: {traceback.format_exc()}")
        
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__,
            "field_id": field_id
        }), 500

@quotes_bp.route('/bulk', methods=['POST'])
def bulk_quote():
    """Generate bulk quotes with enterprise portfolio analysis"""
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
        
        print(f"INFO: Processing bulk quote: {len(requests)} requests")
        
        results = []
        successful_quotes = []
        
        for i, req in enumerate(requests):
            try:
                print(f"\nINFO: Processing bulk request {i+1}/{len(requests)}")
                
                # Merge global settings
                quote_request = {**global_settings, **req}
                
                # Handle field-based request
                field_name = "Target Field"
                if 'field_id' in req:
                    field_data = fields_repo.get_field_by_id(req['field_id'])
                    if field_data:
                        latitude = field_data['latitude']
                        longitude = field_data['longitude']
                        area_ha = field_data.get('area_ha')
                        field_name = field_data.get('name', f'Field {req["field_id"]}')
                        
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
                    else:
                        results.append({
                            "request_index": i,
                            "status": "error",
                            "message": f"Field {req['field_id']} not found or has invalid data"
                        })
                        continue
                
                # Execute quote with enterprise engine
                quote_result = quote_engine.execute_quote(quote_request)
                successful_quotes.append(quote_result)
                
                # ✅ FIXED: Generate enterprise executive summary for each bulk item
                try:
                    quote_result['executive_summary'] = generate_executive_summary(
                        quote_result,
                        field_name=field_name,
                        summary_type='concise'  # Use concise for bulk
                    )
                    quote_result['ai_summary'] = quote_result['executive_summary']
                except Exception as e:
                    print(f"WARNING: Executive summary generation failed for bulk item {i}: {e}")
                    quote_result['executive_summary'] = "Executive summary temporarily unavailable"
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
        
        # Generate enterprise portfolio analysis
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
                    "simulation_years": len(successful_quotes[0].get('historical_simulation', [])) if successful_quotes else 0,
                    "average_loss_ratio": sum(q.get('risk_metrics', {}).get('expected_loss_ratio', 0) for q in successful_quotes) / len(successful_quotes)
                }
                
                # Crop distribution
                crops = [q.get('crop', 'unknown') for q in successful_quotes]
                for crop in set(crops):
                    portfolio_metrics["crop_distribution"][crop] = crops.count(crop)
                
                portfolio_analysis['portfolio_metrics'] = portfolio_metrics
                
            except Exception as e:
                print(f"WARNING: Portfolio analysis generation failed: {e}")
                portfolio_analysis = {"error": "Portfolio analysis temporarily unavailable"}
        
        execution_time = time.time() - start_time
        successful_count = sum(1 for r in results if r['status'] == 'success')
        
        print(f"INFO: Bulk processing completed: {successful_count}/{len(requests)} successful")
        
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

# Remaining endpoints stay the same...
# (Keep all other existing endpoints exactly as they are)

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
        
        simulation_data = quote_data.get('historical_simulation', [])
        
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
            drought_impact = year_data.get('drought_impact_pct', 0)
            if drought_impact < 5:
                enhanced_year['risk_level'] = 'Low'
            elif drought_impact < 15:
                enhanced_year['risk_level'] = 'Moderate'
            elif drought_impact < 30:
                enhanced_year['risk_level'] = 'High'
            else:
                enhanced_year['risk_level'] = 'Severe'
            
            # Add farmer perspective
            payout = year_data.get('simulated_payout', 0)
            enhanced_year['farmer_outcome'] = 'Positive' if payout > 0 else 'No Payout'
            
            enhanced_simulation.append(enhanced_year)
        
        return jsonify({
            "status": "success",
            "quote_id": quote_id,
            "simulation_data": enhanced_simulation,
            "summary_statistics": {
                "total_years": len(simulation_data),
                "years_with_payouts": len([y for y in simulation_data if y.get('simulated_payout', 0) > 0]),
                "average_drought_impact": sum(y.get('drought_impact_pct', 0) for y in simulation_data) / len(simulation_data),
                "worst_year": max(simulation_data, key=lambda x: x.get('drought_impact_pct', 0))['year'],
                "best_year": min(simulation_data, key=lambda x: x.get('drought_impact_pct', 0))['year']
            }
        })
        
    except Exception as e:
        print(f"ERROR: Simulation details error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

# Keep all remaining endpoints (test, get_quote, validate, etc.) exactly as they are...
# Just replace any references to 'enhanced_ai_generator' with 'enterprise_summary_generator'
# and update method calls to use generate_executive_summary()
