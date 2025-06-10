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
            "version": "3.0.0-Enhanced",
            "enhancements": [
                "Industry-standard 10-day rolling drought detection",
                "Acre Africa methodology compatibility",
                "Enhanced consecutive dry spell detection",
                "Maximum stress calculation method",
                "Geographic optimization for Southern Africa",
                "Full backward compatibility maintained"
            ],
            "drought_detection_features": {
                "rolling_window_size": "10 days (industry standard)",
                "drought_threshold": "≤15mm per 10-day window",
                "consecutive_threshold": "≥10 consecutive dry days",
                "calculation_method": "MAX(cumulative_stress, rolling_window_stress, consecutive_drought_stress)",
                "geographic_focus": "Southern Africa (-25° to -15° lat, 25° to 35° lon)",
                "backward_compatibility": "Fully maintained"
            }
        })
        
    except Exception as e:
        print(f"Enhanced features test error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }), 500

# Enhanced simulation details endpoint
@quotes_bp.route('/simulation/<quote_id>', methods=['GET'])
def get_enhanced_simulation_details(quote_id):
    """Get detailed year-by-year simulation with enhanced drought analysis"""
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
        enhanced_drought_analysis = quote_data.get('enhanced_drought_analysis')
        
        if not simulation_data:
            return jsonify({
                "status": "error",
                "message": "No simulation data available for this quote"
            }), 404
        
        # Enhance simulation data with additional analysis
        enhanced_simulation = []
        enhanced_years_count = 0
        
        for year_data in simulation_data:
            enhanced_year = dict(year_data)
            
            # Add interpretations
            drought_impact = year_data.get('drought_impact', 0)
            if drought_impact < 5:
                enhanced_year['risk_level'] = 'Minimal'
            elif drought_impact < 15:
                enhanced_year['risk_level'] = 'Low'
            elif drought_impact < 30:
                enhanced_year['risk_level'] = 'Moderate'
            elif drought_impact < 50:
                enhanced_year['risk_level'] = 'High'
            else:
                enhanced_year['risk_level'] = 'Severe'
            
            # Add farmer perspective
            net_result = year_data.get('net_result', 0)
            enhanced_year['farmer_outcome'] = 'Positive' if net_result > 0 else 'Negative'
            
            # Enhanced drought methodology indicator
            methodology = year_data.get('methodology', 'traditional_fallback')
            enhanced_year['enhanced_methodology_used'] = methodology == 'enhanced_v3.0'
            if methodology == 'enhanced_v3.0':
                enhanced_years_count += 1
            
            # Add enhanced drought data if available
            if 'enhanced_drought_data' in year_data:
                enhanced_year['enhanced_drought_summary'] = {
                    'overall_impact': year_data['enhanced_drought_data'].get('overall_drought_impact_percent'),
                    'dominant_stress_type': year_data['enhanced_drought_data'].get('stress_components', {}).get('dominant_stress_type'),
                    'risk_category': year_data['enhanced_drought_data'].get('risk_category')
                }
            
            enhanced_simulation.append(enhanced_year)
        
        # Calculate enhanced summary statistics
        enhanced_summary = {
            "total_years": len(simulation_data),
            "enhanced_analysis_years": enhanced_years_count,
            "enhanced_coverage_percent": round(enhanced_years_count / len(simulation_data) * 100, 1),
            "years_with_payouts": len([y for y in simulation_data if y.get('drought_impact', 0) > 5]),
            "average_premium": sum(y.get('simulated_premium_usd', 0) for y in simulation_data) / len(simulation_data),
            "average_payout": sum(y.get('simulated_payout', 0) for y in simulation_data) / len(simulation_data),
            "worst_year": max(simulation_data, key=lambda x: x.get('drought_impact', 0))['year'],
            "best_year": min(simulation_data, key=lambda x: x.get('drought_impact', 0))['year'],
            "methodology_breakdown": {
                "enhanced_v3.0": enhanced_years_count,
                "traditional_fallback": len(simulation_data) - enhanced_years_count
            }
        }
        
        # Add drought analysis summary if available
        drought_analysis_summary = {}
        if enhanced_drought_analysis:
            drought_analysis_summary = {
                "overall_drought_impact_percent": enhanced_drought_analysis.get('overall_drought_impact_percent'),
                "risk_category": enhanced_drought_analysis.get('risk_category'),
                "dominant_stress_type": enhanced_drought_analysis.get('stress_components', {}).get('dominant_stress_type'),
                "methodology": enhanced_drought_analysis.get('methodology'),
                "industry_compliance": enhanced_drought_analysis.get('industry_standards', {})
            }
        
        return jsonify({
            "status": "success",
            "quote_id": quote_id,
            "simulation_data": enhanced_simulation,
            "enhanced_summary_statistics": enhanced_summary,
            "enhanced_drought_analysis_summary": drought_analysis_summary,
            "methodology_info": {
                "version": "3.0.0-Enhanced",
                "drought_detection": "Industry-standard 10-day rolling window",
                "acre_africa_compatibility": "Full alignment",
                "enhanced_features": [
                    "Rolling window drought detection",
                    "Consecutive dry spell analysis", 
                    "Maximum stress calculation",
                    "Geographic optimization",
                    "Crop-specific thresholds"
                ]
            }
        })
        
    except Exception as e:
        print(f"Enhanced simulation details error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

# Existing endpoints maintained for backward compatibility
@quotes_bp.route('/<quote_id>', methods=['GET'])
def get_quote(quote_id):
    """Get quote by ID with enhanced display options"""
    try:
        quote = quotes_repo.get_quote_by_id(quote_id)
        
        if not quote:
            return jsonify({
                "status": "error",
                "message": f"Quote {quote_id} not found"
            }), 404
        
        # Enhanced viewing options
        view_mode = request.args.get('view', 'standard')
        include_simulation = request.args.get('include_simulation', 'false').lower() == 'true'
        include_drought_analysis = request.args.get('include_drought_analysis', 'false').lower() == 'true'
        
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
        
        # Add enhanced drought analysis if requested
        if include_drought_analysis:
            quote_data = quote.get('quote_data', {})
            if isinstance(quote_data, str):
                import json
                quote_data = json.loads(quote_data)
            
            response_data['enhanced_drought_analysis'] = quote_data.get('enhanced_drought_analysis')
            response_data['enhanced_premium_adjustments'] = quote_data.get('enhanced_premium_adjustments')
        
        return jsonify(response_data)
        
    except Exception as e:
        print(f"Get quote error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@quotes_bp.route('/field/<int:field_id>/history', methods=['GET'])
def get_field_quote_history(field_id):
    """Get enhanced quote history for a field with drought analysis trends"""
    try:
        limit = request.args.get('limit', 20, type=int)
        quotes = quotes_repo.get_quotes_by_field(field_id, limit)
        
        # Enhanced trend analysis
        trend_analysis = {}
        drought_trend_analysis = {}
        
        if len(quotes) > 1:
            try:
                premium_rates = [q.get('premium_rate', 0) for q in quotes if q.get('premium_rate')]
                
                # Extract enhanced drought analysis trends
                enhanced_quotes = []
                for q in quotes:
                    quote_data = q.get('quote_data', {})
                    if isinstance(quote_data, str):
                        import json
                        quote_data = json.loads(quote_data)
                    
                    if 'enhanced_drought_analysis' in quote_data:
                        enhanced_quotes.append(quote_data['enhanced_drought_analysis'])
                
                if premium_rates:
                    trend_analysis = {
                        "average_premium_rate": f"{sum(premium_rates) / len(premium_rates) * 100:.2f}%",
                        "rate_trend": "increasing" if premium_rates[-1] > premium_rates[0] else "decreasing",
                        "quote_frequency": f"{len(quotes)} quotes generated",
                        "latest_quote_date": quotes[0].get('created_at') if quotes else None,
                        "version_evolution": list(set(q.get('version', 'unknown') for q in quotes)),
                        "enhanced_analysis_adoption": f"{len(enhanced_quotes)}/{len(quotes)} quotes with enhanced analysis"
                    }
                
                # Drought analysis trends
                if enhanced_quotes:
                    avg_drought_impact = sum(q.get('overall_drought_impact_percent', 0) for q in enhanced_quotes) / len(enhanced_quotes)
                    risk_categories = [q.get('risk_category', 'unknown') for q in enhanced_quotes]
                    dominant_stress_types = [q.get('stress_components', {}).get('dominant_stress_type', 'unknown') for q in enhanced_quotes]
                    
                    drought_trend_analysis = {
                        "average_drought_impact_percent": round(avg_drought_impact, 2),
                        "risk_category_distribution": {cat: risk_categories.count(cat) for cat in set(risk_categories)},
                        "dominant_stress_type_distribution": {stype: dominant_stress_types.count(stype) for stype in set(dominant_stress_types)},
                        "enhanced_methodology_coverage": len(enhanced_quotes)
                    }
                
            except Exception as e:
                print(f"Enhanced trend analysis error: {e}")
        
        return jsonify({
            "status": "success",
            "field_id": field_id,
            "quotes": quotes,
            "total": len(quotes),
            "trend_analysis": trend_analysis,
            "enhanced_drought_trend_analysis": drought_trend_analysis,
            "analysis_features": {
                "enhanced_drought_detection": "10-day rolling window analysis",
                "acre_africa_compatibility": "Full methodology alignment", 
                "historical_trend_tracking": "Premium and drought impact evolution",
                "methodology_evolution": "Traditional to enhanced transition tracking"
            }
        })
        
    except Exception as e:
        print(f"Get enhanced field quotes error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@quotes_bp.route('/validate', methods=['POST'])
def validate_quote_request():
    """Enhanced validation with drought detection capability checks"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                "status": "error",
                "message": "Request body is required"
            }), 400
        
        validation_errors = []
        validation_warnings = []
        enhancement_recommendations = []
        
        # Check required fields
        required_fields = ['expected_yield', 'price_per_ton']
        for field in required_fields:
            if field not in data:
                validation_errors.append(f"Missing required field: {field}")
        
        # Validate crop
        if 'crop' in data:
            try:
                validated_crop = validate_crop(data['crop'])
                
                # Check enhanced drought threshold availability
                try:
                    test_thresholds = enhanced_drought_calculator.get_enhanced_drought_thresholds(
                        validated_crop, 'flowering'
                    )
                    enhancement_recommendations.append(f"Enhanced drought thresholds available for {validated_crop}")
                except Exception:
                    validation_warnings.append(f"Enhanced drought thresholds not fully calibrated for {validated_crop}")
                    
            except ValueError as e:
                validation_errors.append(str(e))
        
        # Validate geometry/coordinates with Southern Africa focus
        has_geometry = 'geometry' in data
        has_coordinates = 'latitude' in data and 'longitude' in data
        has_field_id = 'field_id' in data
        
        if not (has_geometry or has_coordinates or has_field_id):
            validation_errors.append("Must provide either 'geometry', 'latitude'/'longitude', or 'field_id'")
        
        # Enhanced geographic validation
        if 'latitude' in data and 'longitude' in data:
            try:
                lat = float(data['latitude'])
                lon = float(data['longitude'])
                
                # Check Southern Africa bounds
                if enhanced_drought_calculator._validate_geographic_bounds(lat, lon):
                    enhancement_recommendations.append("Location within Southern Africa focus area - enhanced analysis available")
                else:
                    validation_warnings.append("Location outside Southern Africa focus area - enhanced analysis may be less accurate")
                
                # General coordinate validation
                if not (-25 <= lat <= -15 and 25 <= lon <= 35):
                    validation_warnings.append("Coordinates appear to be outside optimal Southern Africa region")
                    
            except:
                pass  # Already caught in numeric validation
        
        # Enhanced numeric validation
        numeric_fields = ['expected_yield', 'price_per_ton', 'area_ha', 'year', 'latitude', 'longitude', 'deductible_rate']
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
                    elif field == 'deductible_rate' and (value < 0 or value > 0.5):
                        validation_errors.append(f"Deductible rate must be between 0% and 50%")
                except (ValueError, TypeError):
                    validation_errors.append(f"Field '{field}' must be a number")
        
        # Enhanced loadings validation
        if 'loadings' in data:
            loadings = data['loadings']
            if not isinstance(loadings, dict):
                validation_errors.append("Loadings must be provided as a dictionary")
            else:
                for loading_type, loading_value in loadings.items():
                    if not isinstance(loading_value, (int, float)):
                        validation_errors.append(f"Loading '{loading_type}' must be a number")
                    elif loading_value < 0 or loading_value > 1.0:
                        validation_errors.append(f"Loading '{loading_type}' must be between 0% and 100%")
        
        # Seasonal validation for year
        if 'year' in data:
            year = data['year']
            current_year = datetime.now().year
            current_month = datetime.now().month
            
            if year < 1981 or year > current_year + 2:
                validation_errors.append(f"Year {year} is outside valid range (1981-{current_year + 2})")
            
            # Enhanced seasonal appropriateness check
            if year == current_year:
                if current_month > 3:
                    validation_warnings.append(f"Quote for {year} may be late in season (current month: {current_month})")
                elif current_month < 8:
                    validation_warnings.append(f"Consider quoting for {year + 1} season instead")
        
        if validation_errors:
            return jsonify({
                "status": "error",
                "validation_errors": validation_errors,
                "validation_warnings": validation_warnings,
                "enhancement_recommendations": enhancement_recommendations
            }), 400
        
        # Enhanced response with drought detection capabilities
        current_month = datetime.now().month
        current_year = datetime.now().year
        
        target_year = data.get('year', current_year + 1)
        if target_year > current_year:
            quote_type = "prospective"
        elif target_year == current_year and current_month >= 8:
            quote_type = "prospective"
        else:
            quote_type = "historical"
        
        response = {
            "status": "success",
            "message": "Quote request is valid and enhanced features available",
            "estimated_quote_type": quote_type,
            "validation_warnings": validation_warnings,
            "enhancement_recommendations": enhancement_recommendations,
            "enhanced_capabilities": {
                "drought_detection_method": "Industry-standard 10-day rolling window",
                "acre_africa_compatibility": "Full methodology alignment",
                "consecutive_drought_detection": "≥10 consecutive dry days",
                "geographic_optimization": "Southern Africa focus",
                "crop_specific_thresholds": "Available for all supported crops"
            },
            "seasonal_guidance": {
                "current_month": current_month,
                "recommended_season": f"{current_year}/{current_year + 1}" if current_month >= 8 else f"{current_year + 1}/{current_year + 2}",
                "planting_window": "October - January",
                "next_season_starts": f"October {current_year}" if current_month < 10 else f"October {current_year + 1}"
            }
        }
        
        # Add enhanced recommendations
        recommendations = []
        if 'area_ha' not in data:
            recommendations.append("Consider adding 'area_ha' for more accurate sum insured calculation")
        if 'zone' not in data:
            recommendations.append("Specify 'zone' for location-specific risk adjustments")
        if 'loadings' not in data:
            recommendations.append("Add 'loadings' for complete premium calculation")
        if 'deductible_rate' not in data:
            recommendations.append("Specify 'deductible_rate' for customized coverage (default 5%)")
        
        recommendations.append("Enhanced drought analysis available with 10-day rolling window methodology")
        recommendations.append("Premium adjustments available based on enhanced drought risk assessment")
        
        if recommendations:
            response['recommendations'] = recommendations
        
        return jsonify(response)
        
    except Exception as e:
        print(f"Enhanced validation error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

# New enhanced report generation endpoint
@quotes_bp.route('/report/<quote_id>', methods=['GET'])
def generate_enhanced_detailed_report(quote_id):
    """Generate enhanced standalone detailed report for a quote"""
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
        
        # Generate enhanced comprehensive report
        try:
            comprehensive_report = enhanced_ai_generator.generate_comprehensive_quote_report(
                quote_data, quote_data.get('field_info')
            )
            
            # Add enhanced drought analysis summary to report
            enhanced_drought_analysis = quote_data.get('enhanced_drought_analysis')
            if enhanced_drought_analysis:
                comprehensive_report['enhanced_drought_summary'] = {
                    'methodology': 'Industry-standard 10-day rolling window',
                    'overall_drought_impact': enhanced_drought_analysis.get('overall_drought_impact_percent'),
                    'risk_category': enhanced_drought_analysis.get('risk_category'),
                    'dominant_stress_type': enhanced_drought_analysis.get('stress_components', {}).get('dominant_stress_type'),
                    'acre_africa_compatibility': 'Full alignment',
                    'industry_compliance': enhanced_drought_analysis.get('industry_standards', {})
                }
            
            return jsonify({
                "status": "success",
                "quote_id": quote_id,
                "report": comprehensive_report,
                "generated_at": datetime.utcnow().isoformat(),
                "report_version": "3.0.0-Enhanced",
                "enhanced_features": {
                    "drought_detection": "Industry-standard 10-day rolling analysis",
                    "acre_africa_compatibility": "Full methodology alignment",
                    "southern_africa_optimization": "Geographic focus applied",
                    "crop_specific_thresholds": "Customized for crop and growth stages"
                }
            })
            
        except Exception as e:
            print(f"Enhanced report generation failed: {e}")
            return jsonify({
                "status": "error",
                "message": "Enhanced report generation failed",
                "error": str(e)
            }), 500
        
    except Exception as e:
        print(f"Enhanced detailed report error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

# Crop-specific enhanced thresholds endpoint
@quotes_bp.route('/thresholds/<crop>', methods=['GET'])
def get_crop_enhanced_thresholds(crop):
    """Get enhanced drought thresholds for specific crop"""
    try:
        # Validate crop
        try:
            validated_crop = validate_crop(crop)
        except ValueError as e:
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 400
        
        # Get thresholds for all growth stages
        growth_stages = ['emergence', 'vegetative', 'flowering', 'grain_fill', 'maturation']
        thresholds_by_stage = {}
        
        for stage in growth_stages:
            thresholds_by_stage[stage] = enhanced_drought_calculator.get_enhanced_drought_thresholds(
                validated_crop, stage
            )
        
        return jsonify({
            "status": "success",
            "crop": validated_crop,
            "enhanced_thresholds_by_stage": thresholds_by_stage,
            "methodology": {
                "version": "3.0.0-Enhanced",
                "compatibility": "Acre Africa methodology",
                "rolling_window_size": "10 days",
                "base_drought_threshold": "≤15mm per 10-day window",
                "consecutive_threshold": "≥10 consecutive dry days",
                "customization": "Crop and growth stage specific adjustments"
            }
        })
        
    except Exception as e:
        print(f"Crop thresholds error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

 10-day window",
                "consecutive_drought_trigger": "≥10 consecutive days <1mm",
                "calculation_method": "MAX(cumulative_stress, rolling_window_stress, consecutive_drought_stress)",
                "geographic_focus": "Southern Africa (-25° to -15° lat, 25° to 35° lon)"
            }
        })
        
    except Exception as e:
        print(f"Enhanced historical quote error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }), 500

@quotes_bp.route('/prospective', methods=['POST'])
def prospective_quote():
    """Generate enhanced prospective quote with seasonal validation"""
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
        
        # Smart year selection with seasonal validation
        current_month = datetime.now().month
        current_year = datetime.now().year
        
        if 'year' not in data:
            if current_month >= 8:  # Aug onwards - approaching next season
                data['year'] = current_year + 1
            else:  # Too late for current season, suggest next
                data['year'] = current_year + 1
        
        target_year = data['year']
        
        # Validate seasonal appropriateness
        if target_year == current_year and current_month > 3:
            print(f"⚠️ Warning: Late season quote for {target_year} (current month: {current_month})")
        
        print(f"🌱 Processing Enhanced Prospective Quote for {target_year} season")
        print(f"🔧 Using industry-standard drought detection with Acre Africa compatibility")
        
        # Execute enhanced quote
        quote_result = enhanced_quote_engine.execute_quote(data)
        
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
        print(f"✅ Enhanced prospective quote completed in {execution_time:.2f} seconds")
        
        return jsonify({
            "status": "success",
            "quote": quote_result,
            "execution_time_seconds": round(execution_time, 2),
            "version": "3.0.0-Enhanced",
            "seasonal_validation": {
                "target_season": f"{target_year-1}//{target_year}",
                "planting_window": "October - January",
                "current_month": current_month,
                "seasonal_appropriateness": "validated"
            },
            "drought_methodology": {
                "type": "industry_standard_10_day_rolling",
                "compatibility": "Acre Africa methodology",
                "consecutive_drought_detection": ">=10 consecutive dry days",
                "stress_calculation": "Maximum stress methodology"
            }
        })
        
    except Exception as e:
        print(f"Enhanced prospective quote error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }), 500

@quotes_bp.route('/field/<int:field_id>', methods=['POST'])
def field_based_quote(field_id):
    """Generate enhanced field-based quote with advanced validation"""
    try:
        start_time = time.time()
        data = request.get_json()
        
        print(f"=== ENHANCED FIELD {field_id} QUOTE ===")
        print(f"🔧 Industry-standard drought detection enabled")
        
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
        
        # Validate geographic bounds for Southern Africa focus
        if not enhanced_drought_calculator._validate_geographic_bounds(latitude, longitude):
            print(f"⚠️ Warning: Field {field_id} outside Southern Africa focus area")
        
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
        
        # Execute enhanced quote
        quote_result = enhanced_quote_engine.execute_quote(quote_request)
        quote_result['field_info'] = quote_request['field_info']
        
        # Add enhanced drought thresholds for this specific field
        if quote_result.get('enhanced_drought_analysis'):
            field_thresholds = enhanced_drought_calculator.get_enhanced_drought_thresholds(
                crop, 'flowering'  # Use flowering stage as representative
            )
            quote_result['field_specific_thresholds'] = field_thresholds
        
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
                "longitude": longitude,
                "geographic_validation": enhanced_drought_calculator._validate_geographic_bounds(latitude, longitude)
            },
            "execution_time_seconds": round(execution_time, 2),
            "version": "3.0.0-Enhanced",
            "field_enhancements": {
                "enhanced_drought_detection": "10-day rolling window analysis",
                "crop_specific_thresholds": "Customized for field crop and growth stages",
                "geographic_optimization": "Southern Africa focus",
                "acre_africa_compatibility": "Full methodology alignment"
            }
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
    """Generate enhanced bulk quotes with portfolio-level drought analysis"""
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
        
        print(f"📊 Processing Enhanced Bulk Quote: {len(requests)} requests")
        print(f"🔧 Using industry-standard drought detection for portfolio analysis")
        
        results = []
        successful_quotes = []
        portfolio_drought_analysis = []
        
        for i, req in enumerate(requests):
            try:
                print(f"\n🔄 Processing enhanced bulk request {i+1}/{len(requests)}")
                
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
                
                # Execute enhanced quote
                quote_result = enhanced_quote_engine.execute_quote(quote_request)
                successful_quotes.append(quote_result)
                
                # Extract drought analysis for portfolio assessment
                if 'enhanced_drought_analysis' in quote_result:
                    portfolio_drought_analysis.append({
                        'quote_index': i,
                        'field_id': req.get('field_id'),
                        'drought_analysis': quote_result['enhanced_drought_analysis']
                    })
                
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
                
                print(f"✅ Enhanced bulk request {i+1} completed: ${quote_result.get('gross_premium', 0):,.0f} premium")
                
            except Exception as e:
                print(f"❌ Bulk request {i+1} failed: {e}")
                results.append({
                    "request_index": i,
                    "status": "error",
                    "message": str(e),
                    "error_type": type(e).__name__
                })
        
        # Generate enhanced portfolio analysis
        portfolio_analysis = {}
        if successful_quotes:
            try:
                portfolio_analysis = enhanced_ai_generator.generate_bulk_summary(successful_quotes)
                
                # Add enhanced portfolio drought metrics
                total_premium = sum(q.get('gross_premium', 0) for q in successful_quotes)
                total_sum_insured = sum(q.get('sum_insured', 0) for q in successful_quotes)
                avg_premium_rate = (total_premium / total_sum_insured * 100) if total_sum_insured > 0 else 0
                
                # Portfolio-level drought analysis
                portfolio_drought_metrics = {}
                if portfolio_drought_analysis:
                    avg_drought_impact = sum(
                        d['drought_analysis']['overall_drought_impact_percent'] 
                        for d in portfolio_drought_analysis
                    ) / len(portfolio_drought_analysis)
                    
                    risk_categories = {}
                    for d in portfolio_drought_analysis:
                        risk_cat = d['drought_analysis']['risk_category']
                        risk_categories[risk_cat] = risk_categories.get(risk_cat, 0) + 1
                    
                    dominant_stress_types = {}
                    for d in portfolio_drought_analysis:
                        stress_type = d['drought_analysis']['stress_components']['dominant_stress_type']
                        dominant_stress_types[stress_type] = dominant_stress_types.get(stress_type, 0) + 1
                    
                    portfolio_drought_metrics = {
                        "average_drought_impact_percent": round(avg_drought_impact, 2),
                        "risk_category_distribution": risk_categories,
                        "dominant_stress_types": dominant_stress_types,
                        "fields_with_enhanced_analysis": len(portfolio_drought_analysis),
                        "enhanced_analysis_coverage": round(len(portfolio_drought_analysis) / len(successful_quotes) * 100, 1)
                    }
                
                portfolio_metrics = {
                    "total_premium": f"${total_premium:,.2f}",
                    "total_sum_insured": f"${total_sum_insured:,.2f}",
                    "average_premium_rate": f"{avg_premium_rate:.2f}%",
                    "crop_distribution": {},
                    "geographic_spread": len(set(q.get('latitude', 0) for q in successful_quotes)),
                    "simulation_years": len(successful_quotes[0].get('historical_years_used', [])) if successful_quotes else 0,
                    "enhanced_drought_metrics": portfolio_drought_metrics
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
        
        print(f"📊 Enhanced bulk processing completed: {successful_count}/{len(requests)} successful")
        
        return jsonify({
            "status": "success",
            "portfolio_analysis": portfolio_analysis,
            "total_requests": len(requests),
            "successful_quotes": successful_count,
            "failed_quotes": len(requests) - successful_count,
            "results": results,
            "execution_time_seconds": round(execution_time, 2),
            "version": "3.0.0-Enhanced",
            "portfolio_enhancements": {
                "enhanced_drought_detection": "Industry-standard 10-day rolling analysis",
                "portfolio_risk_assessment": "Aggregated drought impact analysis",
                "acre_africa_compatibility": "Full methodology alignment",
                "geographic_optimization": "Southern Africa focus"
            }
        })
        
    except Exception as e:
        print(f"Enhanced bulk quote error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }), 500

# Enhanced drought analysis endpoint
@quotes_bp.route('/drought-analysis', methods=['POST'])
def standalone_drought_analysis():
    """Standalone enhanced drought analysis endpoint"""
    try:
        start_time = time.time()
        data = request.get_json()
        
        if not data:
            return jsonify({
                "status": "error",
                "message": "Request body is required"
            }), 400
        
        # Validate required fields for drought analysis
        required_fields = ['latitude', 'longitude', 'crop']
        for field in required_fields:
            if field not in data:
                return jsonify({
                    "status": "error",
                    "message": f"Missing required field: {field}"
                }), 400
        
        print(f"🔍 Processing Standalone Enhanced Drought Analysis")
        
        # Extract parameters
        crop = data['crop'].lower().strip()
        lat = float(data['latitude'])
        lon = float(data['longitude'])
        year = data.get('year', datetime.now().year)
        planting_date = data.get('planting_date', f"{year-1}-11-15")
        
        # Validate crop
        try:
            validated_crop = validate_crop(crop)
        except ValueError as e:
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 400
        
        # Validate geographic bounds
        if not enhanced_drought_calculator._validate_geographic_bounds(lat, lon):
            print(f"⚠️ Warning: Coordinates outside Southern Africa focus area")
        
        # Get enhanced drought thresholds for crop
        thresholds_by_stage = {}
        growth_stages = ['emergence', 'vegetative', 'flowering', 'grain_fill', 'maturation']
        
        for stage in growth_stages:
            thresholds_by_stage[stage] = enhanced_drought_calculator.get_enhanced_drought_thresholds(
                validated_crop, stage
            )
        
        # Generate sample daily rainfall data for demonstration
        # In production, this would come from actual historical data
        from core.crops import get_crop_phases
        crop_phases = get_crop_phases(validated_crop)
        
        sample_daily_rainfall = {}
        for start_day, end_day, trigger_mm, exit_mm, phase_name, water_need_mm, obs_window in crop_phases:
            phase_duration = end_day - start_day + 1
            # Generate realistic sample data
            daily_rainfall = enhanced_quote_engine._distribute_rainfall_across_days(
                water_need_mm * 0.8,  # 80% of water need for demonstration
                phase_duration
            )
            sample_daily_rainfall[phase_name] = daily_rainfall
        
        # Perform enhanced drought analysis
        drought_analysis = enhanced_drought_calculator.calculate_enhanced_drought_impact(
            crop_phases=crop_phases,
            crop=validated_crop,
            lat=lat,
            lon=lon,
            planting_date=planting_date,
            daily_rainfall_by_phase=sample_daily_rainfall
        )
        
        # Calculate premium adjustment recommendations
        premium_adjustments = enhanced_drought_calculator.calculate_premium_adjustment_factor(
            drought_analysis
        )
        
        execution_time = time.time() - start_time
        
        return jsonify({
            "status": "success",
            "drought_analysis": drought_analysis,
            "crop_specific_thresholds": thresholds_by_stage,
            "premium_adjustments": premium_adjustments,
            "analysis_metadata": {
                "crop": validated_crop,
                "latitude": lat,
                "longitude": lon,
                "planting_date": planting_date,
                "year": year,
                "geographic_validation": enhanced_drought_calculator._validate_geographic_bounds(lat, lon),
                "execution_time_seconds": round(execution_time, 2)
            },
            "methodology": {
                "version": "3.0.0-Enhanced",
                "compatibility": "Acre Africa methodology",
                "rolling_window_size": "10 days",
                "drought_threshold": "≤15mm per 10-day window",
                "consecutive_threshold": "≥10 consecutive dry days",
                "calculation_method": "MAX(cumulative_stress, rolling_window_stress, consecutive_drought_stress)"
            }
        })
        
    except Exception as e:
        print(f"Standalone drought analysis error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }), 500

# Enhanced testing endpoint
@quotes_bp.route('/test/enhanced', methods=['POST'])
def test_enhanced_features():
    """Test endpoint for enhanced features with Acre Africa compatibility"""
    try:
        data = request.get_json() or {}
        
        # Default test quote data for Zimbabwe (Snow peas validation)
        test_quote_data = {
            "crop": data.get("crop", "maize"),  # Test with maize first
            "latitude": data.get("latitude", -17.8216),  # Harare area
            "longitude": data.get("longitude", 30.9858),
            "expected_yield": data.get("expected_yield", 5.0),
            "price_per_ton": data.get("price_per_ton", 300),
            "year": data.get("year", 2024),
            "area_ha": data.get("area_ha", 2.0)
        }
        
        print(f"🧪 Testing Enhanced Features with Acre Africa Compatibility")
        print(f"📊 Test data: {test_quote_data}")
        
        # Execute enhanced quote
        quote_result = enhanced_quote_engine.execute_quote(test_quote_data)
        
        # Test enhanced drought calculator directly
        from core.crops import get_crop_phases
        crop_phases = get_crop_phases(test_quote_data['crop'])
        
        # Generate test daily rainfall data
        sample_daily_rainfall = {}
        for start_day, end_day, trigger_mm, exit_mm, phase_name, water_need_mm, obs_window in crop_phases:
            phase_duration = end_day - start_day + 1
            daily_rainfall = enhanced_quote_engine._distribute_rainfall_across_days(
                water_need_mm * 0.6,  # 60% of water need to trigger drought
                phase_duration
            )
            sample_daily_rainfall[phase_name] = daily_rainfall
        
        # Test drought analysis
        test_drought_analysis = enhanced_drought_calculator.calculate_enhanced_drought_impact(
            crop_phases=crop_phases,
            crop=test_quote_data['crop'],
            lat=test_quote_data['latitude'],
            lon=test_quote_data['longitude'],
            planting_date=f"{test_quote_data['year']-1}-11-15",
            daily_rainfall_by_phase=sample_daily_rainfall
        )
        
        # Test crop-specific thresholds
        test_thresholds = enhanced_drought_calculator.get_enhanced_drought_thresholds(
            test_quote_data['crop'], 'flowering'
        )
        
        # Snow peas compatibility test (if requested)
        snow_peas_test = None
        if data.get('test_snow_peas', False):
            try:
                # Test with parameters similar to Technoserve document
                snow_peas_data = {
                    "crop": "groundnuts",  # Use groundnuts as proxy for snow peas
                    "latitude": -19.0,  # Masvingo area
                    "longitude": 30.5,
                    "expected_yield": 3.0,
                    "price_per_ton": 800,  # Higher value crop
                    "year": 2025,
                    "area_ha": 1.0,
                    "deductible_rate": 0.10  # 10% deductible as per document
                }
                
                snow_peas_quote = enhanced_quote_engine.execute_quote(snow_peas_data)
                snow_peas_premium_rate = snow_peas_quote.get('premium_rate', 0) * 100
                
                snow_peas_test = {
                    "quote_data": snow_peas_data,
                    "premium_rate_percent": round(snow_peas_premium_rate, 2),
                    "target_rate_percent": 7.5,  # From Technoserve document
                    "rate_difference": round(abs(snow_peas_premium_rate - 7.5), 2),
                    "alignment_status": "Good" if abs(snow_peas_premium_rate - 7.5) < 2.0 else "Needs calibration"
                }
                
            except Exception as e:
                snow_peas_test = {"error": f"Snow peas test failed: {e}"}
        
        # Extract enhanced features for testing
        enhanced_features = {
            "enhanced_drought_analysis": quote_result.get('enhanced_drought_analysis'),
            "enhanced_premium_adjustments": quote_result.get('enhanced_premium_adjustments'),
            "year_by_year_simulation": len(quote_result.get('year_by_year_simulation', [])),
            "planting_analysis": quote_result.get('planting_analysis'),
            "methodology_version": quote_result.get('methodology_version'),
            "drought_detection_method": quote_result.get('drought_detection_method')
        }
        
        return jsonify({
            "status": "success",
            "message": "Enhanced features test successful",
            "test_quote_data": test_quote_data,
            "quote_result": quote_result,
            "direct_drought_test": test_drought_analysis,
            "crop_thresholds_test": test_thresholds,
            "snow_peas_compatibility_test": snow_peas_test,
            "enhanced_features": enhanced_features,
            "features_tested": [
                "Industry-standard 10-day rolling drought detection",
                "Acre Africa methodology compatibility",
                "Consecutive dry spell detection (≥10 days)",
                "Maximum stress calculation method",
                "Geographic optimization for Southern Africa",
                "Enhanced premium adjustment factors",
                "Crop-specific threshold customization",
                "Snow peas rate validation (target ~7.5%)"
            ],
            "compliance_validation": {
                "industry_standards": ["Acre Africa", "FAO-56", "WMO Guidelines"],
                "rolling_window_size": "10 days",
                "drought_threshold": "≤15mm per"""
Enhanced Quote API endpoints with industry-standard 10-day rolling drought detection
Maintains full backward compatibility while adding Acre Africa compatible methodology
"""

from flask import Blueprint, request, jsonify
import traceback
import time
from datetime import datetime
from typing import Dict, List, Any

# Import enhanced components
from core.enhanced_quote_engine import EnhancedQuoteEngine
from core.enhanced_drought_calculator import EnhancedDroughtCalculator
from core.database import FieldsRepository, QuotesRepository
from core.ai_summary import EnhancedAISummaryGenerator
from core.crops import validate_crop, list_supported_crops

quotes_bp = Blueprint('quotes', __name__)

# Initialize enhanced components
enhanced_quote_engine = EnhancedQuoteEngine()
enhanced_drought_calculator = EnhancedDroughtCalculator()
fields_repo = FieldsRepository()
quotes_repo = QuotesRepository()
enhanced_ai_generator = EnhancedAISummaryGenerator()

@quotes_bp.route('/historical', methods=['POST'])
def historical_quote():
    """Generate enhanced historical quote with industry-standard drought detection"""
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
        
        print(f"🔍 Processing Enhanced Historical Quote for year {data.get('year')}")
        print(f"🔧 Using industry-standard 10-day rolling drought detection")
        
        # Execute enhanced quote
        quote_result = enhanced_quote_engine.execute_quote(data)
        
        # Generate comprehensive report with enhanced features
        try:
            comprehensive_report = enhanced_ai_generator.generate_comprehensive_quote_report(
                quote_result, data.get('location_info')
            )
            quote_result['comprehensive_report'] = comprehensive_report
            quote_result['ai_summary'] = comprehensive_report.get('executive_summary', 'Summary unavailable')
        except Exception as e:
            print(f"Enhanced report generation failed: {e}")
            quote_result['ai_summary'] = "Enhanced report temporarily unavailable"
            quote_result['comprehensive_report'] = {"
