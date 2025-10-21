ton' and value > 2000:
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
                if current_month > 3:
                    validation_warnings.append(f"Quote for {year} may be late in season (current month: {current_month})")
                elif current_month < 8:
                    validation_warnings.append(f"Consider quoting for {year + 1} season instead")
        
        # Coordinate validation for Southern Africa
        if 'latitude' in data and 'longitude' in data:
            try:
                lat = float(data['latitude'])
                lon = float(data['longitude'])
                if not (-25 <= lat <= -15 and 25 <= lon <= 35):
                    validation_warnings.append("Coordinates appear to be outside Southern Africa region")
            except:
                pass
        
        if validation_errors:
            return jsonify({
                "status": "error",
                "validation_errors": validation_errors,
                "validation_warnings": validation_warnings
            }), 400
        
        # Determine appropriate quote type
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
        print(f"ERROR: Validation error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

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
            # Extract field name from quote data
            field_name = quote_data.get('field_name') or quote_data.get('field_info', {}).get('name', 'Target Field')
            
            comprehensive_report = enterprise_summary_generator.generate_comprehensive_executive_summary(
                quote_data, 
                field_name=field_name
            )
            
            return jsonify({
                "status": "success",
                "quote_id": quote_id,
                "report": {
                    "executive_summary": comprehensive_report
                },
                "generated_at": datetime.utcnow().isoformat(),
                "report_version": "4.0-Enterprise"
            })
            
        except Exception as e:
            print(f"ERROR: Report generation failed: {e}")
            print(f"ERROR: {traceback.format_exc()}")
            return jsonify({
                "status": "error",
                "message": "Report generation failed",
                "error": str(e)
            }), 500
        
    except Exception as e:
        print(f"ERROR: Detailed report error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@quotes_bp.route('/debug/field/<int:field_id>', methods=['GET'])
def debug_field_data(field_id):
    """Debug endpoint to see what field data structure looks like"""
    try:
        field_data = fields_repo.get_field_by_id(field_id)
        
        if not field_data:
            return jsonify({
                "status": "error",
                "message": f"Field {field_id} not found"
            }), 404
        
        # Extract field name with all possible methods
        field_name_attempts = {
            'name': field_data.get('name'),
            'field_name': field_data.get('field_name'),
            'fieldName': field_data.get('fieldName'),
            'label': field_data.get('label'),
            'field_label': field_data.get('field_label')
        }
        
        # Determine which one worked
        field_name = None
        field_name_source = None
        for key, value in field_name_attempts.items():
            if value and str(value).strip():
                field_name = value
                field_name_source = key
                break
        
        return jsonify({
            "status": "success",
            "field_id": field_id,
            "field_data": field_data,
            "field_data_keys": list(field_data.keys()),
            "field_name_attempts": field_name_attempts,
            "extracted_field_name": field_name,
            "field_name_source": field_name_source,
            "recommendation": f"Use field_data.get('{field_name_source}')" if field_name_source else "No field name found in database"
        })
        
    except Exception as e:
        print(f"ERROR: Debug field data error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@quotes_bp.route('/crops', methods=['GET'])
def get_supported_crops():
    """Get list of supported crops"""
    try:
        crops = list_supported_crops()
        return jsonify({
            "status": "success",
            "crops": crops,
            "total": len(crops)
        })
    except Exception as e:
        print(f"ERROR: Get crops error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
