"""
Fields API endpoints for field management
"""

from flask import Blueprint, request, jsonify
import traceback
from datetime import datetime

from core.database import FieldsRepository
from core.crops import validate_crop

fields_bp = Blueprint('fields', __name__)
fields_repo = FieldsRepository()

@fields_bp.route('', methods=['GET'])
def list_fields():
    """
    List fields with optional filtering
    
    Query parameters:
    - owner_entity_id: Filter by owner
    - crop: Filter by crop type
    - min_area: Minimum area in hectares
    - max_area: Maximum area in hectares
    - limit: Maximum results (default 100)
    """
    try:
        # Get query parameters
        owner_entity_id = request.args.get('owner_entity_id', type=int)
        crop = request.args.get('crop')
        min_area = request.args.get('min_area', type=float)
        max_area = request.args.get('max_area', type=float)
        limit = request.args.get('limit', 100, type=int)
        
        # Validate limit
        if limit > 1000:
            limit = 1000
        
        if owner_entity_id:
            # Get fields by owner
            fields = fields_repo.get_fields_by_owner(owner_entity_id, limit)
        else:
            # Search with filters
            filters = {}
            if crop:
                try:
                    validated_crop = validate_crop(crop)
                    filters['crop'] = validated_crop
                except ValueError as e:
                    return jsonify({
                        "status": "error",
                        "message": str(e)
                    }), 400
            
            if min_area is not None:
                filters['min_area'] = min_area
            if max_area is not None:
                filters['max_area'] = max_area
            if owner_entity_id:
                filters['owner_entity_id'] = owner_entity_id
            
            fields = fields_repo.search_fields(filters, limit)
        
        return jsonify({
            "status": "success",
            "fields": fields,
            "total": len(fields),
            "limit": limit
        })
        
    except Exception as e:
        print(f"List fields error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@fields_bp.route('/<int:field_id>', methods=['GET'])
def get_field(field_id):
    """Get field details by ID"""
    try:
        field_data = fields_repo.get_field_by_id(field_id)
        
        if not field_data:
            return jsonify({
                "status": "error",
                "message": f"Field {field_id} not found"
            }), 404
        
        return jsonify({
            "status": "success",
            "field": field_data
        })
        
    except Exception as e:
        print(f"Get field error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@fields_bp.route('', methods=['POST'])
def create_field():
    """
    Create a new field
    
    Request body:
    {
        "name": "North Field",
        "farmer_name": "John Doe",
        "farmer_phone": "+263771234567",
        "area_ha": 2.5,
        "location": "Harare North",
        "crop": "maize",
        "variety": "SC719",
        "planting_date": "2024-11-15",
        "irrigated": false,
        "latitude": -17.7888,
        "longitude": 30.6015,
        "owner_entity_id": 1,
        "farm_id": 101
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                "status": "error",
                "message": "Request body is required"
            }), 400
        
        # Validate required fields
        required_fields = ['name', 'latitude', 'longitude', 'area_ha']
        for field in required_fields:
            if field not in data:
                return jsonify({
                    "status": "error",
                    "message": f"Missing required field: {field}"
                }), 400
        
        # Validate coordinates
        try:
            lat = float(data['latitude'])
            lng = float(data['longitude'])
            if not (-90 <= lat <= 90):
                raise ValueError("Latitude must be between -90 and 90")
            if not (-180 <= lng <= 180):
                raise ValueError("Longitude must be between -180 and 180")
        except (ValueError, TypeError) as e:
            return jsonify({
                "status": "error",
                "message": f"Invalid coordinates: {str(e)}"
            }), 400
        
        # Validate area
        try:
            area_ha = float(data['area_ha'])
            if area_ha <= 0:
                raise ValueError("Area must be positive")
        except (ValueError, TypeError):
            return jsonify({
                "status": "error",
                "message": "Area must be a positive number"
            }), 400
        
        # Validate crop if provided
        if 'crop' in data:
            try:
                data['crop'] = validate_crop(data['crop'])
            except ValueError as e:
                return jsonify({
                    "status": "error",
                    "message": str(e)
                }), 400
        
        # Validate planting date if provided
        if 'planting_date' in data and data['planting_date']:
            try:
                datetime.strptime(data['planting_date'], '%Y-%m-%d')
            except ValueError:
                return jsonify({
                    "status": "error",
                    "message": "Planting date must be in YYYY-MM-DD format"
                }), 400
        
        # Create field
        field_id = fields_repo.create_field(data)
        
        if not field_id:
            return jsonify({
                "status": "error",
                "message": "Failed to create field"
            }), 500
        
        # Get created field data
        created_field = fields_repo.get_field_by_id(field_id)
        
        return jsonify({
            "status": "success",
            "message": "Field created successfully",
            "field_id": field_id,
            "field": created_field
        }), 201
        
    except Exception as e:
        print(f"Create field error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@fields_bp.route('/<int:field_id>', methods=['PUT'])
def update_field(field_id):
    """Update field details (placeholder - implement as needed)"""
    return jsonify({
        "status": "error",
        "message": "Field update functionality not implemented yet"
    }), 501

@fields_bp.route('/<int:field_id>', methods=['DELETE'])
def delete_field(field_id):
    """Delete field (placeholder - implement as needed)"""
    return jsonify({
        "status": "error",
        "message": "Field deletion functionality not implemented yet"
    }), 501

@fields_bp.route('/validate', methods=['POST'])
def validate_field_data():
    """
    Validate field data without creating
    
    Request body: Same as create_field
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                "status": "error",
                "message": "Request body is required"
            }), 400
        
        validation_errors = []
        
        # Check required fields
        required_fields = ['name', 'latitude', 'longitude', 'area_ha']
        for field in required_fields:
            if field not in data:
                validation_errors.append(f"Missing required field: {field}")
        
        # Validate coordinates
        if 'latitude' in data and 'longitude' in data:
            try:
                lat = float(data['latitude'])
                lng = float(data['longitude'])
                if not (-90 <= lat <= 90):
                    validation_errors.append("Latitude must be between -90 and 90")
                if not (-180 <= lng <= 180):
                    validation_errors.append("Longitude must be between -180 and 180")
            except (ValueError, TypeError):
                validation_errors.append("Latitude and longitude must be numbers")
        
        # Validate area
        if 'area_ha' in data:
            try:
                area_ha = float(data['area_ha'])
                if area_ha <= 0:
                    validation_errors.append("Area must be positive")
            except (ValueError, TypeError):
                validation_errors.append("Area must be a positive number")
        
        # Validate crop
        if 'crop' in data:
            try:
                validate_crop(data['crop'])
            except ValueError as e:
                validation_errors.append(str(e))
        
        # Validate planting date
        if 'planting_date' in data and data['planting_date']:
            try:
                datetime.strptime(data['planting_date'], '%Y-%m-%d')
            except ValueError:
                validation_errors.append("Planting date must be in YYYY-MM-DD format")
        
        if validation_errors:
            return jsonify({
                "status": "error",
                "validation_errors": validation_errors
            }), 400
        
        return jsonify({
            "status": "success",
            "message": "Field data is valid"
        })
        
    except Exception as e:
        print(f"Field validation error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@fields_bp.route('/import', methods=['POST'])
def import_fields():
    """
    Bulk import fields from CSV or JSON
    
    Request body:
    {
        "format": "json",  // or "csv"
        "data": [
            {
                "name": "Field 1",
                "latitude": -17.7888,
                "longitude": 30.6015,
                "area_ha": 2.5,
                "crop": "maize"
            }
        ]
    }
    """
    try:
        data = request.get_json()
        
        if not data or 'data' not in data:
            return jsonify({
                "status": "error",
                "message": "Missing 'data' field in request body"
            }), 400
        
        import_data = data['data']
        if not import_data:
            return jsonify({
                "status": "error",
                "message": "Import data cannot be empty"
            }), 400
        
        results = []
        successful_imports = 0
        
        for i, field_data in enumerate(import_data):
            try:
                # Validate required fields
                required_fields = ['name', 'latitude', 'longitude', 'area_ha']
                missing_fields = [f for f in required_fields if f not in field_data]
                
                if missing_fields:
                    results.append({
                        "row": i + 1,
                        "status": "error",
                        "message": f"Missing fields: {', '.join(missing_fields)}"
                    })
                    continue
                
                # Validate crop if provided
                if 'crop' in field_data:
                    try:
                        field_data['crop'] = validate_crop(field_data['crop'])
                    except ValueError as e:
                        results.append({
                            "row": i + 1,
                            "status": "error",
                            "message": str(e)
                        })
                        continue
                
                # Create field
                field_id = fields_repo.create_field(field_data)
                
                if field_id:
                    results.append({
                        "row": i + 1,
                        "status": "success",
                        "field_id": field_id,
                        "name": field_data['name']
                    })
                    successful_imports += 1
                else:
                    results.append({
                        "row": i + 1,
                        "status": "error",
                        "message": "Failed to create field"
                    })
                
            except Exception as e:
                results.append({
                    "row": i + 1,
                    "status": "error",
                    "message": str(e)
                })
        
        return jsonify({
            "status": "success",
            "total_rows": len(import_data),
            "successful_imports": successful_imports,
            "failed_imports": len(import_data) - successful_imports,
            "results": results
        })
        
    except Exception as e:
        print(f"Field import error: {traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
