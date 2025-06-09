"""
Crop configuration and phenology management
"""

from typing import Dict, List, Tuple, Any

# Enhanced multi-crop configuration
CROP_CONFIG = {
    "maize": {
        "phases": [
            # (start_day, end_day, trigger_mm, exit_mm, phase_name, water_need_mm, observation_window_days)
            (0, 14, 25, 5, "Emergence", 30, 10),
            (15, 49, 60, 15, "Vegetative", 80, 10), 
            (50, 84, 80, 20, "Flowering", 100, 10),
            (85, 120, 70, 10, "Grain Fill", 90, 10)
        ],
        "kc_values": {"initial": 0.3, "mid": 1.2, "late": 0.5},
        "phase_weights": [0.15, 0.25, 0.40, 0.20],
        "planting_window": {"early_start": 11, "early_end": 12, "late_start": 12, "late_end": 1},
        "default_planting_date": "11-15",
        "total_season_days": 120,
        "description": "Maize (corn) - Primary staple crop across Africa"
    },
    "soyabeans": {
        "phases": [
            (0, 14, 20, 3, "Emergence", 25, 10),
            (15, 42, 55, 12, "Vegetative", 70, 10),
            (43, 77, 75, 18, "Flowering", 95, 10),
            (78, 115, 65, 8, "Pod Fill", 85, 10)
        ],
        "kc_values": {"initial": 0.35, "mid": 1.15, "late": 0.6},
        "phase_weights": [0.15, 0.25, 0.40, 0.20],
        "planting_window": {"early_start": 11, "early_end": 12, "late_start": 12, "late_end": 1},
        "default_planting_date": "11-20",
        "total_season_days": 115,
        "description": "Soybeans - Important protein crop and cash crop"
    },
    "sorghum": {
        "phases": [
            (0, 12, 20, 3, "Emergence", 25, 10),
            (13, 38, 50, 10, "Vegetative", 65, 10),
            (39, 73, 70, 15, "Flowering", 85, 10),
            (74, 105, 60, 8, "Grain Fill", 75, 10)
        ],
        "kc_values": {"initial": 0.3, "mid": 1.05, "late": 0.55},
        "phase_weights": [0.15, 0.25, 0.40, 0.20],
        "planting_window": {"early_start": 11, "early_end": 12, "late_start": 12, "late_end": 1},
        "default_planting_date": "11-15",
        "total_season_days": 105,
        "description": "Sorghum - Drought-tolerant cereal crop"
    },
    "cotton": {
        "phases": [
            (0, 15, 22, 4, "Emergence", 28, 10),
            (16, 55, 55, 12, "Vegetative", 75, 10),
            (56, 90, 85, 20, "Flowering", 110, 10),
            (91, 130, 75, 10, "Boll Fill", 95, 10)
        ],
        "kc_values": {"initial": 0.35, "mid": 1.15, "late": 0.6},
        "phase_weights": [0.15, 0.25, 0.40, 0.20],
        "planting_window": {"early_start": 11, "early_end": 12, "late_start": 12, "late_end": 1},
        "default_planting_date": "11-10",
        "total_season_days": 130,
        "description": "Cotton - Major cash crop for export markets"
    },
    "groundnuts": {
        "phases": [
            (0, 12, 18, 3, "Emergence", 22, 10),
            (13, 38, 45, 10, "Vegetative", 60, 10),
            (39, 70, 70, 15, "Flowering", 85, 10),
            (71, 100, 55, 8, "Pod Fill", 70, 10)
        ],
        "kc_values": {"initial": 0.4, "mid": 1.1, "late": 0.7},
        "phase_weights": [0.15, 0.25, 0.40, 0.20],
        "planting_window": {"early_start": 11, "early_end": 12, "late_start": 12, "late_end": 1},
        "default_planting_date": "11-25",
        "total_season_days": 100,
        "description": "Groundnuts (peanuts) - Important protein and oil crop"
    },
    "wheat": {
        "phases": [
            (0, 12, 15, 2, "Emergence", 20, 10),
            (13, 42, 40, 8, "Vegetative", 55, 10),
            (43, 70, 65, 12, "Flowering", 80, 10),
            (71, 95, 50, 5, "Grain Fill", 65, 10)
        ],
        "kc_values": {"initial": 0.3, "mid": 1.15, "late": 0.25},
        "phase_weights": [0.15, 0.25, 0.40, 0.20],
        "planting_window": {"early_start": 11, "early_end": 12, "late_start": 12, "late_end": 1},
        "default_planting_date": "11-20",
        "total_season_days": 95,
        "description": "Wheat - Cool season cereal crop"
    },
    "barley": {
        "phases": [
            (0, 12, 15, 2, "Emergence", 20, 10),
            (13, 42, 40, 8, "Vegetative", 55, 10),
            (43, 70, 65, 12, "Flowering", 80, 10),
            (71, 95, 50, 5, "Grain Fill", 65, 10)
        ],
        "kc_values": {"initial": 0.3, "mid": 1.05, "late": 0.25},
        "phase_weights": [0.15, 0.25, 0.40, 0.20],
        "planting_window": {"early_start": 11, "early_end": 12, "late_start": 12, "late_end": 1},
        "default_planting_date": "11-20",
        "total_season_days": 95,
        "description": "Barley - Hardy cereal crop for cooler areas"
    },
    "millet": {
        "phases": [
            (0, 10, 12, 2, "Emergence", 18, 10),
            (11, 38, 35, 8, "Vegetative", 50, 10),
            (39, 65, 60, 12, "Flowering", 75, 10),
            (66, 95, 45, 5, "Grain Fill", 60, 10)
        ],
        "kc_values": {"initial": 0.3, "mid": 1.0, "late": 0.5},
        "phase_weights": [0.15, 0.25, 0.40, 0.20],
        "planting_window": {"early_start": 11, "early_end": 12, "late_start": 12, "late_end": 1},
        "default_planting_date": "11-10",
        "total_season_days": 95,
        "description": "Millet - Highly drought-tolerant cereal"
    },
    "tobacco": {
        "phases": [
            (0, 14, 22, 4, "Emergence", 28, 10),
            (15, 50, 55, 12, "Vegetative", 75, 10),
            (51, 80, 75, 18, "Flowering", 95, 10),
            (81, 120, 65, 8, "Maturation", 80, 10)
        ],
        "kc_values": {"initial": 0.4, "mid": 1.2, "late": 0.65},
        "phase_weights": [0.15, 0.25, 0.40, 0.20],
        "planting_window": {"early_start": 11, "early_end": 12, "late_start": 12, "late_end": 1},
        "default_planting_date": "11-05",
        "total_season_days": 120,
        "description": "Tobacco - High-value cash crop"
    }
}

# Agroecological Zone Configuration
AGROECOLOGICAL_ZONES = {
    "aez_3_midlands": {
        "name": "AEZ 3 (Midlands)",
        "phase_weight_adjustments": [1.0, 1.0, 1.0, 1.0],
        "description": "Balanced rainfall patterns, moderate drought risk",
        "primary_risk": "Balanced drought/excess",
        "annual_rainfall_range": "750-1000mm"
    },
    "aez_4_masvingo": {
        "name": "AEZ 4 (Masvingo)", 
        "phase_weight_adjustments": [1.33, 0.67, 1.43, 0.57],
        "description": "Semi-arid zone with high drought risk",
        "primary_risk": "Drought",
        "annual_rainfall_range": "450-650mm"
    },
    "aez_5_lowveld": {
        "name": "AEZ 5 (Lowveld)",
        "phase_weight_adjustments": [1.5, 0.5, 1.5, 0.5],
        "description": "Hot, dry lowveld with extreme drought risk",
        "primary_risk": "Severe drought",
        "annual_rainfall_range": "300-500mm"
    },
    "auto_detect": {
        "name": "Auto-detect (Standard)",
        "phase_weight_adjustments": [1.0, 1.0, 1.0, 1.0],
        "description": "Standard weighting for automatic zone detection",
        "primary_risk": "Standard weighting",
        "annual_rainfall_range": "Variable"
    }
}

# Crop aliases for user-friendly input
CROP_ALIASES = {
    "corn": "maize",
    "soya": "soyabeans", 
    "soy": "soyabeans",
    "soybeans": "soyabeans",
    "peanuts": "groundnuts",
    "groundnut": "groundnuts"
}

def validate_crop(crop: str) -> str:
    """
    Validate and normalize crop name
    
    Args:
        crop: Crop name (case insensitive)
        
    Returns:
        str: Normalized crop name
        
    Raises:
        ValueError: If crop is not supported
    """
    if not crop:
        return "maize"  # Default crop
    
    crop_lower = crop.lower().strip()
    
    # Check direct match
    if crop_lower in CROP_CONFIG:
        return crop_lower
    
    # Check aliases
    if crop_lower in CROP_ALIASES:
        return CROP_ALIASES[crop_lower]
    
    # If not found, raise error with suggestions
    available_crops = list(CROP_CONFIG.keys())
    raise ValueError(f"Unsupported crop: '{crop}'. Available crops: {', '.join(available_crops)}")

def get_crop_config(crop: str) -> Dict[str, Any]:
    """
    Get complete crop configuration
    
    Args:
        crop: Crop name
        
    Returns:
        dict: Complete crop configuration
    """
    validated_crop = validate_crop(crop)
    return CROP_CONFIG[validated_crop]

def get_crop_phases(crop: str) -> List[Tuple[int, int, int, int, str, int, int]]:
    """
    Get crop phases configuration
    
    Args:
        crop: Crop name
        
    Returns:
        list: List of phase tuples (start_day, end_day, trigger_mm, exit_mm, phase_name, water_need_mm, observation_window_days)
    """
    config = get_crop_config(crop)
    return config["phases"]

def get_crop_phase_weights(crop: str, zone: str = "auto_detect") -> List[float]:
    """
    Get phase weights adjusted for agroecological zone
    
    Args:
        crop: Crop name
        zone: Agroecological zone
        
    Returns:
        list: Adjusted phase weights
    """
    config = get_crop_config(crop)
    base_weights = config["phase_weights"]
    
    zone_config = AGROECOLOGICAL_ZONES.get(zone, AGROECOLOGICAL_ZONES["auto_detect"])
    adjustments = zone_config["phase_weight_adjustments"]
    
    # Apply zone adjustments
    adjusted_weights = []
    for i, base_weight in enumerate(base_weights):
        adjusted_weight = base_weight * adjustments[i]
        adjusted_weights.append(adjusted_weight)
    
    return adjusted_weights

def get_zone_config(zone: str) -> Dict[str, Any]:
    """
    Get agroecological zone configuration
    
    Args:
        zone: Zone identifier
        
    Returns:
        dict: Zone configuration
    """
    return AGROECOLOGICAL_ZONES.get(zone, AGROECOLOGICAL_ZONES["auto_detect"])

def list_supported_crops() -> Dict[str, Dict[str, Any]]:
    """
    List all supported crops with their details
    
    Returns:
        dict: Crop configurations with metadata
    """
    crops_info = {}
    
    for crop_name, config in CROP_CONFIG.items():
        phases = config["phases"]
        crops_info[crop_name] = {
            "description": config.get("description", ""),
            "total_season_days": config.get("total_season_days", phases[-1][1]),
            "phases_count": len(phases),
            "default_planting_date": config.get("default_planting_date", "11-15"),
            "phase_names": [phase[4] for phase in phases],
            "phase_weights": config["phase_weights"],
            "kc_values": config["kc_values"],
            "planting_window": config["planting_window"]
        }
    
    return crops_info

def get_planting_window_dates(crop: str, year: int) -> Dict[str, str]:
    """
    Get planting window dates for a specific crop and year
    
    Args:
        crop: Crop name
        year: Year
        
    Returns:
        dict: Planting window dates
    """
    config = get_crop_config(crop)
    window = config["planting_window"]
    
    return {
        "early_start": f"{year}-{window['early_start']:02d}-01",
        "early_end": f"{year}-{window['early_end']:02d}-15",
        "late_start": f"{year}-{window['late_start']:02d}-01", 
        "late_end": f"{year + (1 if window['late_end'] == 1 else 0)}-{window['late_end']:02d}-31"
    }

class CropPhenologyCalculator:
    """Calculator for crop phenology stages and water requirements"""
    
    def __init__(self, crop: str, planting_date: str):
        """
        Initialize calculator
        
        Args:
            crop: Crop name
            planting_date: Planting date in YYYY-MM-DD format
        """
        self.crop = validate_crop(crop)
        self.config = get_crop_config(self.crop)
        self.planting_date = planting_date
        
    def get_phase_dates(self) -> List[Dict[str, Any]]:
        """
        Calculate actual dates for all crop phases
        
        Returns:
            list: Phase information with dates
        """
        from datetime import datetime, timedelta
        
        planting_dt = datetime.strptime(self.planting_date, '%Y-%m-%d')
        phases = []
        
        for i, (start_day, end_day, trigger_mm, exit_mm, phase_name, water_need_mm, obs_window) in enumerate(self.config["phases"]):
            start_date = planting_dt + timedelta(days=start_day)
            end_date = planting_dt + timedelta(days=end_day)
            
            phases.append({
                "phase_number": i + 1,
                "phase_name": phase_name,
                "start_day": start_day,
                "end_day": end_day,
                "start_date": start_date.strftime('%Y-%m-%d'),
                "end_date": end_date.strftime('%Y-%m-%d'),
                "duration_days": end_day - start_day + 1,
                "trigger_mm": trigger_mm,
                "exit_mm": exit_mm,
                "water_need_mm": water_need_mm,
                "observation_window_days": obs_window
            })
        
        return phases
    
    def calculate_water_requirements(self, et0_data: List[float] = None) -> Dict[str, float]:
        """
        Calculate crop water requirements using Kc values
        
        Args:
            et0_data: Reference evapotranspiration data (optional)
            
        Returns:
            dict: Water requirement calculations
        """
        kc_values = self.config["kc_values"]
        
        # If no ET0 data, use estimated values
        if et0_data is None:
            # Estimated daily ET0 for Southern Africa during growing season
            daily_et0 = 4.5  # mm/day average
            
            # Calculate seasonal water needs
            total_days = self.config.get("total_season_days", 120)
            initial_days = int(total_days * 0.25)
            mid_days = int(total_days * 0.5)
            late_days = total_days - initial_days - mid_days
            
            initial_etc = initial_days * daily_et0 * kc_values["initial"]
            mid_etc = mid_days * daily_et0 * kc_values["mid"]
            late_etc = late_days * daily_et0 * kc_values["late"]
            
            return {
                "initial_etc_mm": round(initial_etc, 1),
                "mid_etc_mm": round(mid_etc, 1), 
                "late_etc_mm": round(late_etc, 1),
                "total_etc_mm": round(initial_etc + mid_etc + late_etc, 1),
                "daily_et0_assumed": daily_et0
            }
        
        # Use provided ET0 data for more accurate calculation
        # Implementation would depend on ET0 data format
        return {"message": "Custom ET0 calculation not implemented yet"}

def get_crop_summary_stats() -> Dict[str, Any]:
    """
    Get summary statistics for all crops
    
    Returns:
        dict: Summary statistics
    """
    total_crops = len(CROP_CONFIG)
    total_zones = len(AGROECOLOGICAL_ZONES)
    
    # Calculate average season lengths
    season_lengths = [config.get("total_season_days", config["phases"][-1][1]) for config in CROP_CONFIG.values()]
    avg_season_length = sum(season_lengths) / len(season_lengths)
    
    return {
        "total_crops": total_crops,
        "total_zones": total_zones,
        "average_season_days": round(avg_season_length, 1),
        "shortest_season": min(season_lengths),
        "longest_season": max(season_lengths),
        "crop_types": {
            "cereals": ["maize", "sorghum", "wheat", "barley", "millet"],
            "legumes": ["soyabeans", "groundnuts"],
            "cash_crops": ["cotton", "tobacco"]
        }
    }
