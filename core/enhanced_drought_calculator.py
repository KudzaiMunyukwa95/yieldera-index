"""
Enhanced Agricultural Insurance Engine - Industry Standard Drought Detection Implementation
Compatible with Acre Africa methodology and international best practices
"""

import ee
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EnhancedDroughtCalculator:
    """
    Industry-standard drought calculator implementing 10-day rolling window analysis
    Compatible with Acre Africa methodology and international insurance standards
    """
    
    def __init__(self):
        """Initialize enhanced drought calculator with industry standards"""
        # Industry Standard Parameters
        self.ROLLING_WINDOW_SIZE = 10  # 10-day rolling window (industry standard)
        self.DROUGHT_TRIGGER_THRESHOLD = 15.0  # ≤15mm per 10-day window
        self.DRY_DAY_THRESHOLD = 1.0  # <1mm daily rainfall = dry day
        self.CONSECUTIVE_DROUGHT_TRIGGER = 10  # ≥10 consecutive dry days
        
        # Geographic Focus - Southern Africa
        self.GEOGRAPHIC_BOUNDS = {
            'lat_min': -25.0,
            'lat_max': -15.0, 
            'lon_min': 25.0,
            'lon_max': 35.0
        }
        
        # Enhanced stress calculation weights
        self.STRESS_WEIGHTS = {
            'cumulative': 0.4,     # 40% weight to cumulative stress
            'rolling_window': 0.4,  # 40% weight to rolling window stress
            'consecutive': 0.2      # 20% weight to consecutive drought stress
        }
        
        logger.info("✅ Enhanced Drought Calculator initialized with industry standards")
        logger.info(f"🔧 10-day rolling window, ≤{self.DROUGHT_TRIGGER_THRESHOLD}mm threshold")
        logger.info(f"🌍 Geographic focus: Southern Africa ({self.GEOGRAPHIC_BOUNDS})")
    
    def _analyze_rolling_10day_windows(self, daily_rainfall: List[float], 
                                     trigger_mm: float = 15.0) -> List[Dict[str, Any]]:
        """
        Analyze 10-day rolling windows for drought detection
        
        Args:
            daily_rainfall: List of daily rainfall values (mm)
            trigger_mm: Threshold for drought trigger (default 15mm)
            
        Returns:
            List of rolling window analysis results
        """
        if len(daily_rainfall) < self.ROLLING_WINDOW_SIZE:
            logger.warning(f"Insufficient data: {len(daily_rainfall)} days < {self.ROLLING_WINDOW_SIZE}")
            return []
        
        rolling_windows = []
        
        # Calculate rolling 10-day windows
        for i in range(len(daily_rainfall) - self.ROLLING_WINDOW_SIZE + 1):
            window_data = daily_rainfall[i:i + self.ROLLING_WINDOW_SIZE]
            window_total = sum(window_data)
            
            # Drought stress calculation
            if window_total <= trigger_mm:
                stress_intensity = max(0, (trigger_mm - window_total) / trigger_mm)
                drought_detected = True
            else:
                stress_intensity = 0.0
                drought_detected = False
            
            window_result = {
                'window_index': i + 1,
                'start_day': i + 1,
                'end_day': i + self.ROLLING_WINDOW_SIZE,
                'total_rainfall_mm': round(window_total, 1),
                'drought_detected': drought_detected,
                'stress_intensity': round(stress_intensity, 3),
                'trigger_threshold': trigger_mm,
                'days_in_window': self.ROLLING_WINDOW_SIZE
            }
            
            rolling_windows.append(window_result)
        
        # Summary statistics
        drought_windows = [w for w in rolling_windows if w['drought_detected']]
        drought_frequency = len(drought_windows) / len(rolling_windows) * 100 if rolling_windows else 0
        
        logger.info(f"📊 Rolling window analysis: {len(drought_windows)}/{len(rolling_windows)} drought windows")
        logger.info(f"📈 Drought frequency: {drought_frequency:.1f}%")
        
        return rolling_windows
    
    def _find_max_consecutive_dry_days(self, daily_rainfall: List[float], 
                                     threshold_mm: float = 1.0) -> Dict[str, Any]:
        """
        Find maximum consecutive dry days
        
        Args:
            daily_rainfall: List of daily rainfall values (mm)
            threshold_mm: Threshold for dry day definition (default 1mm)
            
        Returns:
            Dictionary with consecutive dry day analysis
        """
        if not daily_rainfall:
            return {'max_consecutive_dry_days': 0, 'dry_periods': []}
        
        dry_periods = []
        current_dry_period = 0
        max_consecutive = 0
        
        for i, rainfall in enumerate(daily_rainfall):
            if rainfall < threshold_mm:
                current_dry_period += 1
                max_consecutive = max(max_consecutive, current_dry_period)
            else:
                if current_dry_period > 0:
                    dry_periods.append({
                        'start_day': i - current_dry_period + 1,
                        'end_day': i,
                        'duration_days': current_dry_period
                    })
                current_dry_period = 0
        
        # Handle case where dry period extends to end of data
        if current_dry_period > 0:
            dry_periods.append({
                'start_day': len(daily_rainfall) - current_dry_period + 1,
                'end_day': len(daily_rainfall),
                'duration_days': current_dry_period
            })
        
        # Calculate consecutive drought stress
        consecutive_stress = 0.0
        if max_consecutive >= self.CONSECUTIVE_DROUGHT_TRIGGER:
            excess_days = max_consecutive - self.CONSECUTIVE_DROUGHT_TRIGGER
            consecutive_stress = min(1.0, (excess_days + self.CONSECUTIVE_DROUGHT_TRIGGER) / 30.0)
        
        result = {
            'max_consecutive_dry_days': max_consecutive,
            'dry_periods': dry_periods,
            'consecutive_drought_detected': max_consecutive >= self.CONSECUTIVE_DROUGHT_TRIGGER,
            'consecutive_stress': round(consecutive_stress, 3),
            'dry_day_threshold': threshold_mm,
            'trigger_threshold': self.CONSECUTIVE_DROUGHT_TRIGGER
        }
        
        if max_consecutive >= self.CONSECUTIVE_DROUGHT_TRIGGER:
            logger.warning(f"🚨 Consecutive drought detected: {max_consecutive} dry days")
        
        return result
    
    def calculate_enhanced_drought_impact(self, crop_phases: List[Tuple], crop: str, 
                                        lat: float, lon: float, planting_date: str,
                                        daily_rainfall_by_phase: Dict[str, List[float]]) -> Dict[str, Any]:
        """
        Calculate enhanced drought impact using maximum stress methodology
        
        Args:
            crop_phases: List of crop phase tuples
            crop: Crop name
            lat: Latitude
            lon: Longitude  
            planting_date: Planting date string
            daily_rainfall_by_phase: Daily rainfall data by phase
            
        Returns:
            Enhanced drought impact analysis
        """
        logger.info(f"🔍 Calculating enhanced drought impact for {crop}")
        logger.info(f"📍 Location: {lat:.4f}, {lon:.4f}")
        logger.info(f"🌱 Planting date: {planting_date}")
        
        # Validate geographic bounds
        if not self._validate_geographic_bounds(lat, lon):
            logger.warning(f"⚠️ Location outside Southern Africa focus area")
        
        phase_analyses = []
        total_stress_components = {
            'cumulative_stress': 0.0,
            'rolling_window_stress': 0.0,
            'consecutive_stress': 0.0
        }
        
        for i, (start_day, end_day, trigger_mm, exit_mm, phase_name, water_need_mm, obs_window) in enumerate(crop_phases):
            phase_rainfall = daily_rainfall_by_phase.get(phase_name, [])
            
            if not phase_rainfall:
                logger.warning(f"⚠️ No rainfall data for phase: {phase_name}")
                continue
            
            # 1. Cumulative drought stress (traditional method)
            total_rainfall = sum(phase_rainfall)
            if total_rainfall < water_need_mm:
                cumulative_stress = (water_need_mm - total_rainfall) / water_need_mm
            else:
                cumulative_stress = 0.0
            
            # 2. Rolling window drought stress (industry standard)
            rolling_analysis = self._analyze_rolling_10day_windows(
                phase_rainfall, self.DROUGHT_TRIGGER_THRESHOLD
            )
            
            if rolling_analysis:
                max_window_stress = max(w['stress_intensity'] for w in rolling_analysis)
                drought_window_count = sum(1 for w in rolling_analysis if w['drought_detected'])
                rolling_stress_factor = drought_window_count / len(rolling_analysis)
            else:
                max_window_stress = 0.0
                rolling_stress_factor = 0.0
                drought_window_count = 0
            
            rolling_window_stress = max_window_stress * rolling_stress_factor
            
            # 3. Consecutive drought stress
            consecutive_analysis = self._find_max_consecutive_dry_days(
                phase_rainfall, self.DRY_DAY_THRESHOLD
            )
            consecutive_drought_stress = consecutive_analysis['consecutive_stress']
            
            # 4. Maximum stress calculation (industry standard)
            phase_stress = max(
                cumulative_stress * self.STRESS_WEIGHTS['cumulative'],
                rolling_window_stress * self.STRESS_WEIGHTS['rolling_window'],
                consecutive_drought_stress * self.STRESS_WEIGHTS['consecutive']
            )
            
            # Get phase weight from crops.py
            from core.crops import get_crop_phase_weights
            phase_weights = get_crop_phase_weights(crop)
            phase_weight = phase_weights[i] if i < len(phase_weights) else 0.25
            
            # Apply phase-specific multipliers based on crop sensitivity
            if 'flowering' in phase_name.lower() or 'grain' in phase_name.lower():
                sensitivity_multiplier = 1.3  # Critical phases get higher weight
            elif 'emergence' in phase_name.lower():
                sensitivity_multiplier = 1.1  # Emergence is also sensitive
            else:
                sensitivity_multiplier = 1.0
            
            weighted_phase_stress = phase_stress * phase_weight * sensitivity_multiplier
            
            # Store stress components for aggregation
            total_stress_components['cumulative_stress'] += cumulative_stress * phase_weight
            total_stress_components['rolling_window_stress'] += rolling_window_stress * phase_weight
            total_stress_components['consecutive_stress'] += consecutive_drought_stress * phase_weight
            
            phase_analysis = {
                'phase_name': phase_name,
                'phase_number': i + 1,
                'start_day': start_day,
                'end_day': end_day,
                'duration_days': end_day - start_day + 1,
                'total_rainfall_mm': round(total_rainfall, 1),
                'water_need_mm': water_need_mm,
                'rainfall_deficit_mm': max(0, water_need_mm - total_rainfall),
                'phase_weight': round(phase_weight, 3),
                'sensitivity_multiplier': sensitivity_multiplier,
                
                # Stress analysis
                'cumulative_stress': round(cumulative_stress, 3),
                'rolling_window_stress': round(rolling_window_stress, 3),
                'consecutive_drought_stress': round(consecutive_drought_stress, 3),
                'maximum_stress': round(phase_stress, 3),
                'weighted_phase_stress': round(weighted_phase_stress, 3),
                
                # Detailed analysis
                'rolling_window_analysis': {
                    'total_windows': len(rolling_analysis),
                    'drought_windows': drought_window_count,
                    'max_window_stress': round(max_window_stress, 3),
                    'window_drought_frequency': round(drought_window_count / len(rolling_analysis) * 100, 1) if rolling_analysis else 0
                },
                'consecutive_analysis': consecutive_analysis,
                
                # Critical thresholds
                'drought_triggers': {
                    'cumulative_threshold': water_need_mm,
                    'rolling_window_threshold': self.DROUGHT_TRIGGER_THRESHOLD,
                    'consecutive_dry_threshold': self.CONSECUTIVE_DROUGHT_TRIGGER
                }
            }
            
            phase_analyses.append(phase_analysis)
            
            logger.info(f"📊 {phase_name}: Stress={phase_stress:.3f}, Weight={phase_weight:.3f}")
        
        # Calculate overall drought impact using maximum stress methodology
        if phase_analyses:
            total_weighted_stress = sum(p['weighted_phase_stress'] for p in phase_analyses)
            
            # Apply maximum stress calculation across all components
            max_cumulative_stress = total_stress_components['cumulative_stress']
            max_rolling_stress = total_stress_components['rolling_window_stress'] 
            max_consecutive_stress = total_stress_components['consecutive_stress']
            
            # Industry standard: MAX(cumulative, rolling_window, consecutive) * 100
            overall_drought_impact = max(
                max_cumulative_stress,
                max_rolling_stress,
                max_consecutive_stress
            ) * 100
            
            # Cap at 100% and apply minimum threshold
            overall_drought_impact = min(100.0, max(0.0, overall_drought_impact))
        else:
            overall_drought_impact = 0.0
            total_weighted_stress = 0.0
        
        # Enhanced risk categorization
        if overall_drought_impact < 5:
            risk_category = "Minimal"
            risk_color = "green"
        elif overall_drought_impact < 15:
            risk_category = "Low"
            risk_color = "yellow"
        elif overall_drought_impact < 30:
            risk_category = "Moderate" 
            risk_color = "orange"
        elif overall_drought_impact < 50:
            risk_category = "High"
            risk_color = "red"
        else:
            risk_category = "Severe"
            risk_color = "darkred"
        
        enhanced_drought_analysis = {
            'methodology': 'enhanced_industry_standard_v1.0',
            'compatible_with': 'Acre Africa methodology',
            'calculation_method': 'MAX(cumulative_stress, rolling_window_stress, consecutive_drought_stress)',
            
            # Main results
            'overall_drought_impact_percent': round(overall_drought_impact, 2),
            'total_weighted_stress': round(total_weighted_stress, 3),
            'risk_category': risk_category,
            'risk_color': risk_color,
            
            # Stress component breakdown
            'stress_components': {
                'cumulative_stress_percent': round(max_cumulative_stress * 100, 2),
                'rolling_window_stress_percent': round(max_rolling_stress * 100, 2),
                'consecutive_stress_percent': round(max_consecutive_stress * 100, 2),
                'dominant_stress_type': max(
                    total_stress_components.items(), 
                    key=lambda x: x[1]
                )[0] if total_stress_components else 'none'
            },
            
            # Phase-by-phase analysis
            'phase_analyses': phase_analyses,
            
            # Summary statistics
            'summary_statistics': {
                'total_phases_analyzed': len(phase_analyses),
                'phases_with_drought_stress': len([p for p in phase_analyses if p['maximum_stress'] > 0.1]),
                'most_affected_phase': max(phase_analyses, key=lambda x: x['maximum_stress'])['phase_name'] if phase_analyses else None,
                'critical_periods_count': len([p for p in phase_analyses if p['maximum_stress'] > 0.5]),
                'average_phase_stress': round(sum(p['maximum_stress'] for p in phase_analyses) / len(phase_analyses), 3) if phase_analyses else 0
            },
            
            # Industry compliance
            'industry_standards': {
                'rolling_window_size_days': self.ROLLING_WINDOW_SIZE,
                'drought_trigger_threshold_mm': self.DROUGHT_TRIGGER_THRESHOLD,
                'consecutive_dry_threshold_days': self.CONSECUTIVE_DROUGHT_TRIGGER,
                'geographic_focus': 'Southern Africa',
                'compliance_standards': ['Acre Africa', 'FAO-56', 'WMO Guidelines']
            },
            
            # Metadata
            'calculation_metadata': {
                'crop': crop,
                'planting_date': planting_date,
                'latitude': lat,
                'longitude': lon,
                'geographic_validation': self._validate_geographic_bounds(lat, lon),
                'calculation_timestamp': datetime.utcnow().isoformat(),
                'stress_weights': self.STRESS_WEIGHTS
            }
        }
        
        logger.info(f"✅ Enhanced drought analysis completed")
        logger.info(f"📊 Overall drought impact: {overall_drought_impact:.2f}% ({risk_category})")
        logger.info(f"🎯 Dominant stress type: {enhanced_drought_analysis['stress_components']['dominant_stress_type']}")
        
        return enhanced_drought_analysis
    
    def _validate_geographic_bounds(self, lat: float, lon: float) -> bool:
        """Validate coordinates are within Southern Africa focus area"""
        return (self.GEOGRAPHIC_BOUNDS['lat_min'] <= lat <= self.GEOGRAPHIC_BOUNDS['lat_max'] and
                self.GEOGRAPHIC_BOUNDS['lon_min'] <= lon <= self.GEOGRAPHIC_BOUNDS['lon_max'])
    
    def get_enhanced_drought_thresholds(self, crop: str, growth_stage: str) -> Dict[str, float]:
        """
        Get enhanced drought thresholds for specific crop and growth stage
        
        Args:
            crop: Crop name
            growth_stage: Growth stage name
            
        Returns:
            Dictionary of thresholds for this crop/stage combination
        """
        # Crop-specific adjustments to industry standards
        crop_adjustments = {
            'maize': {'multiplier': 1.0, 'sensitivity': 'high'},
            'soyabeans': {'multiplier': 0.9, 'sensitivity': 'medium'},
            'sorghum': {'multiplier': 0.8, 'sensitivity': 'low'},  # More drought tolerant
            'cotton': {'multiplier': 1.1, 'sensitivity': 'high'},
            'groundnuts': {'multiplier': 0.9, 'sensitivity': 'medium'},
            'wheat': {'multiplier': 1.0, 'sensitivity': 'medium'},
            'tobacco': {'multiplier': 1.2, 'sensitivity': 'very_high'}
        }
        
        # Growth stage adjustments
        stage_adjustments = {
            'emergence': {'multiplier': 1.1, 'critical': True},
            'vegetative': {'multiplier': 1.0, 'critical': False},
            'flowering': {'multiplier': 1.3, 'critical': True},
            'grain_fill': {'multiplier': 1.2, 'critical': True},
            'maturation': {'multiplier': 0.8, 'critical': False}
        }
        
        crop_adj = crop_adjustments.get(crop.lower(), {'multiplier': 1.0, 'sensitivity': 'medium'})
        stage_adj = stage_adjustments.get(growth_stage.lower(), {'multiplier': 1.0, 'critical': False})
        
        # Calculate adjusted thresholds
        base_threshold = self.DROUGHT_TRIGGER_THRESHOLD
        adjusted_threshold = base_threshold * crop_adj['multiplier'] * stage_adj['multiplier']
        
        consecutive_threshold = self.CONSECUTIVE_DROUGHT_TRIGGER
        if stage_adj['critical']:
            consecutive_threshold = max(7, int(consecutive_threshold * 0.7))  # More sensitive for critical stages
        
        return {
            'rolling_window_threshold_mm': round(adjusted_threshold, 1),
            'consecutive_dry_threshold_days': consecutive_threshold,
            'dry_day_threshold_mm': self.DRY_DAY_THRESHOLD,
            'crop_sensitivity': crop_adj['sensitivity'],
            'stage_criticality': 'critical' if stage_adj['critical'] else 'normal',
            'base_threshold_mm': base_threshold,
            'crop_multiplier': crop_adj['multiplier'],
            'stage_multiplier': stage_adj['multiplier']
        }
    
    def calculate_premium_adjustment_factor(self, drought_analysis: Dict[str, Any]) -> Dict[str, float]:
        """
        Calculate premium adjustment factors based on enhanced drought analysis
        
        Args:
            drought_analysis: Enhanced drought analysis results
            
        Returns:
            Premium adjustment factors
        """
        base_impact = drought_analysis['overall_drought_impact_percent']
        stress_components = drought_analysis['stress_components']
        
        # Base premium adjustment
        if base_impact < 10:
            base_adjustment = 1.0  # No adjustment for low risk
        elif base_impact < 25:
            base_adjustment = 1.1  # 10% increase for moderate risk
        elif base_impact < 50:
            base_adjustment = 1.25  # 25% increase for high risk
        else:
            base_adjustment = 1.5  # 50% increase for severe risk
        
        # Stress type adjustments
        dominant_stress = stress_components['dominant_stress_type']
        stress_adjustments = {
            'rolling_window_stress': 1.05,  # Industry standard method
            'consecutive_stress': 1.1,      # Higher risk for consecutive drought
            'cumulative_stress': 1.0        # Traditional method baseline
        }
        
        stress_adjustment = stress_adjustments.get(dominant_stress, 1.0)
        
        # Geographic adjustment (Southern Africa focus)
        geographic_adjustment = 1.02  # Slight increase for regional climate risk
        
        # Final adjustment factor
        total_adjustment = base_adjustment * stress_adjustment * geographic_adjustment
        
        return {
            'total_premium_adjustment': round(total_adjustment, 3),
            'base_risk_adjustment': round(base_adjustment, 3),
            'stress_type_adjustment': round(stress_adjustment, 3),
            'geographic_adjustment': round(geographic_adjustment, 3),
            'recommended_premium_increase_percent': round((total_adjustment - 1.0) * 100, 1),
            'risk_justification': f"Based on {base_impact:.1f}% drought impact with {dominant_stress} dominance"
        }

# Integration class for backward compatibility
class EnhancedQuoteEngineIntegration:
    """
    Integration layer to maintain backward compatibility while adding enhanced features
    """
    
    def __init__(self, quote_engine):
        """Initialize with existing quote engine"""
        self.quote_engine = quote_engine
        self.enhanced_drought_calculator = EnhancedDroughtCalculator()
        logger.info("✅ Enhanced Quote Engine Integration initialized")
    
    def execute_enhanced_quote(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute quote with enhanced drought detection while maintaining compatibility
        
        Args:
            request_data: Quote request parameters
            
        Returns:
            Enhanced quote with backward compatibility
        """
        # Execute standard quote first
        standard_quote = self.quote_engine.execute_quote(request_data)
        
        # Add enhanced drought analysis if supported
        try:
            enhanced_analysis = self._add_enhanced_drought_analysis(standard_quote, request_data)
            standard_quote['enhanced_drought_analysis'] = enhanced_analysis
            standard_quote['methodology_version'] = 'enhanced_v1.0_acre_compatible'
            
            # Add premium adjustments based on enhanced analysis
            premium_adjustments = self.enhanced_drought_calculator.calculate_premium_adjustment_factor(
                enhanced_analysis
            )
            standard_quote['enhanced_premium_adjustments'] = premium_adjustments
            
            logger.info("✅ Enhanced drought analysis added to quote")
            
        except Exception as e:
            logger.warning(f"⚠️ Enhanced analysis failed, using standard methodology: {e}")
            standard_quote['enhanced_drought_analysis'] = None
            standard_quote['methodology_version'] = 'standard_v2.5_fallback'
        
        return standard_quote
    
    def _add_enhanced_drought_analysis(self, quote_result: Dict[str, Any], 
                                     request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Add enhanced drought analysis to existing quote"""
        
        # Extract necessary data from quote result
        crop = request_data.get('crop', 'maize')
        lat = request_data.get('latitude', 0)
        lon = request_data.get('longitude', 0)
        year = request_data.get('year', datetime.now().year)
        
        # Get crop phases
        from core.crops import get_crop_phases
        crop_phases = get_crop_phases(crop)
        
        # Extract daily rainfall data from year-by-year simulation
        year_by_year = quote_result.get('year_by_year_simulation', [])
        if not year_by_year:
            raise ValueError("No year-by-year simulation data available")
        
        # Use most recent year for enhanced analysis
        recent_year_data = year_by_year[-1] if year_by_year else {}
        rainfall_by_phase = recent_year_data.get('rainfall_mm_by_phase', {})
        
        # Convert phase totals to daily data (estimated distribution)
        daily_rainfall_by_phase = {}
        for phase_name, total_rainfall in rainfall_by_phase.items():
            # Find phase duration
            phase_duration = 30  # Default
            for phase_tuple in crop_phases:
                if phase_tuple[4] == phase_name:  # phase_name is at index 4
                    phase_duration = phase_tuple[1] - phase_tuple[0] + 1
                    break
            
            # Distribute rainfall across days (simplified model)
            daily_rainfall = self._distribute_rainfall_across_days(total_rainfall, phase_duration)
            daily_rainfall_by_phase[phase_name] = daily_rainfall
        
        # Get planting date
        planting_date = recent_year_data.get('planting_date', f"{year-1}-11-15")
        
        # Calculate enhanced drought impact
        enhanced_analysis = self.enhanced_drought_calculator.calculate_enhanced_drought_impact(
            crop_phases=crop_phases,
            crop=crop,
            lat=lat,
            lon=lon,
            planting_date=planting_date,
            daily_rainfall_by_phase=daily_rainfall_by_phase
        )
        
        return enhanced_analysis
    
    def _distribute_rainfall_across_days(self, total_rainfall: float, days: int) -> List[float]:
        """
        Distribute total rainfall across days using realistic pattern
        
        Args:
            total_rainfall: Total rainfall for period (mm)
            days: Number of days in period
            
        Returns:
            List of daily rainfall values
        """
        if days <= 0 or total_rainfall < 0:
            return []
        
        # Create realistic rainfall distribution
        daily_rainfall = [0.0] * days
        
        if total_rainfall > 0:
            # Simulate realistic rainfall patterns
            # 70% of rainfall comes from 30% of days (Pareto-like distribution)
            rain_days = max(1, int(days * 0.3))
            high_rainfall_days = np.random.choice(days, size=rain_days, replace=False)
            
            remaining_rainfall = total_rainfall
            
            # Distribute 70% of total rainfall to high rainfall days
            heavy_rainfall = total_rainfall * 0.7
            for day_idx in high_rainfall_days[:-1]:
                if remaining_rainfall > 0:
                    day_rainfall = min(remaining_rainfall, heavy_rainfall / len(high_rainfall_days))
                    daily_rainfall[day_idx] = day_rainfall
                    remaining_rainfall -= day_rainfall
            
            # Put remaining rainfall in last heavy rain day
            if remaining_rainfall > 0 and len(high_rainfall_days) > 0:
                daily_rainfall[high_rainfall_days[-1]] = remaining_rainfall
        
        return daily_rainfall

# Factory function for easy integration
def create_enhanced_quote_engine(existing_quote_engine):
    """
    Factory function to create enhanced quote engine with backward compatibility
    
    Args:
        existing_quote_engine: Existing QuoteEngine instance
        
    Returns:
        Enhanced quote engine integration
    """
    return EnhancedQuoteEngineIntegration(existing_quote_engine)

# Export main classes
__all__ = [
    'EnhancedDroughtCalculator',
    'EnhancedQuoteEngineIntegration', 
    'create_enhanced_quote_engine'
]
