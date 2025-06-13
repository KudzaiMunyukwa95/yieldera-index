"""
Global Rainfall-Based Index Insurance Quote Engine V4.0
Pure Geospatial Intelligence - Burning Cost Methodology
Global Coverage - No Geographic Limits
Industry Standard 10-Day Rolling Drought Detection
"""

import ee
import json
import math
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import numpy as np

# Import from existing crops.py (using your structure)
from core.crops import (
    CROP_CONFIG, 
    validate_crop, 
    get_crop_config, 
    get_crop_phases,
    get_crop_phase_weights
)

class GlobalRainfallAnalyzer:
    """Pure rainfall-based drought detection - Global coverage"""
    
    def __init__(self):
        # GLOBAL: Standard parameters for worldwide application
        self.rolling_window_days = 10           # Industry standard window size
        self.drought_trigger_threshold = 15.0   # mm per 10-day window
        self.dry_day_threshold = 1.0           # mm - defines a dry day (<1mm)
        self.consecutive_drought_trigger = 10   # consecutive dry days = drought stress
        
        print("🌍 Global Rainfall Analyzer initialized")
        print("📊 Parameters: 10-day rolling (≤15mm), consecutive dry (≥10 days)")
        print("🗺️ Coverage: Global - no geographic restrictions")

    def _analyze_rolling_10day_windows(self, daily_rainfall: List[float], 
                                     trigger_mm: float = 15.0) -> Dict[str, Any]:
        """
        Analyze 10-day rolling windows for drought detection - Global Standard
        """
        if len(daily_rainfall) < self.rolling_window_days:
            return {
                "drought_windows": 0,
                "total_windows": 0,
                "drought_frequency": 0.0,
                "max_deficit": 0.0,
                "consecutive_drought_windows": 0,
                "rolling_stress_factor": 0.0
            }
        
        rolling_totals = []
        drought_windows = 0
        max_deficit = 0.0
        consecutive_count = 0
        max_consecutive = 0
        
        # Calculate 10-day rolling windows
        for i in range(len(daily_rainfall) - self.rolling_window_days + 1):
            window_total = sum(daily_rainfall[i:i + self.rolling_window_days])
            rolling_totals.append(window_total)
            
            # Check if this window triggers drought
            if window_total <= trigger_mm:
                drought_windows += 1
                deficit = trigger_mm - window_total
                max_deficit = max(max_deficit, deficit)
                consecutive_count += 1
                max_consecutive = max(max_consecutive, consecutive_count)
            else:
                consecutive_count = 0
        
        total_windows = len(rolling_totals)
        drought_frequency = (drought_windows / total_windows * 100) if total_windows > 0 else 0
        
        # Calculate rolling stress factor
        base_stress = drought_frequency / 100.0
        severity_multiplier = min(max_deficit / trigger_mm, 2.0)
        rolling_stress_factor = min(base_stress * (1 + severity_multiplier), 1.0)
        
        return {
            "drought_windows": drought_windows,
            "total_windows": total_windows,
            "drought_frequency": round(drought_frequency, 1),
            "max_deficit": round(max_deficit, 1),
            "consecutive_drought_windows": max_consecutive,
            "rolling_stress_factor": round(rolling_stress_factor, 3),
            "window_totals": [round(x, 1) for x in rolling_totals[:10]]
        }

    def _find_max_consecutive_dry_days(self, daily_rainfall: List[float], 
                                     threshold_mm: float = 1.0) -> Dict[str, Any]:
        """
        Find maximum consecutive dry days sequence - Global Standard
        """
        if not daily_rainfall:
            return {
                "max_consecutive_dry_days": 0,
                "drought_stress_triggered": False,
                "dry_spells": [],
                "consecutive_stress_factor": 0.0
            }
        
        consecutive_count = 0
        max_consecutive = 0
        dry_spells = []
        current_spell_start = None
        
        for i, rainfall in enumerate(daily_rainfall):
            if rainfall < threshold_mm:  # Dry day
                if consecutive_count == 0:
                    current_spell_start = i
                consecutive_count += 1
                max_consecutive = max(max_consecutive, consecutive_count)
            else:  # Wet day
                if consecutive_count > 0:
                    dry_spells.append({
                        "start_day": current_spell_start,
                        "end_day": i - 1,
                        "length": consecutive_count
                    })
                consecutive_count = 0
        
        # Handle case where sequence ends with dry spell
        if consecutive_count > 0:
            dry_spells.append({
                "start_day": current_spell_start,
                "end_day": len(daily_rainfall) - 1,
                "length": consecutive_count
            })
        
        # Calculate stress factor
        drought_stress_triggered = max_consecutive >= self.consecutive_drought_trigger
        if drought_stress_triggered:
            excess_days = max_consecutive - self.consecutive_drought_trigger
            consecutive_stress_factor = min(0.3 + (excess_days * 0.05), 1.0)
        else:
            consecutive_stress_factor = 0.0
        
        return {
            "max_consecutive_dry_days": max_consecutive,
            "drought_stress_triggered": drought_stress_triggered,
            "dry_spells": dry_spells[:5],
            "consecutive_stress_factor": round(consecutive_stress_factor, 3),
            "trigger_threshold": self.consecutive_drought_trigger
        }

    def calculate_rainfall_drought_impact(self, crop_phases: List[Tuple], 
                                        daily_rainfall_by_phase: Dict[str, List[float]],
                                        crop: str) -> Dict[str, Any]:
        """
        Calculate drought impact using pure rainfall analysis - Global methodology
        """
        phase_weights = get_crop_phase_weights(crop)  # Still use crop weights, but no sensitivity
        
        total_drought_impact = 0.0
        phase_analyses = []
        
        for i, (start_day, end_day, trigger_mm, exit_mm, phase_name, water_need_mm, obs_window) in enumerate(crop_phases):
            phase_rainfall = daily_rainfall_by_phase.get(phase_name, [])
            
            if not phase_rainfall:
                continue
            
            # SIMPLIFIED: Use standard threshold for all phases
            adjusted_threshold = self.drought_trigger_threshold
            
            # Standard 10-day rolling window analysis
            rolling_analysis = self._analyze_rolling_10day_windows(phase_rainfall, adjusted_threshold)
            
            # Consecutive dry day analysis
            consecutive_analysis = self._find_max_consecutive_dry_days(phase_rainfall, self.dry_day_threshold)
            
            # Calculate cumulative water deficit
            total_rainfall = sum(phase_rainfall)
            water_deficit = max(0, water_need_mm - total_rainfall)
            cumulative_stress = min(water_deficit / water_need_mm, 1.0) if water_need_mm > 0 else 0
            
            # Take maximum of all stress factors (standard methodology)
            rolling_stress = rolling_analysis["rolling_stress_factor"]
            consecutive_stress = consecutive_analysis["consecutive_stress_factor"]
            
            max_stress = max(cumulative_stress, rolling_stress, consecutive_stress)
            
            # Apply phase weight (no sensitivity adjustments)
            phase_weight = phase_weights[i] if i < len(phase_weights) else 0.25
            weighted_impact = max_stress * phase_weight * 100
            total_drought_impact += weighted_impact
            
            # Store detailed phase analysis
            phase_analysis = {
                "phase_name": phase_name,
                "phase_weight": round(phase_weight, 3),
                "total_rainfall_mm": round(total_rainfall, 1),
                "water_need_mm": water_need_mm,
                "water_deficit_mm": round(water_deficit, 1),
                "cumulative_stress": round(cumulative_stress, 3),
                "rolling_window_analysis": rolling_analysis,
                "consecutive_dry_analysis": consecutive_analysis,
                "maximum_stress_factor": round(max_stress, 3),
                "weighted_impact_percent": round(weighted_impact, 2),
                "methodology": "max(cumulative, rolling_10day, consecutive_dry) - Global Standard"
            }
            phase_analyses.append(phase_analysis)
        
        # Cap total impact
        final_drought_impact = min(total_drought_impact, 100.0)
        
        return {
            "total_drought_impact_percent": round(final_drought_impact, 2),
            "methodology": "Global Rainfall-Based 10-Day Rolling + Consecutive Dry + Cumulative",
            "phase_analyses": phase_analyses,
            "summary_statistics": {
                "total_phases_analyzed": len(phase_analyses),
                "phases_with_drought_stress": len([p for p in phase_analyses if p["maximum_stress_factor"] > 0.3]),
                "most_stressed_phase": max(phase_analyses, key=lambda x: x["maximum_stress_factor"])["phase_name"] if phase_analyses else None,
                "average_rolling_drought_frequency": round(sum(p["rolling_window_analysis"]["drought_frequency"] for p in phase_analyses) / len(phase_analyses), 1) if phase_analyses else 0
            },
            "global_coverage": "Worldwide applicability - pure rainfall intelligence"
        }


class GlobalQuoteEngine:
    """Global rainfall-based index insurance quote engine with burning cost methodology"""
    
    def __init__(self):
        """Initialize global quote engine with industry standard parameters"""
        # GLOBAL DATA REQUIREMENTS
        self.ACTUARIAL_MINIMUM_YEARS = 20      # Industry standard
        self.REGULATORY_MINIMUM_YEARS = 15     # Absolute minimum
        self.OPTIMAL_YEARS_RANGE = 25          # Optimal for capturing climate cycles
        self.EARLIEST_RELIABLE_DATA = 1981     # CHIRPS reliable data starts from 1981
        
        # GLOBAL: Rainfall analyzer with standard parameters
        self.rainfall_analyzer = GlobalRainfallAnalyzer()
        
        # BURNING COST: Industry standard methodology
        self.base_loading_factor = 1.0         # Standard burning cost factor
        self.minimum_premium_rate = 0.015      # 1.5% minimum
        self.maximum_premium_rate = 0.30       # 30% maximum
        
        # Dynamic deductible defaults
        self.default_deductible_rate = 0.05    # 5% default
        
        # Default loadings - industry standard
        self.default_loadings = {
            "admin": 0.10,       # 10% administrative costs
            "margin": 0.05,      # 5% profit margin
            "reinsurance": 0.08  # 8% reinsurance costs
        }
        
        # GLOBAL: Planting detection parameters (adaptable)
        self.rainfall_threshold_7day = 20.0  # mm over 7 consecutive days
        self.daily_threshold = 5.0           # mm for individual days
        self.min_rainy_days = 2             # minimum days above daily threshold
        
        # GLOBAL: No seasonal restrictions - adaptable to any region
        self.planting_window_months = 12     # Full year availability
        
        # PERFORMANCE: Lazy-load Earth Engine objects
        self._chirps_collection = None
        
        print("🚀 GLOBAL Index Insurance Quote Engine V4.0 initialized")
        print("🌍 GLOBAL COVERAGE: Worldwide - no geographic restrictions")
        print("🔧 BURNING COST METHODOLOGY: Industry standard expected loss pricing")
        print("📊 PURE RAINFALL INTELLIGENCE: No zone multipliers or crop sensitivity")
        print("🌱 ADAPTABLE PLANTING DETECTION: Global crop calendar support")
        print("⚡ STREAMLINED APPROACH: Simplified for global scalability")
        print(f"📈 DATA STANDARD: {self.ACTUARIAL_MINIMUM_YEARS} years minimum")
        print(f"📅 Data period: {self.EARLIEST_RELIABLE_DATA} onwards")
        print("🎯 GLOBAL DROUGHT DETECTION:")
        print("   • 10-day rolling windows (≤15mm threshold)")
        print("   • Consecutive dry spell detection (≥10 days <1mm)")
        print("   • Pure rainfall methodology")
        print("   • No geographic multipliers")
        print("   • No crop sensitivity adjustments")
    
    def _get_chirps_collection(self):
        """Lazy-load CHIRPS collection after Earth Engine is initialized"""
        if self._chirps_collection is None:
            try:
                self._chirps_collection = ee.ImageCollection('UCSB-CHG/CHIRPS/DAILY')
                print("📡 CHIRPS collection initialized successfully")
            except Exception as e:
                print(f"❌ Failed to initialize CHIRPS collection: {e}")
                raise
        return self._chirps_collection
    
    def _calculate_burning_cost_premium_rate(self, avg_drought_impact: float) -> float:
        """Calculate premium rate using burning cost methodology"""
        
        # BURNING COST: Expected loss = Premium rate (before loadings)
        # Convert drought impact percentage to loss ratio
        expected_loss_ratio = avg_drought_impact / 100.0
        
        # Apply base loading factor (industry standard 1.0 for burning cost)
        burning_cost_rate = expected_loss_ratio * self.base_loading_factor
        
        # Ensure rate is within bounds
        final_rate = max(self.minimum_premium_rate, min(burning_cost_rate, self.maximum_premium_rate))
        
        # Debug logging
        print(f"📊 BURNING COST CALCULATION:")
        print(f"   Average drought impact: {avg_drought_impact:.1f}%")
        print(f"   Expected loss ratio: {expected_loss_ratio:.3f}")
        print(f"   Base loading factor: {self.base_loading_factor}")
        print(f"   Burning cost rate: {burning_cost_rate:.3f} ({burning_cost_rate*100:.1f}%)")
        print(f"   Final rate: {final_rate:.3f} ({final_rate*100:.1f}%)")
        
        return final_rate
    
    def execute_quote(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute global quote with burning cost methodology
        """
        try:
            print(f"\n🚀 Starting GLOBAL BURNING COST quote execution")
            start_time = datetime.now()
            
            # Validate and extract parameters
            params = self._validate_and_extract_params(request_data)
            
            # Determine quote type
            quote_type = self._determine_quote_type(params['year'])
            params['quote_type'] = quote_type
            
            print(f"📋 Quote type: {quote_type}")
            print(f"🌾 Crop: {params['crop']}")
            print(f"🌍 Location: {params['latitude']:.4f}, {params['longitude']:.4f}")
            print(f"🗓️ Target year: {params['year']}")
            print(f"💰 Deductible: {params['deductible_rate']*100:.1f}%")
            print(f"🎯 GLOBAL rainfall analysis with burning cost pricing")
            
            # Validate data availability
            data_validation = self._validate_data_availability(params['year'], quote_type)
            
            if not data_validation['meets_regulatory_minimum']:
                raise ValueError(
                    f"INSUFFICIENT DATA: Only {data_validation['years_available']} years available. "
                    f"Minimum {self.REGULATORY_MINIMUM_YEARS} years required."
                )
            
            # Generate historical years for analysis
            historical_years = self._get_historical_years(params['year'], quote_type)
            print(f"📊 HISTORICAL ANALYSIS: {len(historical_years)} years ({min(historical_years)}-{max(historical_years)})")
            
            # Detect planting dates globally
            planting_dates = self._detect_global_planting_dates(
                params['latitude'], 
                params['longitude'], 
                historical_years,
                params['crop']
            )
            
            # Filter valid planting dates
            valid_planting_dates = self._filter_valid_planting_dates(planting_dates)
            
            if len(valid_planting_dates) < 10:
                raise ValueError(
                    f"INSUFFICIENT VALID SEASONS: Only {len(valid_planting_dates)} valid seasons detected. "
                    f"Minimum 10 seasons required for statistical reliability."
                )
            
            # Perform global rainfall analysis
            year_by_year_analysis = self._perform_global_analysis(
                params, valid_planting_dates
            )
            
            # Calculate quote using burning cost methodology
            quote_result = self._calculate_burning_cost_quote(
                params, year_by_year_analysis, valid_planting_dates
            )
            
            # Add metadata
            quote_result['data_validation'] = data_validation
            quote_result['global_analysis'] = {
                'methodology': 'Global Rainfall-Based Burning Cost',
                'coverage': 'Worldwide - no geographic restrictions',
                'drought_detection': 'Pure rainfall intelligence',
                'pricing_method': 'Industry standard burning cost',
                'base_loading_factor': self.base_loading_factor
            }
            
            execution_time = (datetime.now() - start_time).total_seconds()
            quote_result['execution_time_seconds'] = round(execution_time, 2)
            quote_result['version'] = "4.0.0-Global-BurningCost"
            
            print(f"✅ GLOBAL BURNING COST quote completed in {execution_time:.2f} seconds")
            print(f"💰 Premium rate: {quote_result['premium_rate']*100:.2f}%")
            print(f"💵 Gross premium: ${quote_result['gross_premium']:,.2f}")
            print(f"🌍 Global methodology applied successfully")
            
            return quote_result
            
        except Exception as e:
            print(f"❌ Global quote execution error: {e}")
            raise
    
    def _perform_global_analysis(self, params: Dict[str, Any], 
                                planting_dates: Dict[int, str]) -> List[Dict[str, Any]]:
        """Perform global rainfall analysis with burning cost methodology"""
        year_results = []
        
        print(f"\n📊 Starting GLOBAL RAINFALL analysis for {len(planting_dates)} seasons")
        print(f"⚡ Method: Pure rainfall intelligence with burning cost pricing")
        
        # Batch process all years for daily rainfall data
        batch_daily_rainfall_data = self._calculate_global_daily_rainfall(
            params['latitude'],
            params['longitude'],
            planting_dates,
            params['crop']
        )
        
        # Calculate overall drought impacts for burning cost rate
        all_drought_impacts = []
        for year, planting_date in planting_dates.items():
            year_daily_rainfall_data = batch_daily_rainfall_data.get(year, {})
            if year_daily_rainfall_data and any(year_daily_rainfall_data.values()):
                crop_phases = get_crop_phases(params['crop'])
                try:
                    drought_analysis = self.rainfall_analyzer.calculate_rainfall_drought_impact(
                        crop_phases, year_daily_rainfall_data, params['crop']
                    )
                    all_drought_impacts.append(drought_analysis['total_drought_impact_percent'])
                except Exception as e:
                    print(f"⚠️ Warning: Drought calculation failed for {year}: {e}")
                    continue
        
        if not all_drought_impacts:
            print("⚠️ WARNING: No valid drought impacts calculated - using fallback")
            burning_cost_rate = self.minimum_premium_rate * 2
            avg_drought_impact = 0.0
        else:
            avg_drought_impact = sum(all_drought_impacts) / len(all_drought_impacts)
            burning_cost_rate = self._calculate_burning_cost_premium_rate(avg_drought_impact)
        
        print(f"📊 BURNING COST CALCULATION:")
        print(f"   Average drought impact: {avg_drought_impact:.2f}%")
        print(f"   Burning cost rate: {burning_cost_rate*100:.2f}%")
        
        # Apply to all years
        for year, planting_date in planting_dates.items():
            try:
                year_daily_rainfall_data = batch_daily_rainfall_data.get(year, {})
                year_analysis = self._analyze_individual_year_global(
                    params, year, planting_date, year_daily_rainfall_data, burning_cost_rate
                )
                year_results.append(year_analysis)
                
            except Exception as e:
                print(f"❌ Error in global analysis for {year}: {e}")
                year_results.append({
                    'year': year,
                    'error': str(e),
                    'burning_cost_rate': burning_cost_rate,
                    'simulated_premium_usd': 0.0,
                    'simulated_payout': 0.0,
                    'loss_ratio': 0.0
                })
        
        return year_results
    
    def _analyze_individual_year_global(self, params: Dict[str, Any], year: int, 
                                      planting_date: str, 
                                      daily_rainfall_by_phase: Dict[str, List[float]],
                                      burning_cost_rate: float) -> Dict[str, Any]:
        """Analyze individual year using global methodology"""
        
        if not daily_rainfall_by_phase or not any(daily_rainfall_by_phase.values()):
            # Fallback analysis
            sum_insured = params['expected_yield'] * params['price_per_ton'] * params.get('area_ha', 1.0)
            simulated_premium = sum_insured * burning_cost_rate
            
            return {
                'year': year,
                'planting_date': planting_date,
                'global_drought_analysis': {'total_drought_impact_percent': 0.0},
                'burning_cost_rate': burning_cost_rate,
                'simulated_premium_usd': simulated_premium,
                'simulated_payout': 0.0,
                'loss_ratio': 0.0,
                'methodology': 'Global Rainfall Analysis - Fallback'
            }
        
        crop_phases = get_crop_phases(params['crop'])
        
        # Calculate drought impact using global methodology
        try:
            global_drought_analysis = self.rainfall_analyzer.calculate_rainfall_drought_impact(
                crop_phases, daily_rainfall_by_phase, params['crop']
            )
            drought_impact = global_drought_analysis['total_drought_impact_percent']
        except Exception as e:
            print(f"❌ Global drought calculation failed for {year}: {e}")
            drought_impact = 0.0
            global_drought_analysis = {'total_drought_impact_percent': 0.0, 'error': str(e)}
        
        # Financial calculations
        sum_insured = params['expected_yield'] * params['price_per_ton'] * params.get('area_ha', 1.0)
        simulated_premium = sum_insured * burning_cost_rate
        
        # Apply deductible
        drought_impact_after_deductible = max(0, drought_impact - (params.get('deductible_rate', 0.05) * 100))
        simulated_payout = sum_insured * (drought_impact_after_deductible / 100.0)
        
        # Calculate metrics
        net_result = simulated_payout - simulated_premium
        loss_ratio = (simulated_payout / simulated_premium) if simulated_premium > 0 else 0
        
        return {
            'year': year,
            'planting_date': planting_date,
            'global_drought_analysis': global_drought_analysis,
            'drought_impact': round(drought_impact, 2),
            'drought_impact_after_deductible': round(drought_impact_after_deductible, 2),
            'burning_cost_rate': round(burning_cost_rate, 4),
            'simulated_premium_usd': round(simulated_premium, 2),
            'simulated_payout': round(simulated_payout, 2),
            'net_result': round(net_result, 2),
            'loss_ratio': round(loss_ratio, 4),
            'methodology': 'Global Rainfall Analysis - Burning Cost'
        }
    
    def _calculate_global_daily_rainfall(self, latitude: float, longitude: float,
                                       planting_dates: Dict[int, str], 
                                       crop: str) -> Dict[int, Dict[str, List[float]]]:
        """Calculate daily rainfall for global analysis"""
        try:
            point = ee.Geometry.Point([longitude, latitude])
            crop_phases = get_crop_phases(crop)
            
            print(f"🔄 Global rainfall processing for {len(planting_dates)} years, {len(crop_phases)} phases")
            
            # Build date ranges
            all_phase_ranges = {}
            for year, planting_date in planting_dates.items():
                plant_date = datetime.strptime(planting_date, '%Y-%m-%d')
                year_phases = {}
                
                for start_day, end_day, trigger_mm, exit_mm, phase_name, water_need_mm, obs_window in crop_phases:
                    phase_start = plant_date + timedelta(days=start_day)
                    phase_end = plant_date + timedelta(days=end_day)
                    
                    year_phases[phase_name] = {
                        'start': phase_start.strftime('%Y-%m-%d'),
                        'end': phase_end.strftime('%Y-%m-%d'),
                        'duration_days': end_day - start_day + 1
                    }
                
                all_phase_ranges[year] = year_phases
            
            # Execute rainfall calculation
            batch_result = self._execute_global_rainfall_calculation(point, all_phase_ranges)
            
            print(f"✅ Global rainfall calculation completed")
            return batch_result
            
        except Exception as e:
            print(f"❌ Error in global rainfall calculation: {e}")
            return {year: {} for year in planting_dates.keys()}
    
    def _execute_global_rainfall_calculation(self, point: ee.Geometry.Point, 
                                           all_phase_ranges: Dict) -> Dict[int, Dict[str, List[float]]]:
        """Execute global rainfall calculation with chunking"""
        
        # Find overall date range
        all_dates = []
        for year_data in all_phase_ranges.values():
            for phase_data in year_data.values():
                all_dates.extend([phase_data['start'], phase_data['end']])
        
        overall_start = min(all_dates)
        overall_end = max(all_dates)
        
        print(f"📅 Global analysis period: {overall_start} to {overall_end}")
        
        # Process in yearly chunks
        start_year = datetime.strptime(overall_start, '%Y-%m-%d').year
        end_year = datetime.strptime(overall_end, '%Y-%m-%d').year
        
        daily_rainfall_lookup = {}
        
        for chunk_year in range(start_year, end_year + 1):
            try:
                chunk_start = max(overall_start, f"{chunk_year}-01-01")
                chunk_end = min(overall_end, f"{chunk_year}-12-31")
                
                # Query CHIRPS
                chirps_chunk = self._get_chirps_collection() \
                    .filterDate(chunk_start, chunk_end) \
                    .filterBounds(point)
                
                def extract_daily_rainfall(image):
                    rainfall = image.reduceRegion(
                        reducer=ee.Reducer.mean(),
                        geometry=point,
                        scale=5566,
                        maxPixels=1
                    ).get('precipitation')
                    
                    return ee.Feature(None, {
                        'date': image.date().format('YYYY-MM-dd'),
                        'rainfall': rainfall
                    })
                
                daily_features = chirps_chunk.map(extract_daily_rainfall)
                chunk_data = daily_features.getInfo()
                
                if 'features' in chunk_data:
                    for feature in chunk_data['features']:
                        props = feature['properties']
                        if props.get('rainfall') is not None:
                            daily_rainfall_lookup[props['date']] = float(props['rainfall'])
                
            except Exception as e:
                print(f"❌ Error processing chunk {chunk_year}: {e}")
                continue
        
        # Extract phase data
        global_results = {}
        
        for year, year_phases in all_phase_ranges.items():
            year_daily_data = {}
            
            for phase_name, phase_info in year_phases.items():
                phase_start_date = datetime.strptime(phase_info['start'], '%Y-%m-%d')
                phase_duration = phase_info['duration_days']
                
                phase_daily_rainfall = []
                for i in range(phase_duration):
                    current_date = (phase_start_date + timedelta(days=i)).strftime('%Y-%m-%d')
                    daily_rainfall = daily_rainfall_lookup.get(current_date, 0.0)
                    phase_daily_rainfall.append(daily_rainfall)
                
                year_daily_data[phase_name] = phase_daily_rainfall
            
            global_results[year] = year_daily_data
        
        return global_results
    
    def _calculate_burning_cost_quote(self, params: Dict[str, Any], 
                                    year_analysis: List[Dict[str, Any]],
                                    planting_dates: Dict[int, str]) -> Dict[str, Any]:
        """Calculate final quote using burning cost methodology"""
        valid_years = [y for y in year_analysis if 'error' not in y]
        
        if not valid_years:
            raise ValueError("No valid years for burning cost calculation")
        
        # Use burning cost rate from analysis
        burning_cost_rate = valid_years[0]['burning_cost_rate']
        
        # Calculate metrics
        avg_payout = sum(y['simulated_payout'] for y in valid_years) / len(valid_years)
        
        # Financial calculations
        sum_insured = params['expected_yield'] * params['price_per_ton'] * params.get('area_ha', 1.0)
        burning_cost = sum_insured * burning_cost_rate
        
        # Apply loadings
        loadings_to_use = params['custom_loadings'] if params['custom_loadings'] else self.default_loadings
        
        loadings_breakdown = {}
        total_loadings_amount = 0.0
        
        for loading_type, loading_rate in loadings_to_use.items():
            loading_amount = burning_cost * loading_rate
            loadings_breakdown[loading_type] = {
                'rate': loading_rate,
                'amount': loading_amount
            }
            total_loadings_amount += loading_amount
        
        gross_premium = burning_cost + total_loadings_amount
        deductible_amount = sum_insured * params['deductible_rate']
        
        return {
            # Core quote data
            'crop': params['crop'],
            'year': params['year'],
            'quote_type': params['quote_type'],
            'latitude': params['latitude'],
            'longitude': params['longitude'],
            'area_ha': params.get('area_ha', 1.0),
            
            # Financial summary
            'expected_yield': params['expected_yield'],
            'price_per_ton': params['price_per_ton'],
            'sum_insured': sum_insured,
            'premium_rate': burning_cost_rate,
            'burning_cost': burning_cost,
            'loadings_breakdown': loadings_breakdown,
            'total_loadings': total_loadings_amount,
            'gross_premium': gross_premium,
            'deductible_rate': params['deductible_rate'],
            'deductible_amount': deductible_amount,
            
            # Analysis results
            'historical_years_used': list(planting_dates.keys()),
            'year_by_year_simulation': year_analysis,
            
            # Metadata
            'generated_at': datetime.utcnow().isoformat(),
            'methodology': 'global_burning_cost_v4.0',
            'drought_detection_methodology': 'Global Rainfall-Based 10-Day Rolling + Consecutive Dry'
        }
    
    def _detect_global_planting_dates(self, latitude: float, longitude: float, 
                                    years: List[int], crop: str) -> Dict[int, Optional[str]]:
        """Detect planting dates globally - adaptable to any region"""
        point = ee.Geometry.Point([longitude, latitude])
        results = {}
        
        print(f"🌱 Global planting detection for {len(years)} years")
        print(f"🌍 Location: {latitude:.4f}, {longitude:.4f}")
        
        # Determine optimal planting season based on location
        planting_window = self._determine_optimal_planting_window(latitude, crop)
        
        for year in years:
            try:
                # Calculate season boundaries based on location
                season_start, season_end = self._calculate_season_boundaries(year, latitude, planting_window)
                
                planting_date = self._detect_season_planting_global(point, season_start, season_end)
                results[year] = planting_date
                
            except Exception as e:
                print(f"❌ Error detecting planting for {year}: {e}")
                results[year] = None
        
        return results
    
    def _determine_optimal_planting_window(self, latitude: float, crop: str) -> Dict[str, int]:
        """Determine optimal planting window based on latitude and crop"""
        # Northern Hemisphere
        if latitude > 0:
            if latitude > 40:  # Temperate
                return {'start_month': 4, 'end_month': 6}  # Apr-Jun
            else:  # Subtropical
                return {'start_month': 3, 'end_month': 7}  # Mar-Jul
        
        # Southern Hemisphere
        else:
            if latitude < -30:  # Temperate
                return {'start_month': 10, 'end_month': 12}  # Oct-Dec
            else:  # Subtropical/Tropical
                return {'start_month': 10, 'end_month': 1}   # Oct-Jan (crossing year)
    
    def _calculate_season_boundaries(self, year: int, latitude: float, 
                                   planting_window: Dict[str, int]) -> Tuple[datetime, datetime]:
        """Calculate season boundaries for global application"""
        start_month = planting_window['start_month']
        end_month = planting_window['end_month']
        
        if end_month < start_month:  # Crosses year boundary
            season_start = datetime(year - 1, start_month, 1)
            season_end = datetime(year, end_month, 28)  # Conservative end
        else:
            season_start = datetime(year, start_month, 1)
            season_end = datetime(year, end_month, 28)
        
        return season_start, season_end
    
    def _detect_season_planting_global(self, point: ee.Geometry.Point, 
                                     season_start: datetime, season_end: datetime) -> Optional[str]:
        """Detect planting date globally"""
        try:
            start_date = season_start.strftime('%Y-%m-%d')
            end_date = season_end.strftime('%Y-%m-%d')
            
            season_chirps = self._get_chirps_collection() \
                .filterDate(start_date, end_date) \
                .filterBounds(point)
            
            def extract_daily_rainfall(image):
                rainfall = image.reduceRegion(
                    reducer=ee.Reducer.mean(),
                    geometry=point,
                    scale=5566,
                    maxPixels=1
                ).get('precipitation')
                
                return ee.Feature(None, {
                    'date': image.date().format('YYYY-MM-dd'),
                    'rainfall': rainfall
                })
            
            daily_features = season_chirps.map(extract_daily_rainfall)
            rainfall_data = daily_features.getInfo()
            
            return self._find_global_planting_date(rainfall_data)
            
        except Exception as e:
            print(f"❌ Error in global planting detection: {e}")
            return None
    
    def _find_global_planting_date(self, rainfall_data: Dict) -> Optional[str]:
        """Find planting date using global criteria"""
        try:
            daily_data = []
            
            if 'features' not in rainfall_data:
                return None
                
            for feature in rainfall_data['features']:
                props = feature['properties']
                if props.get('rainfall') is not None:
                    daily_data.append({
                        'date': props['date'],
                        'rainfall': float(props['rainfall'])
                    })
            
            if len(daily_data) < 7:
                return None
            
            daily_data.sort(key=lambda x: x['date'])
            
            # Apply global planting criteria
            for i in range(len(daily_data) - 6):
                window = daily_data[i:i+7]
                total_rainfall = sum(day['rainfall'] for day in window)
                qualifying_days = sum(1 for day in window if day['rainfall'] >= self.daily_threshold)
                
                if (total_rainfall >= self.rainfall_threshold_7day and 
                    qualifying_days >= self.min_rainy_days):
                    return window[-1]['date']
            
            return None
            
        except Exception as e:
            print(f"❌ Error processing global rainfall data: {e}")
            return None
    
    def _filter_valid_planting_dates(self, planting_dates: Dict[int, Optional[str]]) -> Dict[int, str]:
        """Filter valid planting dates - global version"""
        valid_dates = {}
        
        for year, date_str in planting_dates.items():
            if date_str is not None:
                try:
                    datetime.strptime(date_str, '%Y-%m-%d')  # Validate format
                    valid_dates[year] = date_str
                except Exception:
                    continue
                    
        return valid_dates
    
    # Utility methods
    def _validate_and_extract_params(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate parameters for global coverage"""
        required_fields = ['expected_yield', 'price_per_ton']
        for field in required_fields:
            if field not in request_data:
                raise ValueError(f"Missing required field: {field}")
        
        # Global location validation - no restrictions
        if 'geometry' in request_data:
            geometry = request_data['geometry']
            if geometry['type'] == 'Point':
                longitude, latitude = geometry['coordinates']
            else:
                raise ValueError("Only Point geometry is supported")
        elif 'latitude' in request_data and 'longitude' in request_data:
            latitude = float(request_data['latitude'])
            longitude = float(request_data['longitude'])
        else:
            raise ValueError("Must provide either 'geometry' or 'latitude'/'longitude'")
        
        # Global coordinate validation
        if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
            raise ValueError("Invalid global coordinates")
        
        crop = request_data.get('crop', 'maize').lower().strip()
        validated_crop = validate_crop(crop)
        
        expected_yield = float(request_data['expected_yield'])
        price_per_ton = float(request_data['price_per_ton'])
        year = int(request_data.get('year', datetime.now().year))
        
        if expected_yield <= 0 or expected_yield > 50:  # Increased for global range
            raise ValueError(f"Expected yield must be between 0 and 50 tons/ha")
        
        if price_per_ton <= 0 or price_per_ton > 10000:  # Increased for global range
            raise ValueError(f"Price per ton must be between 0 and $10,000")
        
        current_year = datetime.now().year
        if year < self.EARLIEST_RELIABLE_DATA or year > current_year + 5:
            raise ValueError(f"Year must be between {self.EARLIEST_RELIABLE_DATA} and {current_year + 5}")
        
        deductible_rate = float(request_data.get('deductible_rate', self.default_deductible_rate))
        if deductible_rate < 0 or deductible_rate > 0.5:
            raise ValueError(f"Deductible rate must be between 0% and 50%")
        
        custom_loadings = request_data.get('loadings', {})
        if not isinstance(custom_loadings, dict):
            raise ValueError("Loadings must be provided as a dictionary")
        
        return {
            'latitude': latitude,
            'longitude': longitude,
            'crop': validated_crop,
            'expected_yield': expected_yield,
            'price_per_ton': price_per_ton,
            'year': year,
            'area_ha': request_data.get('area_ha', 1.0),
            'deductible_rate': deductible_rate,
            'custom_loadings': custom_loadings
        }
    
    def _validate_data_availability(self, target_year: int, quote_type: str) -> Dict[str, Any]:
        """Validate data availability for global coverage"""
        current_year = datetime.now().year
        
        if quote_type == "historical":
            max_available_years = target_year - self.EARLIEST_RELIABLE_DATA
        else:
            max_available_years = current_year - self.EARLIEST_RELIABLE_DATA
        
        years_available = min(self.OPTIMAL_YEARS_RANGE, max_available_years)
        
        return {
            'years_available': years_available,
            'meets_actuarial_standard': years_available >= self.ACTUARIAL_MINIMUM_YEARS,
            'meets_regulatory_minimum': years_available >= self.REGULATORY_MINIMUM_YEARS
        }
    
    def _get_historical_years(self, target_year: int, quote_type: str) -> List[int]:
        """Generate historical years for global analysis"""
        current_year = datetime.now().year
        
        if quote_type == "historical":
            latest_year = target_year - 1
        else:
            latest_year = current_year - 1
        
        years_to_use = min(self.OPTIMAL_YEARS_RANGE, latest_year - self.EARLIEST_RELIABLE_DATA + 1)
        start_year = latest_year - years_to_use + 1
        
        return list(range(start_year, latest_year + 1))
    
    def _determine_quote_type(self, year: int) -> str:
        """Determine quote type"""
        current_year = datetime.now().year
        return "prospective" if year >= current_year else "historical"


# For backward compatibility
QuoteEngine = GlobalQuoteEngine

if __name__ == "__main__":
    print("🌍 Global Index Insurance Quote Engine V4.0 - Ready for worldwide deployment")
