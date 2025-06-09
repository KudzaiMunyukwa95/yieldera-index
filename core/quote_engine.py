"""
Enhanced Quote Engine V2 with all requested fixes and proper crops.py integration
Resolves crop phase conflicts and implements dynamic deductibles, custom loadings, 
year alignment, rainfall tracking, and field-level storytelling
"""

import ee
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple

# Import from existing crops.py (using your structure)
from core.crops import (
    CROP_CONFIG, 
    AGROECOLOGICAL_ZONES,
    validate_crop, 
    get_crop_config, 
    get_crop_phases,
    get_crop_phase_weights,
    get_zone_config
)

# Try to import zones, with fallback
try:
    from core.zones import get_zone_adjustments
    USING_EXTERNAL_ZONES = True
    print("🗺️ Using external zones.py for zone adjustments")
except ImportError:
    USING_EXTERNAL_ZONES = False
    print("🗺️ Using crops.py zone data (zones.py not found)")

class QuoteEngine:
    """Enhanced quote engine V2 with comprehensive fixes and proper crops.py integration"""
    
    def __init__(self):
        """Initialize with enhanced components using your crops.py structure"""
        # Enhanced simulation parameters
        self.base_loading_factor = 1.5  # Base loading multiplier
        self.minimum_premium_rate = 0.015  # 1.5% minimum
        self.maximum_premium_rate = 0.25   # 25% maximum
        
        # Dynamic deductible defaults (now configurable)
        self.default_deductible_rate = 0.05  # 5% default, now flexible
        
        # Default loadings (if none provided)
        self.default_loadings = {
            "admin": 0.10,       # 10% administrative costs
            "margin": 0.05,      # 5% profit margin
            "reinsurance": 0.08  # 8% reinsurance costs
        }
        
        # Rainfall-based planting detection parameters
        self.rainfall_threshold_7day = 20.0  # mm over 7 consecutive days
        self.daily_threshold = 5.0  # mm for individual days
        self.min_rainy_days = 2  # minimum days above daily threshold
        
        # Seasonal validation - Summer crops only
        self.valid_planting_months = [10, 11, 12, 1]  # Oct-Jan only
        self.season_start_month = 10  # October
        self.season_start_day = 1
        self.season_end_month = 1  # January
        self.season_end_day = 31
        
        print("🔧 Quote Engine V2 initialized with enhanced features")
        print("📚 Using crops.py with 9 crop types and AEZ zones")
        print("🌱 Planting detection: Rainfall-only (no NDVI)")
        print("📊 Features: Dynamic deductibles, custom loadings, year alignment")
        print("🗓️ Season focus: Summer crops only (Oct-Jan planting)")
    
    def execute_quote(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute quote with enhanced year-by-year simulation and all requested fixes
        
        Args:
            request_data: Quote request parameters
            
        Returns:
            Enhanced quote with detailed simulation data and fixes
        """
        try:
            print(f"\n🚀 Starting enhanced quote execution V2")
            start_time = datetime.now()
            
            # Validate and extract parameters (with deductible and loadings support)
            params = self._validate_and_extract_params(request_data)
            
            # Determine quote type with seasonal validation
            quote_type = self._determine_quote_type_with_validation(params['year'])
            params['quote_type'] = quote_type
            
            print(f"📋 Quote type: {quote_type}")
            print(f"🌾 Crop: {params['crop']}")
            print(f"📍 Location: {params['latitude']:.4f}, {params['longitude']:.4f}")
            print(f"🗓️ Target year: {params['year']}")
            print(f"💰 Deductible: {params['deductible_rate']*100:.1f}%")
            print(f"📊 Custom loadings: {len(params['custom_loadings'])} types")
            
            # Generate historical years for analysis (with proper year alignment)
            historical_years = self._get_historical_years_aligned(params['year'], quote_type)
            print(f"📊 Historical analysis: {len(historical_years)} years ({min(historical_years)}-{max(historical_years)})")
            
            # Detect planting dates using refined rainfall-only logic
            planting_dates = self._detect_planting_dates_rainfall_only(
                params['latitude'], 
                params['longitude'], 
                historical_years
            )
            
            # Filter valid planting dates and validate seasons
            valid_planting_dates = self._validate_seasonal_planting_dates(planting_dates)
            
            if len(valid_planting_dates) < 3:
                print(f"⚠️ Warning: Only {len(valid_planting_dates)} valid planting seasons detected")
            
            # Perform detailed year-by-year drought analysis (with rainfall per phase)
            year_by_year_analysis = self._perform_detailed_analysis_with_rainfall(
                params, valid_planting_dates
            )
            
            # Calculate enhanced quote metrics (with dynamic deductible and loadings)
            quote_result = self._calculate_enhanced_quote_v2(
                params, year_by_year_analysis, valid_planting_dates
            )
            
            # Add enhanced simulation results with all requested features
            quote_result['year_by_year_simulation'] = year_by_year_analysis
            quote_result['planting_analysis'] = {
                'detection_method': 'rainfall_only',
                'criteria': {
                    'cumulative_7day_threshold': '≥20mm',
                    'daily_threshold': '≥5mm',
                    'minimum_qualifying_days': 2,
                    'season_window': 'October 1 - January 31'
                },
                'detection_summary': self._get_planting_windows_summary(planting_dates),
                'valid_seasons': len(valid_planting_dates),
                'total_years_analyzed': len(historical_years)
            }
            
            # Add field-level storytelling
            quote_result['field_story'] = self._generate_field_story(
                year_by_year_analysis, params
            )
            
            execution_time = (datetime.now() - start_time).total_seconds()
            quote_result['execution_time_seconds'] = round(execution_time, 2)
            quote_result['version'] = "2.2.0-Enhanced"
            
            print(f"✅ Quote completed in {execution_time:.2f} seconds")
            print(f"💰 Premium rate: {quote_result['premium_rate']*100:.2f}%")
            print(f"💵 Gross premium: ${quote_result['gross_premium']:,.2f}")
            print(f"📈 Total loadings: ${quote_result['total_loadings']:,.2f}")
            
            return quote_result
            
        except Exception as e:
            print(f"❌ Quote execution error: {e}")
            raise
    
    def _validate_and_extract_params(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and extract parameters with enhanced deductible and loadings support"""
        # Required fields
        required_fields = ['expected_yield', 'price_per_ton']
        for field in required_fields:
            if field not in request_data:
                raise ValueError(f"Missing required field: {field}")
        
        # Location validation
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
        
        # Coordinate validation for Southern Africa focus
        if not (-25 <= latitude <= -15 and 25 <= longitude <= 35):
            print(f"⚠️ Warning: Coordinates outside typical Southern Africa range")
        
        # Extract and validate crop using crops.py
        crop = request_data.get('crop', 'maize').lower().strip()
        validated_crop = validate_crop(crop)  # Use your validation function
        
        expected_yield = float(request_data['expected_yield'])
        price_per_ton = float(request_data['price_per_ton'])
        year = int(request_data.get('year', datetime.now().year))
        
        # Validate ranges
        if expected_yield <= 0 or expected_yield > 20:
            raise ValueError(f"Expected yield must be between 0 and 20 tons/ha")
        
        if price_per_ton <= 0 or price_per_ton > 5000:
            raise ValueError(f"Price per ton must be between 0 and $5000")
        
        # Year validation for seasonal appropriateness
        current_year = datetime.now().year
        if year < 2018 or year > current_year + 2:
            raise ValueError(f"Year must be between 2018 and {current_year + 2}")
        
        # ENHANCED: Dynamic deductible support
        deductible_rate = float(request_data.get('deductible_rate', self.default_deductible_rate))
        if deductible_rate < 0 or deductible_rate > 0.5:
            raise ValueError(f"Deductible rate must be between 0% and 50%")
        
        # ENHANCED: Custom loadings support
        custom_loadings = request_data.get('loadings', {})
        if not isinstance(custom_loadings, dict):
            raise ValueError("Loadings must be provided as a dictionary")
        
        # Validate loading values
        for loading_type, loading_value in custom_loadings.items():
            if not isinstance(loading_value, (int, float)):
                raise ValueError(f"Loading '{loading_type}' must be a number")
            if loading_value < 0 or loading_value > 1.0:
                raise ValueError(f"Loading '{loading_type}' must be between 0% and 100%")
        
        return {
            'latitude': latitude,
            'longitude': longitude,
            'crop': validated_crop,
            'expected_yield': expected_yield,
            'price_per_ton': price_per_ton,
            'year': year,
            'area_ha': request_data.get('area_ha', 1.0),
            'zone': request_data.get('zone', 'auto_detect'),
            'deductible_rate': deductible_rate,  # ENHANCED: Dynamic deductible
            'custom_loadings': custom_loadings,  # ENHANCED: Custom loadings
            'buffer_radius': request_data.get('buffer_radius', 1500)
        }
    
    def _get_historical_years_aligned(self, target_year: int, quote_type: str) -> List[int]:
        """ENHANCED: Generate properly aligned historical years"""
        if quote_type == "historical":
            # For historical quotes, analyze seasons that ended before target year
            # If target year is 2024, analyze 2018-2023 seasons
            return list(range(max(2018, target_year - 8), target_year))
        else:
            # For prospective quotes, use recent complete seasons
            # For 2025 prospective, analyze 2018-2024 seasons
            current_year = datetime.now().year
            return list(range(max(2018, current_year - 8), current_year))
    
    def _detect_planting_dates_rainfall_only(self, latitude: float, longitude: float, 
                                           years: List[int]) -> Dict[int, Optional[str]]:
        """Detect planting dates using refined rainfall-only logic with year alignment"""
        point = ee.Geometry.Point([longitude, latitude])
        results = {}
        
        print(f"🌱 Starting refined planting detection for {len(years)} years")
        print(f"📍 Location: {latitude:.4f}, {longitude:.4f}")
        print(f"🌧️ Criteria: ≥{self.rainfall_threshold_7day}mm over 7 days, {self.min_rainy_days}+ days ≥{self.daily_threshold}mm")
        
        for year in years:
            try:
                # ENHANCED: Proper year alignment for planting seasons
                # For growing season ending in 'year', planting happens in previous year
                planting_season_start = datetime(year - 1, self.season_start_month, self.season_start_day)
                planting_season_end = datetime(year, self.season_end_month, self.season_end_day)
                
                print(f"\n📅 Analyzing {year} season: {planting_season_start.strftime('%Y-%m-%d')} to {planting_season_end.strftime('%Y-%m-%d')}")
                
                planting_date = self._detect_season_planting_rainfall(point, planting_season_start, planting_season_end)
                results[year] = planting_date
                
                if planting_date:
                    print(f"✅ {year}: Planting detected on {planting_date}")
                else:
                    print(f"❌ {year}: No suitable planting conditions detected")
                    
            except Exception as e:
                print(f"❌ Error detecting planting for {year}: {e}")
                results[year] = None
        
        return results
    
    def _detect_season_planting_rainfall(self, point: ee.Geometry.Point, 
                                       season_start: datetime, season_end: datetime) -> Optional[str]:
        """Detect planting date within a specific season using rainfall criteria"""
        try:
            # Get CHIRPS rainfall data for the season
            start_date = season_start.strftime('%Y-%m-%d')
            end_date = season_end.strftime('%Y-%m-%d')
            
            chirps = ee.ImageCollection('UCSB-CHG/CHIRPS/DAILY') \
                .filterDate(start_date, end_date) \
                .filterBounds(point)
            
            # Extract daily rainfall time series
            def extract_rainfall(image):
                value = image.reduceRegion(
                    reducer=ee.Reducer.mean(),
                    geometry=point,
                    scale=5566,  # CHIRPS native resolution
                    maxPixels=1
                ).get('precipitation')
                
                return ee.Feature(None, {
                    'date': image.date().format('YYYY-MM-dd'),
                    'rainfall': value
                })
            
            rainfall_features = chirps.map(extract_rainfall)
            rainfall_data = rainfall_features.getInfo()
            
            # Convert to list for easier analysis
            daily_data = []
            for feature in rainfall_data['features']:
                props = feature['properties']
                if props['rainfall'] is not None:
                    daily_data.append({
                        'date': props['date'],
                        'rainfall': float(props['rainfall'])
                    })
            
            if not daily_data:
                print("⚠️ No rainfall data available for season")
                return None
            
            # Sort by date
            daily_data.sort(key=lambda x: x['date'])
            
            print(f"📊 Rainfall data: {len(daily_data)} days")
            
            # Find planting date using 7-day rolling window
            planting_date = self._find_planting_with_criteria_simple(daily_data)
            
            return planting_date
            
        except Exception as e:
            print(f"❌ Error in season planting detection: {e}")
            return None
    
    def _find_planting_with_criteria_simple(self, daily_data: List[Dict]) -> Optional[str]:
        """Find planting date using refined rainfall criteria"""
        if len(daily_data) < 7:
            return None
        
        # Check each possible 7-day window
        for i in range(len(daily_data) - 6):
            # Get 7-day window
            window = daily_data[i:i+7]
            
            # Calculate total rainfall in window
            total_rainfall = sum(day['rainfall'] for day in window)
            
            # Count qualifying days (≥5mm)
            qualifying_days = sum(1 for day in window if day['rainfall'] >= self.daily_threshold)
            
            # Check if criteria are met
            if (total_rainfall >= self.rainfall_threshold_7day and 
                qualifying_days >= self.min_rainy_days):
                
                # Return the last date of the 7-day window as planting date
                planting_date = window[-1]['date']
                
                print(f"🎯 Planting criteria met: {total_rainfall:.1f}mm over 7 days, {qualifying_days} qualifying days")
                
                return planting_date
        
        return None
    
    def _perform_detailed_analysis_with_rainfall(self, params: Dict[str, Any], 
                                               planting_dates: Dict[int, str]) -> List[Dict[str, Any]]:
        """ENHANCED: Perform detailed analysis with rainfall data per phase"""
        year_results = []
        
        print(f"\n📊 Starting detailed year-by-year analysis for {len(planting_dates)} seasons")
        
        for year, planting_date in planting_dates.items():
            try:
                print(f"\n🔍 Analyzing {year} season (planted: {planting_date})")
                
                # Calculate individual year metrics with rainfall tracking
                year_analysis = self._analyze_individual_year_with_rainfall(params, year, planting_date)
                year_results.append(year_analysis)
                
                print(f"📈 {year} results: {year_analysis['drought_impact']:.1f}% loss, "
                      f"{year_analysis['simulated_premium_rate']*100:.2f}% rate, "
                      f"${year_analysis['simulated_payout']:,.0f} payout")
                
            except Exception as e:
                print(f"❌ Error analyzing {year}: {e}")
                # Add error entry to maintain year tracking
                year_results.append({
                    'year': year,
                    'planting_date': planting_date,
                    'planting_year': int(planting_date.split('-')[0]) if planting_date else year-1,
                    'harvest_year': year,
                    'error': str(e),
                    'drought_impact': 0.0,
                    'simulated_premium_rate': 0.0,
                    'simulated_premium_usd': 0.0,
                    'simulated_payout': 0.0,
                    'rainfall_mm_by_phase': {}
                })
        
        return year_results
    
    def _analyze_individual_year_with_rainfall(self, params: Dict[str, Any], year: int, 
                                             planting_date: str) -> Dict[str, Any]:
        """ENHANCED: Analyze individual year with rainfall tracking per phase"""
        # Get crop phases using your crops.py structure
        crop_phases = get_crop_phases(params['crop'])
        
        # Calculate season end date
        plant_date = datetime.strptime(planting_date, '%Y-%m-%d')
        total_season_days = crop_phases[-1][1]  # end_day of last phase
        season_end = plant_date + timedelta(days=total_season_days)
        
        # ENHANCED: Calculate rainfall per phase
        rainfall_by_phase = self._calculate_rainfall_per_phase(
            params['latitude'],
            params['longitude'],
            planting_date,
            season_end.strftime('%Y-%m-%d'),
            crop_phases
        )
        
        # Calculate drought impact using phase-specific analysis
        drought_impact = self._calculate_drought_impact_by_phases(
            crop_phases, rainfall_by_phase, params['crop']
        )
        
        # Simulate individual year premium rate
        base_risk = drought_impact / 100.0
        zone_multiplier = self._get_zone_risk_multiplier(params)
        individual_premium_rate = base_risk * self.base_loading_factor * zone_multiplier
        individual_premium_rate = max(self.minimum_premium_rate, 
                                    min(individual_premium_rate, self.maximum_premium_rate))
        
        # Calculate simulated amounts
        sum_insured = params['expected_yield'] * params['price_per_ton'] * params.get('area_ha', 1.0)
        simulated_premium = sum_insured * individual_premium_rate
        simulated_payout = sum_insured * (drought_impact / 100.0)
        
        # ENHANCED: Add year alignment info
        planting_year = int(planting_date.split('-')[0])
        harvest_year = year
        
        return {
            'year': year,
            'planting_date': planting_date,
            'planting_year': planting_year,        # ENHANCED: Clear year labels
            'harvest_year': harvest_year,          # ENHANCED: Clear year labels
            'season_end_date': season_end.strftime('%Y-%m-%d'),
            'drought_impact': drought_impact,
            'simulated_premium_rate': individual_premium_rate,
            'simulated_premium_usd': simulated_premium,
            'simulated_payout': simulated_payout,
            'net_result': simulated_payout - simulated_premium,  # Farmer perspective
            'loss_ratio': (simulated_payout / simulated_premium) if simulated_premium > 0 else 0,
            'rainfall_mm_by_phase': rainfall_by_phase,          # ENHANCED: Rainfall per phase
            'critical_periods': len([p for p, r in rainfall_by_phase.items() if r < 30])  # Phases with low rainfall
        }
    
    def _calculate_rainfall_per_phase(self, latitude: float, longitude: float,
                                    planting_date: str, season_end: str, 
                                    crop_phases: List[Tuple]) -> Dict[str, float]:
        """ENHANCED: Calculate actual rainfall per crop phase"""
        try:
            point = ee.Geometry.Point([longitude, latitude])
            plant_date = datetime.strptime(planting_date, '%Y-%m-%d')
            
            rainfall_by_phase = {}
            
            for start_day, end_day, trigger_mm, exit_mm, phase_name, water_need_mm, obs_window in crop_phases:
                # Calculate phase date range
                phase_start = plant_date + timedelta(days=start_day)
                phase_end = plant_date + timedelta(days=end_day)
                
                # Get CHIRPS data for this phase
                chirps = ee.ImageCollection('UCSB-CHG/CHIRPS/DAILY') \
                    .filterDate(phase_start.strftime('%Y-%m-%d'), phase_end.strftime('%Y-%m-%d')) \
                    .filterBounds(point)
                
                # Calculate total rainfall for this phase
                total_rainfall = chirps.sum().reduceRegion(
                    reducer=ee.Reducer.mean(),
                    geometry=point,
                    scale=5566,
                    maxPixels=1
                ).getInfo()
                
                phase_rainfall = total_rainfall.get('precipitation', 0)
                rainfall_by_phase[phase_name] = round(phase_rainfall, 1)
                
                print(f"🌧️ {phase_name}: {phase_rainfall:.1f}mm (need: {water_need_mm}mm)")
            
            return rainfall_by_phase
            
        except Exception as e:
            print(f"❌ Error calculating rainfall per phase: {e}")
            # Return empty dict as fallback
            return {phase[4]: 0.0 for phase in crop_phases}
    
    def _calculate_drought_impact_by_phases(self, crop_phases: List[Tuple], 
                                          rainfall_by_phase: Dict[str, float],
                                          crop: str) -> float:
        """Calculate drought impact using phase-specific weights from crops.py"""
        phase_weights = get_crop_phase_weights(crop)  # Use your function
        total_impact = 0.0
        
        for i, (start_day, end_day, trigger_mm, exit_mm, phase_name, water_need_mm, obs_window) in enumerate(crop_phases):
            actual_rainfall = rainfall_by_phase.get(phase_name, 0)
            
            # Calculate phase-specific stress
            if actual_rainfall >= water_need_mm:
                phase_stress = 0.0  # No stress
            else:
                # Linear stress calculation
                phase_stress = (water_need_mm - actual_rainfall) / water_need_mm
                phase_stress = min(phase_stress, 1.0)  # Cap at 100% stress
            
            # Apply phase weight
            weighted_impact = phase_stress * phase_weights[i] * 100
            total_impact += weighted_impact
            
            print(f"📊 {phase_name}: {actual_rainfall:.1f}mm/{water_need_mm}mm, "
                  f"stress: {phase_stress*100:.1f}%, weighted: {weighted_impact:.1f}%")
        
        return min(total_impact, 100.0)  # Cap total impact at 100%
    
    def _calculate_enhanced_quote_v2(self, params: Dict[str, Any], 
                                   year_analysis: List[Dict[str, Any]],
                                   planting_dates: Dict[int, str]) -> Dict[str, Any]:
        """ENHANCED: Calculate quote with dynamic deductible and custom loadings"""
        valid_years = [y for y in year_analysis if 'error' not in y]
        
        if not valid_years:
            raise ValueError("No valid years for quote calculation")
        
        # Calculate aggregated risk metrics
        avg_drought_impact = sum(y['drought_impact'] for y in valid_years) / len(valid_years)
        avg_premium_rate = sum(y['simulated_premium_rate'] for y in valid_years) / len(valid_years)
        avg_payout = sum(y['simulated_payout'] for y in valid_years) / len(valid_years)
        
        # Apply zone adjustments using your zone system
        zone_adjustments = self._get_zone_adjustments_from_crops(params)
        final_premium_rate = avg_premium_rate * zone_adjustments.get('risk_multiplier', 1.0)
        
        # Calculate financial metrics
        sum_insured = params['expected_yield'] * params['price_per_ton'] * params.get('area_ha', 1.0)
        
        # Calculate burning cost
        burning_cost = sum_insured * final_premium_rate
        
        # ENHANCED: Apply custom loadings or defaults
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
        
        # ENHANCED: Apply dynamic deductible
        deductible_amount = sum_insured * params['deductible_rate']
        
        # Generate enhanced phase breakdown using your crops.py
        phase_breakdown = self._generate_enhanced_phase_breakdown_from_crops(params['crop'], valid_years)
        
        # Create comprehensive quote result
        quote_result = {
            # Core quote data
            'crop': params['crop'],
            'year': params['year'],
            'quote_type': params['quote_type'],
            'latitude': params['latitude'],
            'longitude': params['longitude'],
            'area_ha': params.get('area_ha', 1.0),
            
            # Financial summary with enhancements
            'expected_yield': params['expected_yield'],
            'price_per_ton': params['price_per_ton'],
            'sum_insured': sum_insured,
            'premium_rate': final_premium_rate,
            'burning_cost': burning_cost,
            'loadings_breakdown': loadings_breakdown,    # ENHANCED: Detailed loadings
            'total_loadings': total_loadings_amount,     # ENHANCED: Total loading amount
            'gross_premium': gross_premium,              # ENHANCED: Updated calculation
            'deductible_rate': params['deductible_rate'], # ENHANCED: Dynamic deductible
            'deductible_amount': deductible_amount,      # ENHANCED: Deductible amount
            
            # Risk analysis
            'expected_payout_ratio': avg_drought_impact / 100.0,
            'historical_years_used': list(planting_dates.keys()),
            'zone': params.get('zone', 'auto_detected'),
            'zone_adjustments': zone_adjustments,
            
            # Enhanced metrics
            'phase_breakdown': phase_breakdown,
            'simulation_summary': {
                'years_analyzed': len(valid_years),
                'average_drought_impact': avg_drought_impact,
                'average_premium_rate': avg_premium_rate,
                'average_payout': avg_payout,
                'payout_frequency': len([y for y in valid_years if y['drought_impact'] > 5]) / len(valid_years) * 100,
                'maximum_loss_year': max(valid_years, key=lambda x: x['drought_impact'])['year'],
                'minimum_loss_year': min(valid_years, key=lambda x: x['drought_impact'])['year']
            },
            
            # Metadata
            'generated_at': datetime.utcnow().isoformat(),
            'methodology': 'rainfall_based_planting_enhanced_v2'
        }
        
        return quote_result
    
    def _generate_field_story(self, year_analysis: List[Dict[str, Any]], 
                            params: Dict[str, Any]) -> Dict[str, Any]:
        """ENHANCED: Generate field-level storytelling"""
        valid_years = [y for y in year_analysis if 'error' not in y]
        
        if not valid_years:
            return {"summary": "Insufficient data for field story generation"}
        
        # Calculate totals
        total_premiums = sum(y['simulated_premium_usd'] for y in valid_years)
        total_payouts = sum(y['simulated_payout'] for y in valid_years)
        net_position = total_payouts - total_premiums
        
        # Find best and worst years
        best_year = min(valid_years, key=lambda x: x['drought_impact'])
        worst_year = max(valid_years, key=lambda x: x['drought_impact'])
        
        # Calculate value metrics
        years_with_payouts = len([y for y in valid_years if y['simulated_payout'] > 0])
        payout_frequency = years_with_payouts / len(valid_years) * 100
        
        # Generate story
        risk_level = "low" if worst_year['drought_impact'] < 20 else "moderate" if worst_year['drought_impact'] < 50 else "high"
        
        summary = (f"Over the past {len(valid_years)} seasons at this {params['crop']} field, "
                  f"you would have paid ${total_premiums:,.0f} in premiums and received "
                  f"${total_payouts:,.0f} in payouts — ")
        
        if net_position > 0:
            summary += f"indicating you would have gained ${net_position:,.0f} from insurance coverage."
        elif net_position < 0:
            summary += f"indicating you would have paid ${abs(net_position):,.0f} net for risk protection."
        else:
            summary += "indicating break-even insurance performance."
        
        summary += f" This represents {risk_level} historical drought risk at this location."
        
        return {
            "summary": summary,
            "historical_performance": {
                "total_seasons": len(valid_years),
                "total_premiums_paid": total_premiums,
                "total_payouts_received": total_payouts,
                "net_farmer_position": net_position,
                "payout_frequency_percent": payout_frequency
            },
            "best_year": {
                "year": best_year['year'],
                "drought_impact": best_year['drought_impact'],
                "description": f"Excellent growing conditions with only {best_year['drought_impact']:.1f}% drought impact"
            },
            "worst_year": {
                "year": worst_year['year'],
                "drought_impact": worst_year['drought_impact'],
                "payout": worst_year['simulated_payout'],
                "description": f"Severe drought year with {worst_year['drought_impact']:.1f}% loss and ${worst_year['simulated_payout']:,.0f} payout"
            },
            "value_for_money": {
                "loss_ratio": (total_payouts / total_premiums) if total_premiums > 0 else 0,
                "interpretation": "High value" if (total_payouts / total_premiums) > 0.8 else "Standard value" if (total_payouts / total_premiums) > 0.4 else "Low claims experience"
            }
        }
    
    def _generate_enhanced_phase_breakdown_from_crops(self, crop: str, 
                                                    valid_years: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate enhanced phase breakdown using your crops.py structure"""
        crop_phases = get_crop_phases(crop)
        enhanced_phases = []
        
        for i, (start_day, end_day, trigger_mm, exit_mm, phase_name, water_need_mm, obs_window) in enumerate(crop_phases):
            # Collect rainfall data across years for this phase
            phase_rainfall_data = []
            for year_data in valid_years:
                rainfall_by_phase = year_data.get('rainfall_mm_by_phase', {})
                if phase_name in rainfall_by_phase:
                    phase_rainfall_data.append(rainfall_by_phase[phase_name])
            
            # Calculate phase statistics
            if phase_rainfall_data:
                avg_rainfall = sum(phase_rainfall_data) / len(phase_rainfall_data)
                min_rainfall = min(phase_rainfall_data)
                max_rainfall = max(phase_rainfall_data)
                stress_years = len([r for r in phase_rainfall_data if r < water_need_mm])
            else:
                avg_rainfall = min_rainfall = max_rainfall = 0
                stress_years = 0
            
            enhanced_phase = {
                'phase_number': i + 1,
                'phase_name': phase_name,
                'start_day': start_day,
                'end_day': end_day,
                'duration_days': end_day - start_day + 1,
                'trigger_mm': trigger_mm,
                'exit_mm': exit_mm,
                'water_need_mm': water_need_mm,
                'observation_window_days': obs_window,
                'historical_rainfall': {
                    'average_mm': round(avg_rainfall, 1),
                    'minimum_mm': round(min_rainfall, 1),
                    'maximum_mm': round(max_rainfall, 1),
                    'stress_years_count': stress_years,
                    'stress_frequency_percent': (stress_years / len(valid_years) * 100) if valid_years else 0
                }
            }
            
            enhanced_phases.append(enhanced_phase)
        
        return enhanced_phases
    
    def _get_zone_adjustments_from_crops(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get zone adjustments using your crops.py zone system"""
        zone = params.get('zone', 'auto_detect')
        
        if zone == 'auto_detect':
            zone = self._auto_detect_zone(params['latitude'], params['longitude'])
        
        # Use your zone configuration from crops.py
        zone_config = get_zone_config(zone)
        
        # Convert to expected format with risk multiplier
        risk_multiplier = 1.0
        if zone == 'aez_4_masvingo':
            risk_multiplier = 1.2  # Higher risk in semi-arid
        elif zone == 'aez_5_lowveld':
            risk_multiplier = 1.4  # Highest risk in lowveld
        elif zone == 'aez_3_midlands':
            risk_multiplier = 0.9  # Lower risk in midlands
        
        return {
            'zone_name': zone_config['name'],
            'risk_multiplier': risk_multiplier,
            'description': zone_config['description'],
            'primary_risk': zone_config['primary_risk'],
            'annual_rainfall_range': zone_config['annual_rainfall_range']
        }
    
    def _auto_detect_zone(self, latitude: float, longitude: float) -> str:
        """Auto-detect agro-ecological zone based on coordinates with enhanced logic"""
        # Enhanced zone detection for Zimbabwe/Southern Africa
        if latitude > -17.0:
            return 'aez_3_midlands'  # Northern areas - better rainfall
        elif latitude > -19.0:
            return 'aez_4_masvingo'  # Central areas - moderate risk
        else:
            return 'aez_5_lowveld'   # Southern areas - high drought risk
    
    def _get_zone_risk_multiplier(self, params: Dict[str, Any]) -> float:
        """Get zone-specific risk multiplier"""
        zone_adjustments = self._get_zone_adjustments_from_crops(params)
        return zone_adjustments.get('risk_multiplier', 1.0)
    
    def _get_planting_windows_summary(self, planting_dates: Dict[int, Optional[str]]) -> Dict:
        """Generate summary statistics for planting windows"""
        valid_dates = [date for date in planting_dates.values() if date is not None]
        
        if not valid_dates:
            return {
                "detection_rate": 0.0,
                "average_planting_date": None,
                "earliest_planting": None,
                "latest_planting": None,
                "planting_spread_days": None,
                "successful_years": 0,
                "total_years": len(planting_dates)
            }
        
        # Calculate average planting date
        date_objects = [datetime.strptime(date, '%Y-%m-%d') for date in valid_dates]
        
        # Convert to day of year for averaging (handling year transition)
        days_of_year = []
        for date_obj in date_objects:
            day_of_year = date_obj.timetuple().tm_yday
            # Adjust for October-January season
            if day_of_year >= 274:  # October onwards
                adjusted_day = day_of_year - 274  # Oct 1 = day 0
            else:  # January
                adjusted_day = day_of_year + (365 - 274)  # Jan 1 = day 92
            days_of_year.append(adjusted_day)
        
        avg_day = sum(days_of_year) / len(days_of_year)
        
        # Convert back to readable date
        if avg_day <= 91:  # October-December
            avg_date = datetime(2000, 10, 1) + timedelta(days=int(avg_day))
        else:  # January
            days_into_jan = avg_day - 92
            avg_date = datetime(2001, 1, 1) + timedelta(days=int(days_into_jan))
        
        return {
            "detection_rate": len(valid_dates) / len(planting_dates) * 100,
            "average_planting_date": avg_date.strftime('%B %d'),
            "earliest_planting": min(valid_dates),
            "latest_planting": max(valid_dates),
            "planting_spread_days": max(days_of_year) - min(days_of_year) if len(days_of_year) > 1 else 0,
            "successful_years": len(valid_dates),
            "total_years": len(planting_dates)
        }
    
    def _validate_seasonal_planting_dates(self, planting_dates: Dict[int, Optional[str]]) -> Dict[int, str]:
        """Validate planting dates fall within acceptable seasonal windows"""
        valid_dates = {}
        
        for year, date_str in planting_dates.items():
            if date_str is None:
                continue
                
            try:
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                
                # Check if planting month is valid
                if date_obj.month in self.valid_planting_months:
                    valid_dates[year] = date_str
                    print(f"✅ {year}: Valid seasonal planting on {date_str}")
                else:
                    print(f"❌ {year}: Off-season planting rejected ({date_str} - month {date_obj.month})")
                    
            except Exception as e:
                print(f"❌ {year}: Invalid date format ({date_str}): {e}")
                
        return valid_dates
    
    def _determine_quote_type_with_validation(self, year: int) -> str:
        """Determine quote type with seasonal validation"""
        current_year = datetime.now().year
        current_month = datetime.now().month
        
        # For prospective quotes, ensure we're not suggesting off-season planting
        if year > current_year:
            return "prospective"
        elif year == current_year:
            # Check if we're still within or approaching planting season
            if current_month >= 8:  # August onwards - approaching season
                return "prospective"
            else:  # Too late for current season
                return "historical"
        else:
            return "historical"
    
    def _determine_quote_type(self, year: int) -> str:
        """Determine quote type based on year - backward compatibility"""
        return self._determine_quote_type_with_validation(year)
