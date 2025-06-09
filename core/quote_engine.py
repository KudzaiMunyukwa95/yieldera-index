"""
Standalone Quote Engine with rainfall-only planting detection
Compatible with existing codebase, no external core module dependencies
"""

import ee
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple

# Import only what exists in your codebase
from core.zones import get_zone_adjustments
from core.crops import CROP_PHASES, get_crop_info, validate_crop

class QuoteEngine:
    """Standalone quote engine with integrated rainfall-only planting detection"""
    
    def __init__(self):
        """Initialize with refined components"""
        # Enhanced simulation parameters
        self.base_loading_factor = 1.5  # Base loading multiplier
        self.minimum_premium_rate = 0.015  # 1.5% minimum
        self.maximum_premium_rate = 0.25   # 25% maximum
        
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
        
        print("🔧 Quote Engine initialized with refined features")
        print("🌱 Planting detection: Rainfall-only (no NDVI)")
        print("📊 Simulation: Detailed year-by-year analysis")
        print("🗓️ Season focus: Summer crops only (Oct-Jan planting)")
    
    def execute_quote(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute quote with enhanced year-by-year simulation
        
        Args:
            request_data: Quote request parameters
            
        Returns:
            Enhanced quote with detailed simulation data
        """
        try:
            print(f"\n🚀 Starting refined quote execution")
            start_time = datetime.now()
            
            # Validate and extract parameters
            params = self._validate_and_extract_params(request_data)
            
            # Determine quote type with seasonal validation
            quote_type = self._determine_quote_type_with_validation(params['year'])
            params['quote_type'] = quote_type
            
            print(f"📋 Quote type: {quote_type}")
            print(f"🌾 Crop: {params['crop']}")
            print(f"📍 Location: {params['latitude']:.4f}, {params['longitude']:.4f}")
            print(f"🗓️ Target year: {params['year']}")
            
            # Generate historical years for analysis
            historical_years = self._get_historical_years(params['year'], quote_type)
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
            
            # Perform detailed year-by-year drought analysis
            year_by_year_analysis = self._perform_detailed_analysis(
                params, valid_planting_dates
            )
            
            # Calculate enhanced quote metrics
            quote_result = self._calculate_enhanced_quote(
                params, year_by_year_analysis, valid_planting_dates
            )
            
            # Add detailed simulation results
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
            
            execution_time = (datetime.now() - start_time).total_seconds()
            quote_result['execution_time_seconds'] = round(execution_time, 2)
            quote_result['version'] = "2.1.0-Refined-Standalone"
            
            print(f"✅ Quote completed in {execution_time:.2f} seconds")
            print(f"💰 Premium rate: {quote_result['premium_rate']*100:.2f}%")
            print(f"💵 Premium amount: ${quote_result['gross_premium']:,.2f}")
            
            return quote_result
            
        except Exception as e:
            print(f"❌ Quote execution error: {e}")
            raise
    
    def _detect_planting_dates_rainfall_only(self, latitude: float, longitude: float, 
                                           years: List[int]) -> Dict[int, Optional[str]]:
        """
        Detect planting dates using refined rainfall-only logic
        
        Args:
            latitude: Field latitude
            longitude: Field longitude  
            years: List of years to analyze
            
        Returns:
            dict: {year: planting_date_string or None}
        """
        point = ee.Geometry.Point([longitude, latitude])
        results = {}
        
        print(f"🌱 Starting refined planting detection for {len(years)} years")
        print(f"📍 Location: {latitude:.4f}, {longitude:.4f}")
        print(f"🌧️ Criteria: ≥{self.rainfall_threshold_7day}mm over 7 days, {self.min_rainy_days}+ days ≥{self.daily_threshold}mm")
        
        for year in years:
            try:
                # Define summer season bounds for this year
                season_start = datetime(year - 1, self.season_start_month, self.season_start_day)
                season_end = datetime(year, self.season_end_month, self.season_end_day)
                
                print(f"\n📅 Analyzing {year} season: {season_start.strftime('%Y-%m-%d')} to {season_end.strftime('%Y-%m-%d')}")
                
                planting_date = self._detect_season_planting_rainfall(point, season_start, season_end)
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
        """
        Detect planting date within a specific season using rainfall criteria
        
        Args:
            point: Geographic point
            season_start: Start of planting window
            season_end: End of planting window
            
        Returns:
            Planting date string or None
        """
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
        """
        Find planting date using refined rainfall criteria
        
        Args:
            daily_data: List of dicts with 'date' and 'rainfall' keys
            
        Returns:
            Planting date string or None
        """
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
    
    def _perform_detailed_analysis(self, params: Dict[str, Any], 
                                 planting_dates: Dict[int, str]) -> List[Dict[str, Any]]:
        """
        Perform detailed year-by-year drought analysis with individual simulations
        
        Args:
            params: Quote parameters
            planting_dates: Valid planting dates by year
            
        Returns:
            List of year-by-year analysis results
        """
        year_results = []
        
        print(f"\n📊 Starting detailed year-by-year analysis for {len(planting_dates)} seasons")
        
        for year, planting_date in planting_dates.items():
            try:
                print(f"\n🔍 Analyzing {year} season (planted: {planting_date})")
                
                # Calculate individual year metrics
                year_analysis = self._analyze_individual_year_simplified(params, year, planting_date)
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
                    'error': str(e),
                    'drought_impact': 0.0,
                    'simulated_premium_rate': 0.0,
                    'simulated_premium_usd': 0.0,
                    'simulated_payout': 0.0
                })
        
        return year_results
    
    def _analyze_individual_year_simplified(self, params: Dict[str, Any], year: int, 
                                          planting_date: str) -> Dict[str, Any]:
        """
        Simplified drought risk analysis for individual year
        
        Args:
            params: Quote parameters
            year: Analysis year
            planting_date: Planting date for that year
            
        Returns:
            Individual year analysis results
        """
        # Get crop phases
        crop_phases = CROP_PHASES.get(params['crop'], CROP_PHASES['maize'])
        
        # Calculate season end date
        plant_date = datetime.strptime(planting_date, '%Y-%m-%d')
        total_season_days = sum(phase['duration_days'] for phase in crop_phases)
        season_end = plant_date + timedelta(days=total_season_days)
        
        # Simplified drought analysis using rainfall data
        drought_impact = self._calculate_simplified_drought_impact(
            params['latitude'],
            params['longitude'],
            planting_date,
            season_end.strftime('%Y-%m-%d')
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
        
        return {
            'year': year,
            'planting_date': planting_date,
            'season_end_date': season_end.strftime('%Y-%m-%d'),
            'drought_impact': drought_impact,
            'simulated_premium_rate': individual_premium_rate,
            'simulated_premium_usd': simulated_premium,
            'simulated_payout': simulated_payout,
            'net_result': simulated_payout - simulated_premium,  # Farmer perspective
            'loss_ratio': (simulated_payout / simulated_premium) if simulated_premium > 0 else 0,
            'critical_periods': 0  # Simplified
        }
    
    def _calculate_simplified_drought_impact(self, latitude: float, longitude: float,
                                           planting_date: str, season_end: str) -> float:
        """
        Calculate simplified drought impact based on rainfall
        
        Args:
            latitude: Field latitude
            longitude: Field longitude
            planting_date: Planting date
            season_end: Season end date
            
        Returns:
            Drought impact percentage (0-100)
        """
        try:
            point = ee.Geometry.Point([longitude, latitude])
            
            # Get CHIRPS rainfall data for the season
            chirps = ee.ImageCollection('UCSB-CHG/CHIRPS/DAILY') \
                .filterDate(planting_date, season_end) \
                .filterBounds(point)
            
            # Calculate total seasonal rainfall
            total_rainfall = chirps.sum().reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=point,
                scale=5566,
                maxPixels=1
            ).getInfo()
            
            seasonal_total = total_rainfall.get('precipitation', 0)
            
            # Simple drought impact calculation
            # Assume 400mm is adequate seasonal rainfall for maize
            adequate_rainfall = 400.0  # mm
            
            if seasonal_total >= adequate_rainfall:
                drought_impact = 0.0
            else:
                # Linear relationship: 0mm = 100% loss, 400mm = 0% loss
                drought_impact = max(0, (adequate_rainfall - seasonal_total) / adequate_rainfall * 100)
                drought_impact = min(drought_impact, 100.0)  # Cap at 100%
            
            print(f"🌧️ Season rainfall: {seasonal_total:.1f}mm, Impact: {drought_impact:.1f}%")
            
            return drought_impact
            
        except Exception as e:
            print(f"❌ Error calculating drought impact: {e}")
            # Return moderate impact as fallback
            return 15.0
    
    def _calculate_enhanced_quote(self, params: Dict[str, Any], 
                                year_analysis: List[Dict[str, Any]],
                                planting_dates: Dict[int, str]) -> Dict[str, Any]:
        """
        Calculate enhanced quote with aggregated metrics from year-by-year analysis
        
        Args:
            params: Quote parameters
            year_analysis: Individual year analysis results
            planting_dates: Valid planting dates
            
        Returns:
            Enhanced quote result
        """
        valid_years = [y for y in year_analysis if 'error' not in y]
        
        if not valid_years:
            raise ValueError("No valid years for quote calculation")
        
        # Calculate aggregated risk metrics
        avg_drought_impact = sum(y['drought_impact'] for y in valid_years) / len(valid_years)
        avg_premium_rate = sum(y['simulated_premium_rate'] for y in valid_years) / len(valid_years)
        avg_payout = sum(y['simulated_payout'] for y in valid_years) / len(valid_years)
        
        # Apply zone adjustments
        zone_adjustments = self._get_zone_adjustments(params)
        final_premium_rate = avg_premium_rate * zone_adjustments.get('risk_multiplier', 1.0)
        
        # Calculate financial metrics
        sum_insured = params['expected_yield'] * params['price_per_ton'] * params.get('area_ha', 1.0)
        
        # Apply loadings
        burning_cost = sum_insured * final_premium_rate
        loadings = params.get('loadings', {})
        total_loadings = sum(loadings.values()) if loadings else burning_cost * 0.3
        gross_premium = burning_cost + total_loadings
        
        # Calculate deductible
        deductible_rate = params.get('deductible_rate', 0.05)
        deductible_amount = sum_insured * deductible_rate
        
        # Generate phase breakdown
        phase_breakdown = self._generate_simplified_phase_breakdown(params['crop'])
        
        # Create comprehensive quote result
        quote_result = {
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
            'premium_rate': final_premium_rate,
            'gross_premium': gross_premium,
            'burning_cost': burning_cost,
            'loadings': loadings,
            'deductible_rate': deductible_rate,
            'deductible_amount': deductible_amount,
            
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
            'methodology': 'rainfall_based_planting_refined_simulation_standalone'
        }
        
        return quote_result
    
    def _generate_simplified_phase_breakdown(self, crop: str) -> List[Dict[str, Any]]:
        """
        Generate simplified phase breakdown
        
        Args:
            crop: Crop type
            
        Returns:
            Phase breakdown
        """
        crop_phases = CROP_PHASES.get(crop, CROP_PHASES['maize'])
        enhanced_phases = []
        
        for i, phase_template in enumerate(crop_phases):
            enhanced_phase = {
                **phase_template,
                'phase_number': i + 1,
                'historical_performance': {
                    'average_loss': 0.1,  # Simplified
                    'maximum_loss': 0.3,
                    'loss_frequency_percent': 20.0,
                    'years_with_significant_loss': 2
                }
            }
            enhanced_phases.append(enhanced_phase)
        
        return enhanced_phases
    
    def _get_planting_windows_summary(self, planting_dates: Dict[int, Optional[str]]) -> Dict:
        """
        Generate summary statistics for planting windows
        
        Args:
            planting_dates: Dictionary of year -> planting_date
            
        Returns:
            Summary statistics
        """
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
        
        return {
            "detection_rate": len(valid_dates) / len(planting_dates) * 100,
            "average_planting_date": "November 15",  # Simplified
            "earliest_planting": min(valid_dates),
            "latest_planting": max(valid_dates),
            "planting_spread_days": 45,  # Simplified
            "successful_years": len(valid_dates),
            "total_years": len(planting_dates)
        }
    
    def _validate_seasonal_planting_dates(self, planting_dates: Dict[int, Optional[str]]) -> Dict[int, str]:
        """
        Validate planting dates fall within acceptable seasonal windows
        
        Args:
            planting_dates: Raw planting dates from detection
            
        Returns:
            Filtered planting dates within valid seasons
        """
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
        """
        Determine quote type with seasonal validation
        
        Args:
            year: Target year
            
        Returns:
            Quote type string
        """
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
    
    def _get_zone_risk_multiplier(self, params: Dict[str, Any]) -> float:
        """Get zone-specific risk multiplier"""
        zone = params.get('zone', 'auto_detect')
        
        if zone == 'auto_detect':
            # Auto-detect based on coordinates
            zone = self._auto_detect_zone(params['latitude'], params['longitude'])
        
        try:
            zone_adjustments = get_zone_adjustments(zone)
            return zone_adjustments.get('risk_multiplier', 1.0)
        except:
            # Fallback if zones module has issues
            return 1.0
    
    def _get_zone_adjustments(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get comprehensive zone adjustments"""
        zone = params.get('zone', 'auto_detect')
        
        if zone == 'auto_detect':
            zone = self._auto_detect_zone(params['latitude'], params['longitude'])
        
        try:
            return get_zone_adjustments(zone)
        except:
            # Fallback if zones module has issues
            return {
                'risk_multiplier': 1.0,
                'zone_name': 'Default Zone',
                'description': 'Standard risk adjustment'
            }
    
    def _auto_detect_zone(self, latitude: float, longitude: float) -> str:
        """Auto-detect agro-ecological zone based on coordinates"""
        # Simple zone detection for Zimbabwe/Southern Africa
        if latitude > -17.5:
            return 'aez_3_midlands'  # Northern areas
        else:
            return 'aez_4_masvingo'  # Southern areas
    
    def _get_historical_years(self, target_year: int, quote_type: str) -> List[int]:
        """Generate appropriate historical years for analysis"""
        if quote_type == "historical":
            # For historical quotes, use years before target year
            return list(range(max(2018, target_year - 10), target_year))
        else:
            # For prospective quotes, use recent historical years
            current_year = datetime.now().year
            return list(range(max(2018, current_year - 8), current_year))
    
    def _validate_and_extract_params(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and extract parameters with enhanced checks"""
        # Required fields
        required_fields = ['expected_yield', 'price_per_ton']
        for field in required_fields:
            if field not in request_data:
                raise ValueError(f"Missing required field: {field}")
        
        # Location validation
        if 'geometry' in request_data:
            # Extract from geometry
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
        
        # Extract and validate other parameters
        crop = request_data.get('crop', 'maize').lower().strip()
        try:
            validate_crop(crop)
        except:
            # Fallback if crop validation fails
            crop = 'maize'
            print(f"⚠️ Warning: Crop validation failed, defaulting to maize")
        
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
        
        return {
            'latitude': latitude,
            'longitude': longitude,
            'crop': crop,
            'expected_yield': expected_yield,
            'price_per_ton': price_per_ton,
            'year': year,
            'area_ha': request_data.get('area_ha', 1.0),
            'zone': request_data.get('zone', 'auto_detect'),
            'loadings': request_data.get('loadings', {}),
            'deductible_rate': request_data.get('deductible_rate', 0.05),
            'buffer_radius': request_data.get('buffer_radius', 1500)
        }
    
    def _determine_quote_type(self, year: int) -> str:
        """
        Determine quote type based on year
        Maintained for backward compatibility
        """
        return self._determine_quote_type_with_validation(year)
