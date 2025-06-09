"""
Refined Quote Engine with detailed year-by-year simulation
Implements proper seasonal logic and individual year analysis
"""

import ee
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import numpy as np

from core.drought_analyzer import DroughtAnalyzer
from core.refined_planting_detection import RefinedPlantingDetector  # Updated import
from core.zones import get_zone_adjustments
from core.crops import CROP_PHASES, get_crop_info, validate_crop

class RefinedQuoteEngine:
    """Enhanced quote engine with detailed year-by-year simulation"""
    
    def __init__(self):
        """Initialize with refined components"""
        self.drought_analyzer = DroughtAnalyzer()
        self.planting_detector = RefinedPlantingDetector()  # Updated detector
        
        # Enhanced simulation parameters
        self.base_loading_factor = 1.5  # Base loading multiplier
        self.minimum_premium_rate = 0.015  # 1.5% minimum
        self.maximum_premium_rate = 0.25   # 25% maximum
        
        # Seasonal validation
        self.valid_planting_months = [10, 11, 12, 1]  # Oct-Jan only
        
        print("🔧 Refined Quote Engine initialized")
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
            planting_dates = self.planting_detector.detect_planting_dates(
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
                'detection_summary': self.planting_detector.get_planting_windows_summary(planting_dates),
                'valid_seasons': len(valid_planting_dates),
                'total_years_analyzed': len(historical_years)
            }
            
            execution_time = (datetime.now() - start_time).total_seconds()
            quote_result['execution_time_seconds'] = round(execution_time, 2)
            quote_result['version'] = "2.1.0-Refined"
            
            print(f"✅ Quote completed in {execution_time:.2f} seconds")
            print(f"💰 Premium rate: {quote_result['premium_rate']*100:.2f}%")
            print(f"💵 Premium amount: ${quote_result['gross_premium']:,.2f}")
            
            return quote_result
            
        except Exception as e:
            print(f"❌ Quote execution error: {e}")
            raise
    
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
                year_analysis = self._analyze_individual_year(params, year, planting_date)
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
    
    def _analyze_individual_year(self, params: Dict[str, Any], year: int, 
                               planting_date: str) -> Dict[str, Any]:
        """
        Analyze drought risk and simulate premium/payout for individual year
        
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
        
        # Perform drought analysis for this specific year
        drought_result = self.drought_analyzer.analyze_drought_risk(
            latitude=params['latitude'],
            longitude=params['longitude'],
            crop=params['crop'],
            planting_date=planting_date,
            season_end_date=season_end.strftime('%Y-%m-%d')
        )
        
        # Calculate individual year risk metrics
        phase_losses = drought_result.get('phase_analysis', [])
        total_loss = sum(phase.get('normalized_loss', 0) for phase in phase_losses)
        drought_impact = min(total_loss * 100, 100.0)  # Cap at 100%
        
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
            'phase_losses': phase_losses,
            'simulated_premium_rate': individual_premium_rate,
            'simulated_premium_usd': simulated_premium,
            'simulated_payout': simulated_payout,
            'net_result': simulated_payout - simulated_premium,  # Farmer perspective
            'loss_ratio': (simulated_payout / simulated_premium) if simulated_premium > 0 else 0,
            'rainfall_adequacy': drought_result.get('season_summary', {}).get('total_rainfall', 0),
            'critical_periods': len([p for p in phase_losses if p.get('normalized_loss', 0) > 0.1])
        }
    
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
        avg_drought_impact = np.mean([y['drought_impact'] for y in valid_years])
        avg_premium_rate = np.mean([y['simulated_premium_rate'] for y in valid_years])
        avg_payout = np.mean([y['simulated_payout'] for y in valid_years])
        
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
        phase_breakdown = self._generate_enhanced_phase_breakdown(params, valid_years)
        
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
            'methodology': 'rainfall_based_planting_refined_simulation'
        }
        
        return quote_result
    
    def _generate_enhanced_phase_breakdown(self, params: Dict[str, Any], 
                                         valid_years: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Generate enhanced phase breakdown from year-by-year analysis
        
        Args:
            params: Quote parameters
            valid_years: Valid year analysis results
            
        Returns:
            Enhanced phase breakdown
        """
        crop_phases = CROP_PHASES.get(params['crop'], CROP_PHASES['maize'])
        enhanced_phases = []
        
        for i, phase_template in enumerate(crop_phases):
            # Aggregate phase data across years
            phase_losses = []
            for year_data in valid_years:
                year_phases = year_data.get('phase_losses', [])
                if i < len(year_phases):
                    phase_losses.append(year_phases[i].get('normalized_loss', 0))
            
            avg_loss = np.mean(phase_losses) if phase_losses else 0
            max_loss = max(phase_losses) if phase_losses else 0
            loss_frequency = len([l for l in phase_losses if l > 0.05]) / len(phase_losses) * 100 if phase_losses else 0
            
            enhanced_phase = {
                **phase_template,
                'phase_number': i + 1,
                'historical_performance': {
                    'average_loss': avg_loss,
                    'maximum_loss': max_loss,
                    'loss_frequency_percent': loss_frequency,
                    'years_with_significant_loss': len([l for l in phase_losses if l > 0.1])
                }
            }
            
            enhanced_phases.append(enhanced_phase)
        
        return enhanced_phases
    
    def _get_zone_risk_multiplier(self, params: Dict[str, Any]) -> float:
        """Get zone-specific risk multiplier"""
        zone = params.get('zone', 'auto_detect')
        
        if zone == 'auto_detect':
            # Auto-detect based on coordinates
            zone = self._auto_detect_zone(params['latitude'], params['longitude'])
        
        zone_adjustments = get_zone_adjustments(zone)
        return zone_adjustments.get('risk_multiplier', 1.0)
    
    def _get_zone_adjustments(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Get comprehensive zone adjustments"""
        zone = params.get('zone', 'auto_detect')
        
        if zone == 'auto_detect':
            zone = self._auto_detect_zone(params['latitude'], params['longitude'])
        
        return get_zone_adjustments(zone)
    
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
        validate_crop(crop)
        
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
