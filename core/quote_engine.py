"""
Core quote engine for index insurance calculations
"""

import ee
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple, Optional
import statistics
import traceback

from config import Config
from core.gee_client import RainfallExtractor
from core.crops import (
    validate_crop, get_crop_config, get_crop_phase_weights, 
    get_zone_config, CropPhenologyCalculator
)

class QuoteEngine:
    """Main quote engine for index insurance calculations"""
    
    def __init__(self):
        self.rainfall_extractor = RainfallExtractor()
        self.default_deductible = Config.DEFAULT_DEDUCTIBLE
        self.historical_range = Config.HISTORICAL_YEARS_RANGE
        self.min_valid_years = Config.MIN_VALID_YEARS
    
    def execute_quote(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a quote based on request parameters
        
        Args:
            request_data: Quote request parameters
            
        Returns:
            dict: Complete quote result
        """
        try:
            # Validate and extract parameters
            quote_params = self._validate_quote_request(request_data)
            
            # Determine quote type
            quote_type = self._determine_quote_type(quote_params['year'])
            
            # Execute appropriate quote type
            if quote_type == 'historical':
                return self._execute_historical_quote(quote_params)
            else:
                return self._execute_prospective_quote(quote_params)
                
        except Exception as e:
            print(f"Quote execution error: {e}")
            print(traceback.format_exc())
            raise
    
    def _validate_quote_request(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and normalize quote request parameters"""
        
        # Required fields
        required_fields = ['expected_yield', 'price_per_ton']
        for field in required_fields:
            if field not in request_data:
                raise ValueError(f"Missing required field: {field}")
        
        # Geometry validation
        geometry = None
        if 'geometry' in request_data:
            geometry = ee.Geometry(request_data['geometry'])
        elif 'latitude' in request_data and 'longitude' in request_data:
            lat = float(request_data['latitude'])
            lng = float(request_data['longitude'])
            buffer_radius = request_data.get('buffer_radius', 1500)
            
            point = ee.Geometry.Point([lng, lat])
            geometry = point.buffer(buffer_radius)
        else:
            raise ValueError("Must provide either 'geometry' or 'latitude'/'longitude'")
        
        # Normalize parameters
        params = {
            'geometry': geometry,
            'year': request_data.get('year', datetime.now().year),
            'crop': validate_crop(request_data.get('crop', 'maize')),
            'expected_yield': float(request_data['expected_yield']),
            'price_per_ton': float(request_data['price_per_ton']),
            'area_ha': float(request_data['area_ha']) if request_data.get('area_ha') else None,
            'loadings': request_data.get('loadings', {}),
            'zone': request_data.get('zone', 'auto_detect'),
            'deductible_rate': request_data.get('deductible_rate', self.default_deductible),
            'deductible_threshold': request_data.get('deductible_threshold', 0.0),
            'field_info': request_data.get('field_info', {})
        }
        
        return params
    
    def _determine_quote_type(self, year: int) -> str:
        """Determine quote type based on year"""
        current_year = datetime.now().year
        current_month = datetime.now().month
        
        if year < current_year:
            return "historical"
        elif year == current_year and current_month >= 11:
            return "historical"  # Current season with potential data
        else:
            return "prospective"
    
    def _execute_historical_quote(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute historical quote for a specific year"""
        
        geometry = params['geometry']
        year = params['year']
        crop = params['crop']
        
        print(f"Executing historical quote for {crop} in {year}")
        
        # Detect planting date
        planting_date, planting_description = self.rainfall_extractor.detect_planting_date(
            geometry, year, crop
        )
        
        if not planting_date:
            raise Exception(f"Unable to detect planting date for {year}")
        
        # Calculate phase details
        phase_details = self._calculate_phase_payouts(
            geometry, planting_date, year, crop, params['zone']
        )
        
        if not phase_details:
            raise Exception("Failed to calculate phase details")
        
        # Calculate financial metrics
        quote_result = self._calculate_quote_financials(
            phase_details, 
            params['expected_yield'],
            params['price_per_ton'],
            params['area_ha'],
            params['loadings'],
            params['deductible_rate'],
            params['deductible_threshold']
        )
        
        # Add metadata
        quote_result.update({
            "quote_type": "historical",
            "year": year,
            "crop": crop,
            "planting_date": planting_date,
            "planting_trigger_description": planting_description,
            "phase_breakdown": phase_details,
            "historical_years_used": [year],
            "zone": params['zone'],
            "zone_name": get_zone_config(params['zone'])["name"],
            "note": f"Historical analysis for {year} season based on actual CHIRPS rainfall data"
        })
        
        return quote_result
    
    def _execute_prospective_quote(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute prospective quote based on historical analysis"""
        
        geometry = params['geometry']
        year = params['year']
        crop = params['crop']
        
        print(f"Executing prospective quote for {crop} in {year}")
        
        # Determine historical reference period
        current_year = datetime.now().year
        lookback_years = min(self.historical_range, current_year - 2010)
        start_year = max(2010, current_year - lookback_years)
        end_year = current_year - 1
        
        historical_years = list(range(start_year, end_year + 1))
        
        # Analyze historical years
        valid_years = []
        all_phase_payouts = {i: [] for i in range(4)}  # Track payouts by phase
        valid_planting_dates = []
        
        for hist_year in historical_years:
            try:
                # Detect planting date for historical year
                planting_date, _ = self.rainfall_extractor.detect_planting_date(
                    geometry, hist_year, crop
                )
                
                if planting_date:
                    # Calculate phase payouts for this year
                    phase_details = self._calculate_phase_payouts(
                        geometry, planting_date, hist_year, crop, params['zone']
                    )
                    
                    if phase_details and len(phase_details) == 4:
                        valid_years.append(hist_year)
                        valid_planting_dates.append(planting_date)
                        
                        # Collect payout ratios by phase
                        for i, phase in enumerate(phase_details):
                            all_phase_payouts[i].append(phase['payout_ratio'])
                        
            except Exception as e:
                print(f"Error processing historical year {hist_year}: {e}")
                continue
        
        if len(valid_years) < self.min_valid_years:
            raise Exception(f"Insufficient historical data. Found {len(valid_years)} valid years, need at least {self.min_valid_years}")
        
        # Calculate average payout ratios per phase
        crop_config = get_crop_config(crop)
        phase_weights = get_crop_phase_weights(crop, params['zone'])
        
        avg_phase_details = []
        total_avg_payout_ratio = 0
        
        for phase_idx in range(4):
            phase_payouts = all_phase_payouts[phase_idx]
            avg_payout_ratio = statistics.mean(phase_payouts) if phase_payouts else 0
            
            phase_detail = {
                "phase_name": ["Emergence", "Vegetative", "Flowering", "Grain Fill"][phase_idx],
                "phase_number": phase_idx + 1,
                "payout_ratio": round(avg_payout_ratio, 4),
                "phase_weight": round(phase_weights[phase_idx], 4),
                "historical_payouts": phase_payouts,
                "min_payout": round(min(phase_payouts), 4) if phase_payouts else 0,
                "max_payout": round(max(phase_payouts), 4) if phase_payouts else 0,
                "std_dev": round(statistics.stdev(phase_payouts), 4) if len(phase_payouts) > 1 else 0
            }
            
            avg_phase_details.append(phase_detail)
            total_avg_payout_ratio += phase_weights[phase_idx] * avg_payout_ratio
        
        # Calculate financial metrics using historical averages
        quote_result = self._calculate_prospective_financials(
            avg_phase_details,
            total_avg_payout_ratio,
            params['expected_yield'],
            params['price_per_ton'],
            params['area_ha'],
            params['loadings'],
            params['deductible_rate'],
            params['deductible_threshold']
        )
        
        # Add metadata
        quote_result.update({
            "quote_type": "prospective",
            "year": year,
            "crop": crop,
            "phase_breakdown": avg_phase_details,
            "historical_years_used": valid_years,
            "analysis_period": f"{start_year}-{end_year}",
            "data_points": len(valid_years),
            "zone": params['zone'],
            "zone_name": get_zone_config(params['zone'])["name"],
            "planting_window_estimate": self._estimate_planting_window(valid_planting_dates),
            "note": f"Prospective quote based on {len(valid_years)} years of historical rainfall analysis"
        })
        
        return quote_result
    
    def _calculate_phase_payouts(self, geometry, planting_date: str, year: int, crop: str, zone: str) -> List[Dict[str, Any]]:
        """Calculate detailed phase payout analysis"""
        
        try:
            crop_config = get_crop_config(crop)
            phases_config = crop_config["phases"]
            phase_weights = get_crop_phase_weights(crop, zone)
            
            phase_details = []
            
            for phase_idx, (start_day, end_day, trigger_mm, exit_mm, phase_name, water_need_mm, obs_window) in enumerate(phases_config):
                
                # Calculate phase dates
                planting_dt = datetime.strptime(planting_date, '%Y-%m-%d')
                phase_start_date = (planting_dt + timedelta(days=start_day)).strftime('%Y-%m-%d')
                phase_end_date = (planting_dt + timedelta(days=end_day)).strftime('%Y-%m-%d')
                
                phase_duration = end_day - start_day + 1
                
                # Get total rainfall for phase
                total_rainfall = self.rainfall_extractor.get_period_rainfall(
                    geometry, phase_start_date, phase_end_date
                )
                
                # Get rolling window analysis
                rolling_windows = self.rainfall_extractor.get_rolling_window_analysis(
                    geometry, phase_start_date, phase_duration, obs_window
                )
                
                # Calculate payout based on rolling windows
                max_payout_ratio = 0
                triggering_period = None
                
                for window in rolling_windows:
                    rainfall_mm = window['rainfall_mm']
                    
                    # Calculate payout ratio for this window
                    if rainfall_mm <= exit_mm:
                        payout_ratio = 1.0  # Full payout
                    elif rainfall_mm >= trigger_mm:
                        payout_ratio = 0.0  # No payout
                    else:
                        # Linear interpolation between exit and trigger
                        payout_ratio = 1.0 - ((rainfall_mm - exit_mm) / (trigger_mm - exit_mm))
                    
                    if payout_ratio > max_payout_ratio:
                        max_payout_ratio = payout_ratio
                        if payout_ratio > 0:
                            triggering_period = {
                                "period": window['period'],
                                "start_date": window['start_date'],
                                "end_date": window['end_date'],
                                "rainfall_mm": rainfall_mm,
                                "payout_ratio": round(payout_ratio, 4)
                            }
                
                # Calculate water deficit
                water_deficit = max(0, water_need_mm - total_rainfall)
                deficit_percentage = (water_deficit / water_need_mm * 100) if water_need_mm > 0 else 0
                
                phase_detail = {
                    "phase_name": phase_name,
                    "phase_number": phase_idx + 1,
                    "start_day": start_day,
                    "end_day": end_day,
                    "start_date": phase_start_date,
                    "end_date": phase_end_date,
                    "duration_days": phase_duration,
                    "rainfall_received_mm": round(total_rainfall, 1),
                    "water_need_mm": water_need_mm,
                    "deficit_mm": round(water_deficit, 1),
                    "water_deficit_percentage": round(deficit_percentage, 1),
                    "trigger_mm": trigger_mm,
                    "exit_mm": exit_mm,
                    "observation_window_days": obs_window,
                    "payout_ratio": round(max_payout_ratio, 4),
                    "phase_weight": round(phase_weights[phase_idx], 4),
                    "triggering_period": triggering_period,
                    "rolling_windows": rolling_windows[:5] if rolling_windows else []  # First 5 windows for summary
                }
                
                phase_details.append(phase_detail)
            
            return phase_details
            
        except Exception as e:
            print(f"Error calculating phase payouts: {e}")
            return []
    
    def _calculate_quote_financials(self, phase_details: List[Dict], expected_yield: float, 
                                   price_per_ton: float, area_ha: Optional[float],
                                   loadings: Dict[str, float], deductible_rate: float,
                                   deductible_threshold: float) -> Dict[str, Any]:
        """Calculate financial metrics for quote"""
        
        # Calculate sum insured
        if area_ha:
            sum_insured = expected_yield * price_per_ton * area_ha
        else:
            sum_insured = expected_yield * price_per_ton
        
        # Calculate total payout
        total_payout_ratio = 0
        total_payout_usd = 0
        
        for phase in phase_details:
            phase_payout_ratio = phase['payout_ratio']
            phase_weight = phase['phase_weight']
            
            # Calculate USD payout for this phase
            phase_payout_usd = sum_insured * phase_weight * phase_payout_ratio
            phase['payout_usd'] = round(phase_payout_usd, 2)
            
            # Add to totals
            total_payout_ratio += phase_weight * phase_payout_ratio
            total_payout_usd += phase_payout_usd
        
        # Check deductible threshold
        threshold_triggered = total_payout_ratio < deductible_threshold
        threshold_note = None
        
        if threshold_triggered:
            original_total = total_payout_usd
            total_payout_usd = 0
            for phase in phase_details:
                phase['payout_usd'] = 0.0
            threshold_note = f"No payout due to {deductible_threshold * 100:.1f}% threshold (payout index: {total_payout_ratio * 100:.2f}%)"
        
        # Calculate burning cost (expected payout)
        burning_cost = sum_insured * total_payout_ratio
        
        # Apply loadings
        loading_amounts = {}
        total_loadings = 0
        
        for loading_name, loading_rate in loadings.items():
            loading_amount = burning_cost * loading_rate
            loading_amounts[loading_name] = round(loading_amount, 2)
            total_loadings += loading_amount
        
        # Calculate premium
        gross_premium = burning_cost + total_loadings
        premium_rate = (gross_premium / sum_insured) if sum_insured > 0 else 0
        
        # Apply standard deductible
        deductible_amount = sum_insured * deductible_rate
        net_payout_usd = max(0, total_payout_usd - deductible_amount) if not threshold_triggered else 0
        
        result = {
            "sum_insured": round(sum_insured, 2),
            "burning_cost": round(burning_cost, 2),
            "loadings": loading_amounts,
            "gross_premium": round(gross_premium, 2),
            "premium_rate": round(premium_rate, 4),
            "total_payout_ratio": round(total_payout_ratio, 4),
            "total_payout_usd": round(total_payout_usd, 2),
            "deductible_rate": deductible_rate,
            "deductible_amount": round(deductible_amount, 2),
            "deductible_threshold": deductible_threshold,
            "net_payout_usd": round(net_payout_usd, 2),
            "threshold_triggered": threshold_triggered
        }
        
        if threshold_note:
            result["threshold_note"] = threshold_note
        
        return result
    
    def _calculate_prospective_financials(self, phase_details: List[Dict], total_payout_ratio: float,
                                        expected_yield: float, price_per_ton: float, area_ha: Optional[float],
                                        loadings: Dict[str, float], deductible_rate: float,
                                        deductible_threshold: float) -> Dict[str, Any]:
        """Calculate financial metrics for prospective quote"""
        
        # Calculate sum insured
        if area_ha:
            sum_insured = expected_yield * price_per_ton * area_ha
        else:
            sum_insured = expected_yield * price_per_ton
        
        # Calculate expected payouts based on historical averages
        total_expected_payout = 0
        for phase in phase_details:
            phase_expected_payout = sum_insured * phase['phase_weight'] * phase['payout_ratio']
            phase['expected_payout_usd'] = round(phase_expected_payout, 2)
            total_expected_payout += phase_expected_payout
        
        # Use historical average for burning cost
        burning_cost = sum_insured * total_payout_ratio
        
        # Apply loadings
        loading_amounts = {}
        total_loadings = 0
        
        for loading_name, loading_rate in loadings.items():
            loading_amount = burning_cost * loading_rate
            loading_amounts[loading_name] = round(loading_amount, 2)
            total_loadings += loading_amount
        
        # Calculate premium
        gross_premium = burning_cost + total_loadings
        premium_rate = (gross_premium / sum_insured) if sum_insured > 0 else 0
        
        result = {
            "sum_insured": round(sum_insured, 2),
            "burning_cost": round(burning_cost, 2),
            "loadings": loading_amounts,
            "gross_premium": round(gross_premium, 2),
            "premium_rate": round(premium_rate, 4),
            "expected_payout_ratio": round(total_payout_ratio, 4),
            "expected_payout_usd": round(total_expected_payout, 2),
            "deductible_rate": deductible_rate,
            "deductible_threshold": deductible_threshold
        }
        
        return result
    
    def _estimate_planting_window(self, planting_dates: List[str]) -> str:
        """Estimate planting window from historical dates"""
        try:
            if not planting_dates:
                return "November-December"
            
            # Convert to datetime objects
            date_objects = []
            for date_str in planting_dates:
                try:
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                    date_objects.append(date_obj)
                except:
                    continue
            
            if not date_objects:
                return "November-December"
            
            # Calculate median date
            timestamps = [d.timestamp() for d in date_objects]
            median_timestamp = statistics.median(timestamps)
            median_date = datetime.fromtimestamp(median_timestamp)
            
            # Create window around median (+/- 10 days)
            start_date = median_date - timedelta(days=10)
            end_date = median_date + timedelta(days=10)
            
            return f"{start_date.strftime('%b %d')} - {end_date.strftime('%b %d')}"
        
        except Exception as e:
            print(f"Error estimating planting window: {e}")
            return "November-December"
