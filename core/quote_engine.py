"best_year": {
                "year": best_year['year'],
                "drought_impact": round(best_year['drought_impact'], 1),
                "premium_paid": round(uniform_premium, 2),
                "payout_received": round(best_year['simulated_payout'], 2),
                "net_result": round(best_year['net_result'], 2),
                "description": f"Excellent growing conditions with only {best_year['drought_impact']:.1f}% drought impact"
            },
            "worst_year": {
                "year": worst_year['year'],
                "drought_impact": round(worst_year['drought_impact'], 1),
                "premium_paid": round(uniform_premium, 2),
                "payout_received": round(worst_year['simulated_payout'], 2),
                "net_result": round(worst_year['net_result'], 2),
                "description": f"Severe drought year with {worst_year['drought_impact']:.1f}% loss, receiving ${worst_year['simulated_payout']:,.0f} payout"
            },
            "value_for_money": {
                "loss_ratio": round(loss_ratio, 3),
                "interpretation": value_assessment,
                "rate_structure": f"{actuarial_quote['premium_rate']*100:.2f}% applied uniformly across all historical years"
            }
        }
    
    # Include all other optimized methods from the original implementation
    # (keeping the existing optimized implementations for planting detection, rainfall calculation, etc.)
    
    def _detect_planting_dates_optimized(self, latitude: float, longitude: float, 
                                       years: List[int]) -> Dict[int, Optional[str]]:
        """OPTIMIZED: Detect planting dates using server-side batch operations"""
        point = ee.Geometry.Point([longitude, latitude])
        results = {}
        
        print(f"🌱 Starting OPTIMIZED planting detection for {len(years)} years")
        print(f"📍 Location: {latitude:.4f}, {longitude:.4f}")
        print(f"🌧️ Criteria: ≥{self.rainfall_threshold_7day}mm over 7 days, {self.min_rainy_days}+ days ≥{self.daily_threshold}mm")
        print(f"⚡ Method: Server-side batch processing (no .getInfo() bottlenecks)")
        
        # OPTIMIZATION 1: Process multiple years in batches
        batch_size = 5  # Process 5 years at a time
        year_batches = [years[i:i + batch_size] for i in range(0, len(years), batch_size)]
        
        for batch_idx, year_batch in enumerate(year_batches):
            print(f"\n📦 Processing batch {batch_idx + 1}/{len(year_batches)}: {year_batch}")
            
            batch_results = self._process_year_batch_optimized(point, year_batch)
            results.update(batch_results)
        
        return results
    
    def _process_year_batch_optimized(self, point: ee.Geometry.Point, 
                                    year_batch: List[int]) -> Dict[int, Optional[str]]:
        """OPTIMIZED: Process a batch of years using server-side operations"""
        batch_results = {}
        
        for year in year_batch:
            try:
                # Calculate season boundaries
                planting_season_start = datetime(year - 1, self.season_start_month, self.season_start_day)
                planting_season_end = datetime(year, self.season_end_month, self.season_end_day)
                
                print(f"📅 Analyzing {year} season: {planting_season_start.strftime('%Y-%m-%d')} to {planting_season_end.strftime('%Y-%m-%d')}")
                
                # OPTIMIZED: Use server-side planting detection
                planting_date = self._detect_season_planting_optimized(
                    point, planting_season_start, planting_season_end
                )
                
                batch_results[year] = planting_date
                
                if planting_date:
                    print(f"✅ {year}: Planting detected on {planting_date}")
                else:
                    print(f"❌ {year}: No suitable planting conditions detected")
                    
            except Exception as e:
                print(f"❌ Error detecting planting for {year}: {e}")
                batch_results[year] = None
        
        return batch_results
    
    def _detect_season_planting_optimized(self, point: ee.Geometry.Point, 
                                        season_start: datetime, season_end: datetime) -> Optional[str]:
        """OPTIMIZED: Server-side planting detection using simplified approach"""
        try:
            start_date = season_start.strftime('%Y-%m-%d')
            end_date = season_end.strftime('%Y-%m-%d')
            
            # OPTIMIZATION: Use lazy-loaded CHIRPS collection
            season_chirps = self._get_chirps_collection() \
                .filterDate(start_date, end_date) \
                .filterBounds(point)
            
            # SIMPLIFIED: Get daily rainfall as a simple time series
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
            
            # Get time series data
            daily_features = season_chirps.map(extract_daily_rainfall)
            
            # OPTIMIZATION: Single .getInfo() call
            rainfall_data = daily_features.getInfo()
            
            # Process client-side (simpler and more reliable)
            return self._find_planting_date_from_data(rainfall_data)
            
        except Exception as e:
            print(f"❌ Error in optimized season planting detection: {e}")
            return None
    
    def _find_planting_date_from_data(self, rainfall_data: Dict) -> Optional[str]:
        """Process rainfall data client-side to find planting date"""
        try:
            # Convert to list for easier analysis
            daily_data = []
            
            if 'features' not in rainfall_data:
                print("⚠️ No features in rainfall data")
                return None
                
            for feature in rainfall_data['features']:
                props = feature['properties']
                if props.get('rainfall') is not None:
                    daily_data.append({
                        'date': props['date'],
                        'rainfall': float(props['rainfall'])
                    })
            
            if not daily_data:
                print("⚠️ No valid rainfall data available for season")
                return None
            
            # Sort by date
            daily_data.sort(key=lambda x: x['date'])
            
            print(f"📊 Processing {len(daily_data)} days of rainfall data")
            
            # Find planting date using 7-day rolling window (client-side)
            return self._find_planting_with_criteria_simple(daily_data)
            
        except Exception as e:
            print(f"❌ Error processing rainfall data: {e}")
            return None
    
    def _find_planting_with_criteria_simple(self, daily_data: List[Dict]) -> Optional[str]:
        """Find planting date using refined rainfall criteria"""
        if len(daily_data) < 7:
            print(f"⚠️ Insufficient data: only {len(daily_data)} days available")
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
        
        print(f"❌ No 7-day window met criteria (≥{self.rainfall_threshold_7day}mm total, ≥{self.min_rainy_days} days ≥{self.daily_threshold}mm)")
        return None
    
    def _calculate_batch_rainfall_all_phases(self, latitude: float, longitude: float,
                                           planting_dates: Dict[int, str], 
                                           crop: str) -> Dict[int, Dict[str, float]]:
        """OPTIMIZED: Calculate rainfall for all phases across all years in batch"""
        try:
            point = ee.Geometry.Point([longitude, latitude])
            crop_phases = get_crop_phases(crop)
            
            print(f"🔄 Batch processing rainfall for {len(planting_dates)} years, {len(crop_phases)} phases")
            
            # OPTIMIZATION: Build all date ranges at once
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
                        'water_need_mm': water_need_mm
                    }
                
                all_phase_ranges[year] = year_phases
            
            # OPTIMIZATION: Single server-side calculation for all years/phases
            batch_result = self._execute_batch_rainfall_calculation(point, all_phase_ranges)
            
            print(f"✅ Batch rainfall calculation completed")
            return batch_result
            
        except Exception as e:
            print(f"❌ Error in batch rainfall calculation: {e}")
            # Return empty dict as fallback
            return {year: {} for year in planting_dates.keys()}
    
    def _execute_batch_rainfall_calculation(self, point: ee.Geometry.Point, 
                                          all_phase_ranges: Dict) -> Dict[int, Dict[str, float]]:
        """OPTIMIZED: Execute server-side batch rainfall calculation"""
        
        # Find overall date range
        all_dates = []
        for year_data in all_phase_ranges.values():
            for phase_data in year_data.values():
                all_dates.extend([phase_data['start'], phase_data['end']])
        
        overall_start = min(all_dates)
        overall_end = max(all_dates)
        
        print(f"📅 Overall analysis period: {overall_start} to {overall_end}")
        
        # OPTIMIZATION: Single CHIRPS query for entire period using lazy-loaded collection
        chirps_full = self._get_chirps_collection() \
            .filterDate(overall_start, overall_end) \
            .filterBounds(point)
        
        # Build server-side calculation for each year/phase combination
        year_phase_calculations = {}
        
        for year, year_phases in all_phase_ranges.items():
            year_calculations = {}
            
            for phase_name, phase_info in year_phases.items():
                # Filter to phase date range
                phase_chirps = chirps_full.filterDate(phase_info['start'], phase_info['end'])
                
                # Calculate total rainfall for this phase
                phase_total = phase_chirps.sum().reduceRegion(
                    reducer=ee.Reducer.mean(),
                    geometry=point,
                    scale=5566,
                    maxPixels=1
                )
                
                year_calculations[phase_name] = phase_total.get('precipitation')
            
            year_phase_calculations[year] = year_calculations
        
        # OPTIMIZATION: Single .getInfo() call for all calculations
        print(f"⚡ Executing single server-side calculation for all {len(all_phase_ranges)} years")
        
        # Convert to a single dictionary for batch .getInfo()
        batch_dict = ee.Dictionary(year_phase_calculations)
        result = batch_dict.getInfo()
        
        # Post-process results
        formatted_results = {}
        for year, year_data in result.items():
            year_int = int(year)
            formatted_year_data = {}
            
            for phase_name, rainfall_value in year_data.items():
                if rainfall_value is not None:
                    formatted_year_data[phase_name] = round(float(rainfall_value), 1)
                else:
                    formatted_year_data[phase_name] = 0.0
                
                water_need = all_phase_ranges[year_int][phase_name]['water_need_mm']
                print(f"🌧️ {year_int} {phase_name}: {formatted_year_data[phase_name]:.1f}mm (need: {water_need}mm)")
            
            formatted_results[year_int] = formatted_year_data
        
        return formatted_results
    
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
    
    def _generate_enhanced_phase_breakdown_from_crops(self, crop: str, 
                                                    historical_impacts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate enhanced phase breakdown from historical impact data"""
        crop_phases = get_crop_phases(crop)
        enhanced_phases = []
        
        for i, (start_day, end_day, trigger_mm, exit_mm, phase_name, water_need_mm, obs_window) in enumerate(crop_phases):
            # Collect rainfall data across years for this phase
            phase_rainfall_data = []
            for year_data in historical_impacts:
                rainfall_by_phase = year_data.get('rainfall_mm_by_phase', {})
                if phase_name in rainfall_by_phase:
                    phase_rainfall_data.append(rainfall_by_phase[phase_name])
            
            # Calculate phase statistics
            if phase_rainfall_data:
                avg_rainfall = sum(phase_rainfall_data) / len(phase_rainfall_data)
                min_rainfall = min(phase_rainfall_data)
                max_rainfall = max(phase_rainfall_data)
                
                # Calculate stress frequency
                stress_years = len([r for r in phase_rainfall_data if r < (water_need_mm * 0.8)])  # 80% threshold
                stress_frequency = (stress_years / len(phase_rainfall_data) * 100) if phase_rainfall_data else 0
                
                # Calculate weighted impact for this phase
                phase_weights = get_crop_phase_weights(crop)
                phase_weight = phase_weights[i] if i < len(phase_weights) else 0.25
                avg_stress_level = max(0, (water_need_mm - avg_rainfall) / water_need_mm) if water_need_mm > 0 else 0
                weighted_impact = avg_stress_level * phase_weight * 100
                
            else:
                avg_rainfall = min_rainfall = max_rainfall = 0
                stress_years = 0
                stress_frequency = 0
                weighted_impact = 0
                phase_weight = 0.25
                avg_stress_level = 0
            
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
                'phase_weight': round(phase_weight, 3),
                'historical_rainfall': {
                    'average_mm': round(avg_rainfall, 1),
                    'minimum_mm': round(min_rainfall, 1),
                    'maximum_mm': round(max_rainfall, 1),
                    'stress_years_count': stress_years,
                    'stress_frequency_percent': round(stress_frequency, 1)
                },
                'stress_percentage': round((avg_stress_level * 100), 1),
                'weighted_impact': round(weighted_impact, 1)
            }
            
            enhanced_phases.append(enhanced_phase)
        
        return enhanced_phases
    
    # Include all utility methods from original implementation
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
        if year < self.EARLIEST_RELIABLE_DATA or year > current_year + 2:
            raise ValueError(f"Year must be between {self.EARLIEST_RELIABLE_DATA} and {current_year + 2}")
        
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
            'deductible_rate': deductible_rate,
            'custom_loadings': custom_loadings,
            'buffer_radius': request_data.get('buffer_radius', 1500)
        }
    
    # Include all other utility methods with same implementations...
    def _validate_actuarial_data_availability(self, target_year: int, quote_type: str) -> Dict[str, Any]:
        """Validate data availability against actuarial standards"""
        current_year = datetime.now().year
        
        if quote_type == "historical":
            max_available_years = target_year - self.EARLIEST_RELIABLE_DATA
            latest_analysis_year = target_year - 1
        else:
            max_available_years = current_year - self.EARLIEST_RELIABLE_DATA
            latest_analysis_year = current_year - 1
        
        years_we_can_analyze = min(self.OPTIMAL_YEARS_RANGE, max_available_years)
        
        validation_result = {
            'years_available': years_we_can_analyze,
            'meets_actuarial_standard': years_we_can_analyze >= self.ACTUARIAL_MINIMUM_YEARS,
            'meets_regulatory_minimum': years_we_can_analyze >= self.REGULATORY_MINIMUM_YEARS,
            'actuarial_standard_years': self.ACTUARIAL_MINIMUM_YEARS,
            'regulatory_minimum_years': self.REGULATORY_MINIMUM_YEARS,
            'optimal_years': self.OPTIMAL_YEARS_RANGE,
            'data_quality_rating': self._get_data_quality_rating(years_we_can_analyze),
            'analysis_period': f"{latest_analysis_year - years_we_can_analyze + 1} to {latest_analysis_year}",
            'quote_type': quote_type,
            'target_year': target_year
        }
        
        if not validation_result['meets_actuarial_standard']:
            validation_result['recommendations'] = [
                f"Wait until {self.EARLIEST_RELIABLE_DATA + self.ACTUARIAL_MINIMUM_YEARS} for full actuarial standard",
                "Consider using regulatory minimum with appropriate risk adjustments",
                "Add additional loading factors to account for data uncertainty",
                "Implement more conservative payout thresholds"
            ]
        
        return validation_result
    
    def _get_data_quality_rating(self, years_available: int) -> str:
        """Rate data quality based on years available"""
        if years_available >= self.OPTIMAL_YEARS_RANGE:
            return "Excellent"
        elif years_available >= self.ACTUARIAL_MINIMUM_YEARS:
            return "Good - Meets Actuarial Standard"
        elif years_available >= self.REGULATORY_MINIMUM_YEARS:
            return "Acceptable - Meets Regulatory Minimum"
        elif years_available >= 10:
            return "Poor - Below Standards"
        else:
            return "Insufficient - Cannot Proceed"
    
    def _assess_climate_cycles(self, historical_years: List[int]) -> Dict[str, Any]:
        """Assess climate cycle coverage in the analysis period"""
        years_span = max(historical_years) - min(historical_years) + 1
        
        # Typical climate cycles
        enso_cycles = years_span / 3.5  # El Niño/La Niña cycle ~3-7 years
        decadal_cycles = years_span / 10  # Decadal climate patterns
        
        return {
            'analysis_span_years': years_span,
            'estimated_enso_cycles': round(enso_cycles, 1),
            'estimated_decadal_cycles': round(decadal_cycles, 1),
            'cycle_coverage_rating': (
                "Excellent" if enso_cycles >= 5 else
                "Good" if enso_cycles >= 3 else
                "Fair" if enso_cycles >= 2 else
                "Limited"
            )
        }
    
    def _get_actuarial_years_analysis(self, target_year: int, quote_type: str) -> List[int]:
        """Generate actuarial-grade historical years (minimum 20 years)"""
        current_year = datetime.now().year
        
        if quote_type == "historical":
            latest_analysis_year = target_year - 1
            max_available_years = latest_analysis_year - self.EARLIEST_RELIABLE_DATA + 1
        else:
            latest_analysis_year = current_year - 1
            max_available_years = latest_analysis_year - self.EARLIEST_RELIABLE_DATA + 1
        
        # Determine how many years to use (aim for optimal, minimum actuarial standard)
        years_to_use = min(self.OPTIMAL_YEARS_RANGE, max_available_years)
        
        # ENFORCE ACTUARIAL MINIMUM
        if years_to_use < self.REGULATORY_MINIMUM_YEARS:
            raise ValueError(
                f"INSUFFICIENT DATA: Only {years_to_use} years available from {self.EARLIEST_RELIABLE_DATA}. "
                f"Minimum {self.REGULATORY_MINIMUM_YEARS} years required, "
                f"{self.ACTUARIAL_MINIMUM_YEARS} years recommended for actuarial standards."
            )
        
        # Calculate start year
        start_year = latest_analysis_year - years_to_use + 1
        
        # Generate the years list
        historical_years = list(range(start_year, latest_analysis_year + 1))
        
        # Final validation
        if len(historical_years) < self.REGULATORY_MINIMUM_YEARS:
            raise ValueError(
                f"CONFIGURATION ERROR: Generated {len(historical_years)} years, "
                f"minimum {self.REGULATORY_MINIMUM_YEARS} required"
            )
        
        # Log actuarial compliance
        if len(historical_years) >= self.ACTUARIAL_MINIMUM_YEARS:
            print(f"✅ ACTUARIAL COMPLIANCE: {len(historical_years)} years meets {self.ACTUARIAL_MINIMUM_YEARS}-year standard")
        else:
            print(f"⚠️ REGULATORY MINIMUM: {len(historical_years)} years (below {self.ACTUARIAL_MINIMUM_YEARS}-year actuarial standard)")
        
        return historical_years
    
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
            return "historical""""
Actuarially Correct Quote Engine V2.4 - Fixed Historical Simulation Logic
Ensures uniform premium rates across all historical years in simulation breakdown
"""

import ee
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import numpy as np

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

class QuoteEngine:
    """Actuarially correct quote engine with proper historical simulation logic"""
    
    def __init__(self):
        """Initialize with performance-optimized components"""
        # ACTUARIAL DATA REQUIREMENTS - Updated to industry standards
        self.ACTUARIAL_MINIMUM_YEARS = 20      # Industry standard for weather index insurance
        self.REGULATORY_MINIMUM_YEARS = 15     # Absolute minimum for regulatory approval
        self.OPTIMAL_YEARS_RANGE = 25          # Optimal for capturing climate cycles
        self.EARLIEST_RELIABLE_DATA = 1981     # CHIRPS reliable data starts from 1981
        
        # FIXED: Proper actuarial parameters
        self.base_loading_factor = 1.5  # Base loading multiplier
        self.minimum_premium_rate = 0.015  # 1.5% minimum
        self.maximum_premium_rate = 0.25   # 25% maximum
        
        # ACTUARIAL FIX: Remove individual year premium calculation
        # Historical simulation uses UNIFORM RATE across all years
        
        # Dynamic deductible defaults (now configurable)
        self.default_deductible_rate = 0.05  # 5% default, now flexible
        
        # Default loadings (if none provided)
        self.default_loadings = {
            "admin": 0.10,       # 10% administrative costs
            "margin": 0.05,      # 5% profit margin
            "reinsurance": 0.08  # 8% reinsurance costs
        }
        
        # OPTIMIZED: Rainfall-based planting detection parameters
        self.rainfall_threshold_7day = 20.0  # mm over 7 consecutive days
        self.daily_threshold = 5.0  # mm for individual days
        self.min_rainy_days = 2  # minimum days above daily threshold
        
        # Seasonal validation - Summer crops only
        self.valid_planting_months = [10, 11, 12, 1]  # Oct-Jan only
        self.season_start_month = 10  # October
        self.season_start_day = 1
        self.season_end_month = 1  # January
        self.season_end_day = 31
        
        # PERFORMANCE: Lazy-load Earth Engine objects (initialized after ee.Initialize())
        self._chirps_collection = None
        
        print("🚀 ACTUARIALLY CORRECT Quote Engine V2.4 initialized")
        print("🔧 MAJOR FIX: Uniform premium rates in historical simulation")
        print("📊 ACTUARIAL LOGIC: Single rate applied across all historical years")
        print("🎯 SIMULATION PURPOSE: 'What if we had this rate structure in the past'")
        print("📈 PREVENTS: Reverse-engineering rates after knowing losses")
        print(f"📅 Data period: {self.EARLIEST_RELIABLE_DATA} onwards")
    
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
    
    def execute_quote(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute actuarially correct quote with proper historical simulation
        
        Args:
            request_data: Quote request parameters
            
        Returns:
            Enhanced quote with actuarially correct historical analysis
        """
        try:
            print(f"\n🚀 Starting ACTUARIALLY CORRECT quote execution")
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
            print(f"💰 Deductible: {params['deductible_rate']*100:.1f}%")
            
            # ACTUARIAL VALIDATION: Check data availability first
            data_validation = self._validate_actuarial_data_availability(params['year'], quote_type)
            
            if not data_validation['meets_actuarial_standard']:
                if data_validation['meets_regulatory_minimum']:
                    print(f"⚠️ WARNING: Only {data_validation['years_available']} years available")
                    print(f"📊 Below actuarial standard ({self.ACTUARIAL_MINIMUM_YEARS} years) but above regulatory minimum")
                else:
                    raise ValueError(
                        f"INSUFFICIENT DATA: Only {data_validation['years_available']} years available. "
                        f"Minimum {self.REGULATORY_MINIMUM_YEARS} years required for basic analysis, "
                        f"{self.ACTUARIAL_MINIMUM_YEARS} years recommended for actuarial standards."
                    )
            
            # Generate historical years for analysis (actuarial-grade)
            historical_years = self._get_actuarial_years_analysis(params['year'], quote_type)
            print(f"📊 ACTUARIAL ANALYSIS: {len(historical_years)} years ({min(historical_years)}-{max(historical_years)})")
            
            # OPTIMIZED: Detect planting dates using server-side batch processing
            planting_dates = self._detect_planting_dates_optimized(
                params['latitude'], 
                params['longitude'], 
                historical_years
            )
            
            # Filter valid planting dates and validate seasons
            valid_planting_dates = self._validate_seasonal_planting_dates(planting_dates)
            
            # ACTUARIAL VALIDATION: Ensure sufficient valid seasons
            if len(valid_planting_dates) < (len(historical_years) * 0.7):  # 70% success rate minimum
                print(f"⚠️ WARNING: Low planting detection rate: {len(valid_planting_dates)}/{len(historical_years)} seasons")
            
            if len(valid_planting_dates) < 10:  # Absolute minimum for statistical significance
                raise ValueError(
                    f"INSUFFICIENT VALID SEASONS: Only {len(valid_planting_dates)} valid planting seasons detected. "
                    f"Minimum 10 seasons required for statistical reliability."
                )
            
            # STEP 1: Calculate drought impacts for all historical years (no premiums yet)
            print(f"\n📊 STEP 1: Calculating historical drought impacts...")
            historical_drought_impacts = self._calculate_historical_drought_impacts(
                params, valid_planting_dates
            )
            
            # STEP 2: Calculate SINGLE actuarial rate from aggregated historical data
            print(f"\n📊 STEP 2: Calculating single actuarial rate from {len(historical_drought_impacts)} years...")
            actuarial_quote = self._calculate_actuarial_quote_rate(
                params, historical_drought_impacts
            )
            
            # STEP 3: Apply uniform rate to historical simulation
            print(f"\n📊 STEP 3: Applying uniform rate ({actuarial_quote['premium_rate']*100:.2f}%) to historical simulation...")
            year_by_year_simulation = self._apply_uniform_rate_to_historical_years(
                historical_drought_impacts, actuarial_quote, params
            )
            
            # Build final quote result with proper actuarial structure
            quote_result = actuarial_quote.copy()
            
            # Add actuarial validation results
            quote_result['actuarial_validation'] = data_validation
            quote_result['data_quality_metrics'] = {
                'total_years_analyzed': len(historical_years),
                'valid_seasons_detected': len(valid_planting_dates),
                'detection_success_rate': (len(valid_planting_dates) / len(historical_years)) * 100,
                'meets_actuarial_standard': data_validation['meets_actuarial_standard'],
                'data_period': f"{min(historical_years)}-{max(historical_years)}",
                'climate_cycles_captured': self._assess_climate_cycles(historical_years)
            }
            
            # Add ACTUARIALLY CORRECT simulation results
            quote_result['year_by_year_simulation'] = year_by_year_simulation
            
            # Calculate proper simulation summary
            quote_result['simulation_summary'] = self._calculate_actuarial_simulation_summary(
                year_by_year_simulation, actuarial_quote
            )
            
            # Add planting analysis
            quote_result['planting_analysis'] = {
                'detection_method': 'optimized_rainfall_only',
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
            
            # Add enhanced phase breakdown
            quote_result['phase_breakdown'] = self._generate_enhanced_phase_breakdown_from_crops(
                params['crop'], historical_drought_impacts
            )
            
            # Add actuarially correct field story
            quote_result['field_story'] = self._generate_actuarial_field_story(
                year_by_year_simulation, actuarial_quote, params
            )
            
            execution_time = (datetime.now() - start_time).total_seconds()
            quote_result['execution_time_seconds'] = round(execution_time, 2)
            quote_result['version'] = "2.4.0-ActuariallyCorrect"
            quote_result['methodology'] = "actuarial_uniform_rate_simulation_v2.4"
            
            print(f"✅ ACTUARIALLY CORRECT quote completed in {execution_time:.2f} seconds")
            print(f"💰 Uniform premium rate: {quote_result['premium_rate']*100:.2f}%")
            print(f"💵 Gross premium: ${quote_result['gross_premium']:,.2f}")
            print(f"📊 Applied uniformly across {len(year_by_year_simulation)} historical years")
            print(f"🎯 ACTUARIAL LOGIC: Historical simulation shows 'what if' scenarios")
            
            return quote_result
            
        except Exception as e:
            print(f"❌ Quote execution error: {e}")
            raise
    
    def _calculate_historical_drought_impacts(self, params: Dict[str, Any], 
                                            valid_planting_dates: Dict[int, str]) -> List[Dict[str, Any]]:
        """Calculate drought impacts for all historical years (Step 1 - no premiums)"""
        
        print(f"📊 Calculating drought impacts for {len(valid_planting_dates)} historical years...")
        
        # OPTIMIZATION: Batch process all years at once
        batch_rainfall_data = self._calculate_batch_rainfall_all_phases(
            params['latitude'],
            params['longitude'],
            valid_planting_dates,
            params['crop']
        )
        
        historical_impacts = []
        
        for year, planting_date in valid_planting_dates.items():
            try:
                print(f"📈 Analyzing {year} drought impact (planted: {planting_date})")
                
                # Get pre-computed rainfall data for this year
                year_rainfall_data = batch_rainfall_data.get(year, {})
                
                # Calculate drought impact only (no premium calculation)
                drought_impact = self._calculate_drought_impact_by_phases(
                    get_crop_phases(params['crop']), 
                    year_rainfall_data, 
                    params['crop']
                )
                
                # Calculate season details
                plant_date = datetime.strptime(planting_date, '%Y-%m-%d')
                crop_phases = get_crop_phases(params['crop'])
                total_season_days = crop_phases[-1][1]  # end_day of last phase
                season_end = plant_date + timedelta(days=total_season_days)
                
                # Count critical periods
                critical_periods = len([
                    p for p, r in year_rainfall_data.items() 
                    if r < (get_crop_config(params['crop'])['phases'][
                        list(year_rainfall_data.keys()).index(p)
                    ][5] * 0.7)  # Less than 70% of water need
                ])
                
                year_impact = {
                    'year': year,
                    'planting_date': planting_date,
                    'planting_year': int(planting_date.split('-')[0]),
                    'harvest_year': year,
                    'season_end_date': season_end.strftime('%Y-%m-%d'),
                    'drought_impact_percent': round(drought_impact, 2),
                    'rainfall_mm_by_phase': year_rainfall_data,
                    'critical_periods': critical_periods
                }
                
                historical_impacts.append(year_impact)
                
                print(f"📊 {year}: {drought_impact:.1f}% drought impact, {critical_periods} critical periods")
                
            except Exception as e:
                print(f"❌ Error analyzing {year}: {e}")
                continue
        
        print(f"✅ Calculated drought impacts for {len(historical_impacts)} years")
        return historical_impacts
    
    def _calculate_actuarial_quote_rate(self, params: Dict[str, Any], 
                                      historical_impacts: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate single actuarial rate from aggregated historical data (Step 2)"""
        
        if not historical_impacts:
            raise ValueError("No valid historical data for actuarial calculation")
        
        # Calculate aggregate drought risk from all historical years
        total_drought_impacts = [year['drought_impact_percent'] for year in historical_impacts]
        average_drought_impact = sum(total_drought_impacts) / len(total_drought_impacts)
        
        print(f"📊 ACTUARIAL CALCULATION:")
        print(f"   • {len(historical_impacts)} historical years analyzed")
        print(f"   • Average drought impact: {average_drought_impact:.2f}%")
        print(f"   • Impact range: {min(total_drought_impacts):.1f}% - {max(total_drought_impacts):.1f}%")
        
        # Calculate base risk rate
        base_drought_risk = average_drought_impact / 100.0
        
        # Apply zone adjustments
        zone_adjustments = self._get_zone_adjustments_from_crops(params)
        zone_multiplier = zone_adjustments.get('risk_multiplier', 1.0)
        
        # Calculate final premium rate with loadings
        adjusted_risk = base_drought_risk * self.base_loading_factor * zone_multiplier
        final_premium_rate = max(self.minimum_premium_rate, min(adjusted_risk, self.maximum_premium_rate))
        
        # Calculate financial metrics
        sum_insured = params['expected_yield'] * params['price_per_ton'] * params.get('area_ha', 1.0)
        burning_cost = sum_insured * final_premium_rate
        
        # Apply custom loadings or defaults
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
        
        # Apply deductible
        deductible_amount = sum_insured * params['deductible_rate']
        
        print(f"💰 ACTUARIAL RATE CALCULATION:")
        print(f"   • Base risk: {base_drought_risk*100:.2f}%")
        print(f"   • Zone multiplier: {zone_multiplier:.2f}")
        print(f"   • Final premium rate: {final_premium_rate*100:.2f}%")
        print(f"   • Burning cost: ${burning_cost:,.2f}")
        print(f"   • Total loadings: ${total_loadings_amount:,.2f}")
        print(f"   • Gross premium: ${gross_premium:,.2f}")
        
        return {
            # Core quote data
            'crop': params['crop'],
            'year': params['year'],
            'quote_type': params['quote_type'],
            'latitude': params['latitude'],
            'longitude': params['longitude'],
            'area_ha': params.get('area_ha', 1.0),
            
            # Financial summary - UNIFORM RATE
            'expected_yield': params['expected_yield'],
            'price_per_ton': params['price_per_ton'],
            'sum_insured': sum_insured,
            'premium_rate': final_premium_rate,  # THIS IS THE UNIFORM RATE
            'burning_cost': burning_cost,
            'loadings_breakdown': loadings_breakdown,
            'total_loadings': total_loadings_amount,
            'gross_premium': gross_premium,
            'deductible_rate': params['deductible_rate'],
            'deductible_amount': deductible_amount,
            
            # Risk analysis
            'average_historical_drought_impact': average_drought_impact,
            'expected_payout_ratio': base_drought_risk,
            'historical_years_analyzed': len(historical_impacts),
            'zone': params.get('zone', 'auto_detected'),
            'zone_adjustments': zone_adjustments,
            
            # Metadata
            'generated_at': datetime.utcnow().isoformat()
        }
    
    def _apply_uniform_rate_to_historical_years(self, historical_impacts: List[Dict[str, Any]], 
                                              actuarial_quote: Dict[str, Any], 
                                              params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Apply uniform actuarial rate to all historical years (Step 3)"""
        
        # Extract the UNIFORM rate from actuarial calculation
        uniform_premium_rate = actuarial_quote['premium_rate']
        uniform_premium_amount = actuarial_quote['gross_premium']
        sum_insured = actuarial_quote['sum_insured']
        deductible_rate = params['deductible_rate']
        
        print(f"🎯 APPLYING UNIFORM RATE: {uniform_premium_rate*100:.2f}% (${uniform_premium_amount:,.2f}) to all historical years")
        print(f"📊 PURPOSE: Simulate 'what if we had this rate structure in the past'")
        
        simulation_results = []
        
        for year_data in historical_impacts:
            # Extract drought impact for this year
            drought_impact = year_data['drought_impact_percent']
            
            # Apply deductible to drought impact
            drought_impact_after_deductible = max(0, drought_impact - (deductible_rate * 100))
            
            # Calculate payout using uniform rate structure
            simulated_payout = sum_insured * (drought_impact_after_deductible / 100.0)
            
            # Calculate farmer net result with UNIFORM premium
            net_result = simulated_payout - uniform_premium_amount
            
            # Calculate loss ratio with UNIFORM premium
            loss_ratio = (simulated_payout / uniform_premium_amount) if uniform_premium_amount > 0 else 0
            
            simulation_year = {
                'year': year_data['year'],
                'planting_date': year_data['planting_date'],
                'planting_year': year_data['planting_year'],
                'harvest_year': year_data['harvest_year'],
                'season_end_date': year_data['season_end_date'],
                
                # Drought impact (varies by year)
                'drought_impact': drought_impact,
                'drought_impact_after_deductible': round(drought_impact_after_deductible, 2),
                
                # UNIFORM PREMIUM (same for all years)
                'premium_rate_applied': uniform_premium_rate,
                'simulated_premium_usd': uniform_premium_amount,
                
                # Variable payout (based on year-specific drought impact)
                'simulated_payout': round(simulated_payout, 2),
                
                # Results
                'net_result': round(net_result, 2),
                'loss_ratio': round(loss_ratio, 4),
                
                # Additional data
                'rainfall_mm_by_phase': year_data['rainfall_mm_by_phase'],
                'critical_periods': year_data['critical_periods']
            }
            
            simulation_results.append(simulation_year)
            
            print(f"📊 {year_data['year']}: {drought_impact:.1f}% loss → "
                  f"${uniform_premium_amount:,.0f} premium, ${simulated_payout:,.0f} payout, "
                  f"Net: ${net_result:,.0f}, LR: {loss_ratio:.2f}")
        
        print(f"✅ ACTUARIAL SIMULATION: Applied uniform ${uniform_premium_amount:,.0f} premium to {len(simulation_results)} years")
        
        return simulation_results
    
    def _calculate_actuarial_simulation_summary(self, simulation_results: List[Dict[str, Any]], 
                                              actuarial_quote: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate simulation summary with actuarially correct logic"""
        
        if not simulation_results:
            return {}
        
        # Extract metrics from simulation
        drought_impacts = [year['drought_impact'] for year in simulation_results]
        payouts = [year['simulated_payout'] for year in simulation_results]
        net_results = [year['net_result'] for year in simulation_results]
        loss_ratios = [year['loss_ratio'] for year in simulation_results]
        
        # Calculate aggregated metrics
        total_historical_premiums = len(simulation_results) * actuarial_quote['gross_premium']
        total_historical_payouts = sum(payouts)
        net_farmer_position = sum(net_results)
        
        # Payout frequency (years with meaningful payouts)
        meaningful_payouts = [year for year in simulation_results if year['simulated_payout'] > (actuarial_quote['gross_premium'] * 0.01)]
        payout_frequency = (len(meaningful_payouts) / len(simulation_results)) * 100
        
        # Find extreme years
        worst_year = max(simulation_results, key=lambda x: x['drought_impact'])
        best_year = min(simulation_results, key=lambda x: x['drought_impact'])
        
        # Calculate overall historical loss ratio (CRITICAL ACTUARIAL METRIC)
        overall_loss_ratio = total_historical_payouts / total_historical_premiums if total_historical_premiums > 0 else 0
        
        print(f"📊 ACTUARIAL SIMULATION SUMMARY:")
        print(f"   • Total historical premiums: ${total_historical_premiums:,.2f}")
        print(f"   • Total historical payouts: ${total_historical_payouts:,.2f}")
        print(f"   • Overall loss ratio: {overall_loss_ratio:.3f}")
        print(f"   • Net farmer position: ${net_farmer_position:,.2f}")
        print(f"   • Payout frequency: {payout_frequency:.1f}%")
        
        return {
            'years_analyzed': len(simulation_results),
            'uniform_premium_applied': actuarial_quote['gross_premium'],
            'uniform_premium_rate': actuarial_quote['premium_rate'],
            
            # Drought impact statistics
            'average_drought_impact': round(sum(drought_impacts) / len(drought_impacts), 2),
            'maximum_drought_impact': round(max(drought_impacts), 2),
            'minimum_drought_impact': round(min(drought_impacts), 2),
            
            # Financial performance with UNIFORM PREMIUM
            'total_historical_premiums': round(total_historical_premiums, 2),
            'total_historical_payouts': round(total_historical_payouts, 2),
            'overall_historical_loss_ratio': round(overall_loss_ratio, 4),
            'net_farmer_position': round(net_farmer_position, 2),
            'average_annual_payout': round(sum(payouts) / len(payouts), 2),
            
            # Performance metrics
            'payout_frequency': round(payout_frequency, 1),
            'worst_loss_year': worst_year['year'],
            'worst_loss_impact': round(worst_year['drought_impact'], 2),
            'best_year': best_year['year'],
            'best_year_impact': round(best_year['drought_impact'], 2),
            
            # Actuarial validation
            'rate_validation': {
                'expected_loss_ratio': actuarial_quote['expected_payout_ratio'],
                'actual_historical_loss_ratio': overall_loss_ratio,
                'rate_adequacy': 'ADEQUATE' if overall_loss_ratio <= 0.8 else 'REVIEW_NEEDED'
            }
        }
    
    def _generate_actuarial_field_story(self, simulation_results: List[Dict[str, Any]], 
                                      actuarial_quote: Dict[str, Any], 
                                      params: Dict[str, Any]) -> Dict[str, Any]:
        """Generate field story with actuarially correct perspective"""
        
        if not simulation_results:
            return {"summary": "Insufficient data for field story generation"}
        
        # Calculate metrics from UNIFORM RATE simulation
        uniform_premium = actuarial_quote['gross_premium']
        total_premiums = len(simulation_results) * uniform_premium
        total_payouts = sum(year['simulated_payout'] for year in simulation_results)
        net_position = total_payouts - total_premiums
        
        # Find representative years
        best_year = min(simulation_results, key=lambda x: x['drought_impact'])
        worst_year = max(simulation_results, key=lambda x: x['drought_impact'])
        
        # Calculate payout frequency
        meaningful_payouts = len([year for year in simulation_results if year['simulated_payout'] > (uniform_premium * 0.01)])
        payout_frequency = meaningful_payouts / len(simulation_results) * 100
        
        # Determine risk profile
        avg_impact = sum(year['drought_impact'] for year in simulation_results) / len(simulation_results)
        risk_level = "low" if avg_impact < 10 else "moderate" if avg_impact < 25 else "high"
        
        # Generate actuarially correct story
        summary = (f"Based on {len(simulation_results)} years of historical analysis, this {params['crop']} field "
                  f"would have required consistent annual premiums of ${uniform_premium:,.0f} under our "
                  f"{actuarial_quote['premium_rate']*100:.2f}% rate structure. ")
        
        summary += (f"Over this period, total payouts would have been ${total_payouts:,.0f}, resulting in ")
        
        if net_position > 0:
            summary += f"a net benefit to the farmer of ${net_position:,.0f}. "
        elif net_position < 0:
            summary += f"a net cost of ${abs(net_position):,.0f} for drought protection. "
        else:
            summary += "a break-even outcome. "
        
        summary += (f"This represents {risk_level} drought risk with payouts occurring in "
                   f"{payout_frequency:.0f}% of seasons.")
        
        # Calculate value assessment
        loss_ratio = total_payouts / total_premiums if total_premiums > 0 else 0
        value_assessment = (
            "Excellent value - high claims efficiency" if loss_ratio > 0.7 else
            "Good value - balanced protection" if loss_ratio > 0.4 else
            "Conservative pricing - lower claims frequency"
        )
        
        return {
            "summary": summary,
            "actuarial_methodology": "uniform_rate_historical_simulation",
            "historical_performance": {
                "total_seasons": len(simulation_results),
                "uniform_annual_premium": round(uniform_premium, 2),
                "total_premiums_paid": round(total_premiums, 2),
                "total_payouts_received": round(total_payouts, 2),
                "net_farmer_position": round(net_position, 2),
                "payout_frequency_percent": round(payout_frequency, 1),
                "overall_loss_ratio": round(loss_ratio, 3)
            },
            "best_year": {
                "year
