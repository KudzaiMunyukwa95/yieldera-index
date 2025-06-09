"""
Enhanced AI-powered summary generation with detailed report structure
Matches and exceeds ACRE Africa term sheet detail level
"""

import os
from openai import OpenAI
from typing import Dict, List, Any, Optional
from config import Config

class EnhancedAISummaryGenerator:
    """Enhanced AI summary generator with comprehensive report structure"""
    
    def __init__(self):
        if not Config.OPENAI_API_KEY:
            print("Warning: OpenAI API key not configured. AI summaries will not be available.")
            self.client = None
        else:
            self.client = OpenAI(api_key=Config.OPENAI_API_KEY)
    
    def generate_comprehensive_quote_report(self, quote_data: Dict[str, Any], location_info: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Generate comprehensive quote report exceeding ACRE Africa detail level
        
        Args:
            quote_data: Complete quote data
            location_info: Optional location information
            
        Returns:
            dict: Enhanced report structure with detailed sections
        """
        if not self.client:
            return self._fallback_report_structure(quote_data, location_info)
        
        try:
            # Generate enhanced report sections
            report = {
                "executive_summary": self._generate_executive_summary(quote_data, location_info),
                "coverage_details": self._generate_coverage_details(quote_data),
                "risk_analysis": self._generate_risk_analysis(quote_data),
                "payout_structure": self._generate_payout_structure(quote_data),
                "phase_breakdown": self._enhance_phase_breakdown(quote_data),
                "financial_summary": self._generate_financial_summary(quote_data),
                "technical_specifications": self._generate_technical_specs(quote_data),
                "claims_procedure": self._generate_claims_procedure(quote_data),
                "terms_and_conditions": self._generate_terms_conditions(quote_data)
            }
            
            return report
                
        except Exception as e:
            print(f"Error generating comprehensive report: {e}")
            return self._fallback_report_structure(quote_data, location_info)
    
    def _generate_executive_summary(self, quote_data: Dict[str, Any], location_info: Optional[Dict] = None) -> str:
        """Generate executive summary section"""
        
        crop = quote_data.get('crop', 'maize').title()
        year = quote_data.get('year', 'current')
        sum_insured = quote_data.get('sum_insured', 0)
        gross_premium = quote_data.get('gross_premium', 0)
        premium_rate = quote_data.get('premium_rate', 0) * 100
        quote_type = quote_data.get('quote_type', 'unknown')
        
        # Location context
        location_name = "the insured location"
        if location_info:
            if location_info.get('type') == 'field' and location_info.get('name'):
                location_name = f"{location_info['name']} field"
            elif location_info.get('label'):
                location_name = location_info['label']
        
        # Zone information
        zone_name = quote_data.get('zone_name', 'Standard coverage zone')
        
        prompt = f"""
        Generate a professional executive summary for a {crop} weather index insurance quote.

        **QUOTE OVERVIEW:**
        - Product: Weather Index Insurance (WII) for {crop}
        - Location: {location_name}
        - Coverage Year: {year}
        - Quote Type: {quote_type.title()}
        - Zone: {zone_name}
        - Sum Insured: ${sum_insured:,.2f}
        - Premium Rate: {premium_rate:.1f}%
        - Total Premium: ${gross_premium:,.2f}

        Write a compelling 2-3 paragraph executive summary that:
        1. Positions this as professional agricultural index insurance
        2. Highlights the automatic, transparent payout mechanism
        3. Emphasizes protection during critical growth phases
        4. Mentions satellite-based rainfall monitoring (CHIRPS)
        5. Uses professional insurance terminology
        6. Compares favorably to traditional crop insurance approaches
        
        Make it sound like a premium insurance product suitable for institutional clients.
        """
        
        return self._call_openai(prompt, "executive_summary")
    
    def _generate_coverage_details(self, quote_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate detailed coverage structure"""
        
        crop = quote_data.get('crop', 'maize')
        phases = quote_data.get('phase_breakdown', [])
        zone = quote_data.get('zone', 'auto_detect')
        
        coverage_details = {
            "product_type": "Weather Index Insurance (WII)",
            "crop_insured": crop.title(),
            "coverage_basis": "CHIRPS satellite rainfall measurements monitored daily",
            "geographical_coverage": "1.5km radius buffer around insured coordinates",
            "insured_period": "120 days from planting detection",
            "automatic_start": "Coverage begins when 7-day cumulative rainfall ≥15mm with 2+ days ≥3mm",
            "observation_method": "10-day rolling windows for dry spell detection",
            "phase_structure": []
        }
        
        # Add phase structure details
        for i, phase in enumerate(phases):
            phase_detail = {
                "phase_number": i + 1,
                "phase_name": phase.get('phase_name', f'Phase {i+1}'),
                "coverage_days": f"Day {phase.get('start_day', 0)} to Day {phase.get('end_day', 0)}",
                "duration": f"{phase.get('duration_days', 0)} days",
                "observation_windows": f"{phase.get('observation_window_days', 10)}-day rolling periods",
                "trigger_threshold": f"{phase.get('trigger_mm', 0)}mm",
                "exit_threshold": f"{phase.get('exit_mm', 0)}mm",
                "maximum_payout": f"{phase.get('phase_weight', 0) * 100:.1f}% of sum insured"
            }
            coverage_details["phase_structure"].append(phase_detail)
        
        return coverage_details
    
    def _generate_risk_analysis(self, quote_data: Dict[str, Any]) -> str:
        """Generate risk analysis section"""
        
        historical_years = quote_data.get('historical_years_used', [])
        zone_name = quote_data.get('zone_name', 'Standard zone')
        expected_payout_ratio = quote_data.get('expected_payout_ratio', 0) * 100
        
        prompt = f"""
        Generate a professional risk analysis for agricultural index insurance.

        **RISK PARAMETERS:**
        - Analysis Period: {len(historical_years)} years of historical data
        - Risk Zone: {zone_name}
        - Expected Annual Payout Frequency: {expected_payout_ratio:.1f}%
        - Data Source: CHIRPS satellite rainfall (daily resolution)
        
        Write a comprehensive risk analysis covering:
        1. Historical rainfall patterns and drought frequency
        2. Zone-specific risk factors and adjustments
        3. Satellite data reliability and coverage
        4. Seasonal risk distribution across growth phases
        5. Climate change considerations
        6. Basis risk mitigation through satellite monitoring
        
        Use professional actuarial and meteorological terminology.
        """
        
        return self._call_openai(prompt, "risk_analysis")
    
    def _generate_payout_structure(self, quote_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate detailed payout structure"""
        
        deductible_rate = quote_data.get('deductible_rate', 0.05) * 100
        deductible_threshold = quote_data.get('deductible_threshold', 0) * 100
        phases = quote_data.get('phase_breakdown', [])
        
        payout_structure = {
            "payout_mechanism": "Linear interpolation between trigger and exit points",
            "deductible": f"{deductible_rate:.0f}% of sum insured",
            "deductible_threshold": f"{deductible_threshold:.1f}% minimum payout threshold",
            "maximum_payout": "100% of sum insured",
            "payout_formula": "L = {1 - [(Rainfall - Exit) / (Trigger - Exit)]} × Maximum Loss",
            "settlement_period": "30 days after season end",
            "front_loaded_weighting": "Earlier periods weighted higher due to crop vulnerability",
            "phase_weights": []
        }
        
        # Add phase-specific payout weights
        for phase in phases:
            weight_info = {
                "phase": phase.get('phase_name', 'Unknown'),
                "weight": f"{phase.get('phase_weight', 0) * 100:.1f}%",
                "rationale": f"Critical {phase.get('phase_name', 'growth')} period for {quote_data.get('crop', 'crop')} development"
            }
            payout_structure["phase_weights"].append(weight_info)
        
        return payout_structure
    
    def _enhance_phase_breakdown(self, quote_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Enhance phase breakdown with detailed analysis"""
        
        phases = quote_data.get('phase_breakdown', [])
        enhanced_phases = []
        
        for phase in phases:
            enhanced_phase = dict(phase)  # Copy original data
            
            # Add enhanced descriptions
            phase_name = phase.get('phase_name', '')
            if 'emergence' in phase_name.lower() or 'germination' in phase_name.lower():
                enhanced_phase['description'] = "Critical establishment phase - seed germination and early root development"
                enhanced_phase['risk_factors'] = ["Seed rot", "Poor germination", "Early seedling mortality"]
            elif 'vegetative' in phase_name.lower():
                enhanced_phase['description'] = "Vegetative growth phase - leaf development and root expansion"
                enhanced_phase['risk_factors'] = ["Stunted growth", "Leaf wilting", "Root system damage"]
            elif 'flowering' in phase_name.lower():
                enhanced_phase['description'] = "Reproductive phase - pollination and grain/pod formation"
                enhanced_phase['risk_factors'] = ["Pollination failure", "Flower abortion", "Poor grain set"]
            elif 'grain' in phase_name.lower() or 'fill' in phase_name.lower():
                enhanced_phase['description'] = "Grain filling phase - yield determination period"
                enhanced_phase['risk_factors'] = ["Grain shrinkage", "Poor grain weight", "Quality deterioration"]
            
            # Add water stress indicators
            trigger_mm = phase.get('trigger_mm', 0)
            exit_mm = phase.get('exit_mm', 0)
            enhanced_phase['stress_levels'] = {
                "mild_stress": f"Rainfall between {trigger_mm}mm and {(trigger_mm + exit_mm) / 2:.0f}mm",
                "moderate_stress": f"Rainfall between {(trigger_mm + exit_mm) / 2:.0f}mm and {exit_mm}mm",
                "severe_stress": f"Rainfall below {exit_mm}mm (exit point)"
            }
            
            enhanced_phases.append(enhanced_phase)
        
        return enhanced_phases
    
    def _generate_financial_summary(self, quote_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate comprehensive financial summary"""
        
        sum_insured = quote_data.get('sum_insured', 0)
        gross_premium = quote_data.get('gross_premium', 0)
        burning_cost = quote_data.get('burning_cost', 0)
        loadings = quote_data.get('loadings', {})
        deductible_amount = quote_data.get('deductible_amount', 0)
        
        financial_summary = {
            "sum_insured": f"${sum_insured:,.2f}",
            "gross_premium": f"${gross_premium:,.2f}",
            "premium_rate": f"{quote_data.get('premium_rate', 0) * 100:.2f}%",
            "burning_cost": f"${burning_cost:,.2f}",
            "technical_premium_rate": f"{(burning_cost / sum_insured * 100) if sum_insured > 0 else 0:.2f}%",
            "deductible": f"${deductible_amount:,.2f} ({quote_data.get('deductible_rate', 0) * 100:.0f}%)",
            "loadings_breakdown": {},
            "loss_ratio_projection": f"{quote_data.get('expected_payout_ratio', 0) * 100:.1f}%"
        }
        
        # Add loading breakdown
        total_loadings = sum(loadings.values()) if loadings else 0
        for loading_name, loading_amount in loadings.items():
            loading_rate = (loading_amount / burning_cost * 100) if burning_cost > 0 else 0
            financial_summary["loadings_breakdown"][loading_name] = {
                "amount": f"${loading_amount:,.2f}",
                "rate": f"{loading_rate:.1f}% of burning cost"
            }
        
        financial_summary["total_loadings"] = f"${total_loadings:,.2f}"
        
        return financial_summary
    
    def _generate_technical_specs(self, quote_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate technical specifications"""
        
        return {
            "data_source": "CHIRPS (Climate Hazards Group InfraRed Precipitation with Station data)",
            "satellite_resolution": "0.05° (~5.5km) spatial resolution",
            "temporal_resolution": "Daily rainfall measurements",
            "data_latency": "2-3 days from observation",
            "historical_baseline": f"{len(quote_data.get('historical_years_used', []))} years of calibration data",
            "quality_control": "Automated data validation and gap-filling procedures",
            "monitoring_frequency": "Daily observation with 10-day rolling assessment windows",
            "payout_triggers": "Objective satellite-based thresholds with no field visits required",
            "basis_risk_mitigation": "1.5km radius buffer around insured coordinates",
            "data_provider": "University of California Santa Barbara Climate Hazards Group"
        }
    
    def _generate_claims_procedure(self, quote_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate claims procedure details"""
        
        return {
            "claims_process": "Fully automated - no claims submission required",
            "trigger_assessment": "Automatic daily monitoring of rainfall thresholds",
            "payout_calculation": "Objective formula-based calculation using satellite data",
            "settlement_timeline": "30 days after season end verification",
            "documentation_required": "None - satellite data provides objective verification",
            "disputes_resolution": "Independent satellite data verification available",
            "payout_method": "Direct payment to registered account",
            "seasonal_reporting": "Monthly risk monitoring reports during growing season",
            "transparency": "Full satellite data access for verification purposes"
        }
    
    def _generate_terms_conditions(self, quote_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate terms and conditions"""
        
        return {
            "policy_period": "Single season coverage from planting to harvest",
            "coverage_territory": "Specified GPS coordinates with 1.5km buffer",
            "automatic_renewal": "Not applicable - seasonal coverage only",
            "premium_payment": "Full premium due before coverage inception",
            "policy_cancellation": "Not permitted after planting date confirmation",
            "force_majeure": "Coverage continues regardless of external factors",
            "data_availability": "Coverage void if satellite data unavailable for >10 consecutive days",
            "basis_risk_disclosure": "Satellite measurements may not perfectly correlate with field conditions",
            "exclusions": "No exclusions for weather-related perils within defined parameters",
            "governing_law": "Local jurisdiction where insured location is situated"
        }
    
    def _fallback_report_structure(self, quote_data: Dict[str, Any], location_info: Optional[Dict] = None) -> Dict[str, Any]:
        """Fallback report structure when AI is unavailable"""
        
        return {
            "executive_summary": "Professional weather index insurance quote - AI summary temporarily unavailable",
            "coverage_details": self._generate_coverage_details(quote_data),
            "risk_analysis": "Risk analysis based on satellite rainfall data - detailed analysis requires AI service",
            "payout_structure": self._generate_payout_structure(quote_data),
            "phase_breakdown": self._enhance_phase_breakdown(quote_data),
            "financial_summary": self._generate_financial_summary(quote_data),
            "technical_specifications": self._generate_technical_specs(quote_data),
            "claims_procedure": self._generate_claims_procedure(quote_data),
            "terms_and_conditions": self._generate_terms_conditions(quote_data)
        }
    
    def _call_openai(self, prompt: str, section_type: str) -> str:
        """Make OpenAI API call with error handling"""
        
        try:
            response = self.client.chat.completions.create(
                model=Config.OPENAI_MODEL,
                messages=[
                    {
                        "role": "system", 
                        "content": "You are an expert agricultural insurance underwriter and actuary. Provide detailed, professional analysis using industry-standard terminology. Focus on satellite-based index insurance and drought risk assessment."
                    },
                    {"role": "user", "content": prompt}
                ],
                max_tokens=1200,
                temperature=0.3
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as api_error:
            print(f"OpenAI API call error for {section_type}: {api_error}")
            return f"Professional {section_type.replace('_', ' ')} - AI analysis temporarily unavailable"
    
    def generate_quote_summary(self, quote_data: Dict[str, Any], location_info: Optional[Dict] = None) -> str:
        """Backward compatibility method for existing code"""
        comprehensive_report = self.generate_comprehensive_quote_report(quote_data, location_info)
        return comprehensive_report.get("executive_summary", "Quote summary unavailable")
    
    def generate_bulk_summary(self, quotes: List[Dict[str, Any]]) -> str:
        """Enhanced bulk summary generation"""
        
        if not self.client:
            return "Bulk portfolio analysis - AI summary not available"
        
        if not quotes:
            return "No quotes to analyze"
        
        try:
            # Portfolio analysis
            total_quotes = len(quotes)
            crops = [q.get('crop', 'unknown') for q in quotes]
            crop_counts = {crop: crops.count(crop) for crop in set(crops)}
            
            total_premium = sum(q.get('gross_premium', 0) for q in quotes)
            total_sum_insured = sum(q.get('sum_insured', 0) for q in quotes)
            avg_premium_rate = (total_premium / total_sum_insured * 100) if total_sum_insured > 0 else 0
            
            # Risk analysis
            avg_payout_ratio = sum(q.get('expected_payout_ratio', 0) for q in quotes) / total_quotes if total_quotes > 0 else 0
            
            prompt = f"""
            Generate a comprehensive portfolio analysis for agricultural index insurance.

            **PORTFOLIO SUMMARY:**
            - Total Policies: {total_quotes}
            - Crop Distribution: {', '.join([f'{crop}: {count}' for crop, count in crop_counts.items()])}
            - Total Sum Insured: ${total_sum_insured:,.2f}
            - Total Premium: ${total_premium:,.2f}
            - Portfolio Premium Rate: {avg_premium_rate:.2f}%
            - Expected Portfolio Loss Ratio: {avg_payout_ratio * 100:.1f}%

            Write a professional portfolio analysis covering:
            1. Risk diversification across crops and locations
            2. Premium adequacy and competitive positioning
            3. Expected profitability and loss projections
            4. Portfolio management recommendations
            5. Growth opportunities and risk mitigation strategies
            
            Use institutional insurance terminology suitable for underwriters and portfolio managers.
            """
            
            return self._call_openai(prompt, "bulk_portfolio")
            
        except Exception as e:
            print(f"Error generating bulk summary: {e}")
            return f"Portfolio analysis: {total_quotes} policies, ${total_sum_insured:,.0f} total coverage - detailed analysis temporarily unavailable"
