"""
AI-powered summary generation for insurance quotes
"""

import os
from openai import OpenAI
from typing import Dict, List, Any, Optional
from config import Config

class AISummaryGenerator:
    """Generates AI-powered summaries for insurance quotes"""
    
    def __init__(self):
        if not Config.OPENAI_API_KEY:
            print("Warning: OpenAI API key not configured. AI summaries will not be available.")
            self.client = None
        else:
            self.client = OpenAI(api_key=Config.OPENAI_API_KEY)
    
    def generate_quote_summary(self, quote_data: Dict[str, Any], location_info: Optional[Dict] = None) -> str:
        """
        Generate comprehensive AI summary for a quote
        
        Args:
            quote_data: Complete quote data
            location_info: Optional location information
            
        Returns:
            str: Generated summary
        """
        if not self.client:
            return "AI summary not available - OpenAI API key not configured."
        
        try:
            # Determine quote type and generate appropriate summary
            quote_type = quote_data.get('quote_type', 'unknown')
            
            if quote_type == 'historical':
                return self._generate_historical_summary(quote_data, location_info)
            else:
                return self._generate_prospective_summary(quote_data, location_info)
                
        except Exception as e:
            print(f"Error generating AI summary: {e}")
            return f"AI summary temporarily unavailable: {str(e)}"
    
    def _generate_historical_summary(self, quote_data: Dict[str, Any], location_info: Optional[Dict] = None) -> str:
        """Generate summary for historical quotes"""
        
        crop = quote_data.get('crop', 'maize').title()
        year = quote_data.get('year', 'current')
        sum_insured = quote_data.get('sum_insured', 0)
        gross_premium = quote_data.get('gross_premium', 0)
        premium_rate = quote_data.get('premium_rate', 0) * 100
        total_payout = quote_data.get('total_payout_usd', 0)
        net_payout = quote_data.get('net_payout_usd', 0)
        deductible = quote_data.get('deductible_amount', 0)
        
        # Location context
        location_name = "the selected area"
        if location_info:
            if location_info.get('type') == 'field' and location_info.get('name'):
                location_name = f"{location_info['name']} field"
            elif location_info.get('label'):
                location_name = location_info['label']
        
        # Planting information
        planting_date = quote_data.get('planting_date', 'November 2024')
        planting_info = quote_data.get('planting_trigger_description', 'Planting date estimated from rainfall patterns')
        
        # Build phase analysis
        phase_insights = self._analyze_phases(quote_data.get('phase_breakdown', []))
        
        # Threshold information
        threshold_note = quote_data.get('threshold_note', '')
        threshold_info = f"\n\n**THRESHOLD NOTE:** {threshold_note}" if threshold_note else ""
        
        prompt = f"""
        Generate a professional agricultural insurance analysis for {crop} crop at {location_name} for the {year} season.

        **POLICY OVERVIEW:**
        - Crop: {crop}
        - Location: {location_name}
        - Planting Date: {planting_date}
        - Planting Detection: {planting_info}
        - Sum Insured: ${sum_insured:,.2f}
        - Premium: ${gross_premium:,.2f} ({premium_rate:.1f}% of sum insured)

        **DROUGHT RISK ANALYSIS BY GROWTH PHASE:**
        {phase_insights}

        **FINANCIAL OUTCOME:**
        - Total Potential Claim: ${total_payout:,.2f}
        - Deductible: ${deductible:,.2f}
        - Net Payout: ${net_payout:,.2f}
        - Analysis: Historical simulation for {year} season{threshold_info}

        Write a clear, professional summary that:
        1. Explains what actually happened with rainfall during each critical growth phase
        2. Clearly distinguishes between total rainfall adequacy and dry spell impacts
        3. States the financial outcome (premium vs claim)
        4. Mentions the planting date detection method
        5. Provides practical insights for farmers about drought risk management
        6. Uses precise language about trigger vs exit thresholds
        7. Focuses specifically on rainfall/drought risk
        
        Important: When a phase has adequate total rainfall but still triggers a payout, explain that dry spells within the phase caused the trigger.
        """
        
        return self._call_openai(prompt, "historical")
    
    def _generate_prospective_summary(self, quote_data: Dict[str, Any], location_info: Optional[Dict] = None) -> str:
        """Generate summary for prospective quotes"""
        
        crop = quote_data.get('crop', 'maize').title()
        year = quote_data.get('year', 'current')
        sum_insured = quote_data.get('sum_insured', 0)
        gross_premium = quote_data.get('gross_premium', 0)
        premium_rate = quote_data.get('premium_rate', 0) * 100
        expected_payout_ratio = quote_data.get('expected_payout_ratio', 0) * 100
        deductible = quote_data.get('deductible_amount', 0)
        
        # Location context
        location_name = "the selected area"
        if location_info:
            if location_info.get('type') == 'field' and location_info.get('name'):
                location_name = f"{location_info['name']} field"
            elif location_info.get('label'):
                location_name = location_info['label']
        
        # Historical context
        historical_years = quote_data.get('historical_years_used', [])
        years_range = f"{min(historical_years)}-{max(historical_years)}" if historical_years else "Historical"
        data_points = len(historical_years)
        
        # Planting window
        planting_window = quote_data.get('planting_window_estimate', 'November-December')
        
        prompt = f"""
        Generate a professional agricultural insurance quote summary for {crop} crop at {location_name} for the {year} season.

        **PROSPECTIVE COVERAGE:**
        - Crop: {crop}
        - Location: {location_name}
        - Expected Planting Window: {planting_window}
        - Sum Insured: ${sum_insured:,.2f}
        - Annual Premium: ${gross_premium:,.2f} ({premium_rate:.1f}% of sum insured)

        **RISK ASSESSMENT:**
        - Premium calculated using {data_points} years of historical data ({years_range})
        - Expected Average Payout Probability: {expected_payout_ratio:.1f}%
        - Deductible: ${deductible:,.2f} ({quote_data.get('deductible_rate', 0) * 100:.1f}% of sum insured)

        **COVERAGE STRUCTURE:**
        This drought index insurance covers 4 critical growth phases with automatic rainfall monitoring:
        1. Emergence (Days 1-14): Early establishment phase
        2. Vegetative Growth (Days 15-49): Root and leaf development  
        3. Flowering (Days 50-84): Critical reproductive phase
        4. Grain Fill (Days 85-120): Yield formation phase

        **AUTOMATIC PLANTING DETECTION:** Coverage begins when 7-day cumulative rainfall ≥15mm with 2+ days ≥3mm, or default date if no trigger detected.

        **PAYOUT MECHANISM:** Payouts trigger when 10-day rolling observation windows fall below crop-specific rainfall thresholds during each phase, protecting against damaging dry spells.

        Write a compelling summary that:
        1. Explains the value proposition of rainfall index insurance
        2. Highlights the automatic, transparent payout mechanism
        3. Emphasizes protection during critical growth phases
        4. Clarifies that payouts protect against dry spells, not just total rainfall deficits
        5. Mentions the historical years used for premium calculation
        6. Focuses on drought risk protection with transparent trigger logic
        7. Notes automatic planting detection system
        """
        
        return self._call_openai(prompt, "prospective")
    
    def _analyze_phases(self, phase_breakdown: List[Dict[str, Any]]) -> str:
        """Analyze phase breakdown and create insights"""
        
        if not phase_breakdown:
            return "Phase analysis not available"
        
        phase_insights = []
        
        for phase in phase_breakdown:
            payout_usd = phase.get('payout_usd', 0)
            rain_received = phase.get('rainfall_received_mm', 0)
            water_need = phase.get('water_need_mm', 0)
            deficit = phase.get('deficit_mm', 0)
            trigger_mm = phase.get('trigger_mm', 0)
            exit_mm = phase.get('exit_mm', 0)
            triggering_period = phase.get('triggering_period')
            payout_ratio = phase.get('payout_ratio', 0)
            
            # Create accurate phase analysis
            if payout_usd > 0:
                if triggering_period:
                    trigger_start = triggering_period['start_date']
                    trigger_end = triggering_period['end_date']
                    trigger_rain = triggering_period['rainfall_mm']
                    phase_insights.append(
                        f"**{phase['phase_name']}**: Total rainfall {rain_received}mm (vs {water_need}mm needed). "
                        f"Payout triggered by dry spell {trigger_start} to {trigger_end} with only {trigger_rain}mm "
                        f"(below {trigger_mm}mm trigger threshold). Payout: ${payout_usd:,.2f}"
                    )
                else:
                    phase_insights.append(
                        f"**{phase['phase_name']}**: Received {rain_received}mm vs {water_need}mm needed "
                        f"({deficit}mm deficit), payout ratio {payout_ratio:.2%}, triggering ${payout_usd:,.2f} payout"
                    )
            else:
                if rain_received >= water_need:
                    phase_insights.append(
                        f"**{phase['phase_name']}**: Received {rain_received}mm vs {water_need}mm needed. "
                        f"Adequate rainfall throughout phase, no dry spells below {trigger_mm}mm threshold. No payout."
                    )
                else:
                    phase_insights.append(
                        f"**{phase['phase_name']}**: Received {rain_received}mm vs {water_need}mm needed "
                        f"({deficit}mm deficit), but rainfall above {exit_mm}mm exit threshold. No payout."
                    )
        
        return "\n".join(phase_insights)
    
    def _call_openai(self, prompt: str, quote_type: str) -> str:
        """Make OpenAI API call with error handling"""
        
        try:
            response = self.client.chat.completions.create(
                model=Config.OPENAI_MODEL,
                messages=[
                    {
                        "role": "system", 
                        "content": "You are an expert agricultural insurance analyst specializing in index-based crop insurance. Focus on rainfall/drought risks only. Provide clear, accurate explanations of dry spell triggers vs total rainfall adequacy. Be precise about 10-day observation windows and avoid generic insurance language."
                    },
                    {"role": "user", "content": prompt}
                ],
                max_tokens=900,
                temperature=0.3
            )
            
            summary_text = response.choices[0].message.content.strip()
            return summary_text
            
        except Exception as api_error:
            print(f"OpenAI API call error: {api_error}")
            return f"AI summary temporarily unavailable: {str(api_error)}"
    
    def generate_bulk_summary(self, quotes: List[Dict[str, Any]]) -> str:
        """Generate summary for bulk quotes"""
        
        if not self.client:
            return "AI summary not available - OpenAI API key not configured."
        
        if not quotes:
            return "No quotes to summarize."
        
        try:
            # Analyze bulk quote patterns
            total_quotes = len(quotes)
            crops = [q.get('crop', 'unknown') for q in quotes]
            crop_counts = {crop: crops.count(crop) for crop in set(crops)}
            
            total_premium = sum(q.get('gross_premium', 0) for q in quotes)
            total_sum_insured = sum(q.get('sum_insured', 0) for q in quotes)
            avg_premium_rate = (total_premium / total_sum_insured * 100) if total_sum_insured > 0 else 0
            
            prompt = f"""
            Generate a professional summary for a bulk insurance quote covering {total_quotes} agricultural fields.

            **PORTFOLIO OVERVIEW:**
            - Total Fields: {total_quotes}
            - Crop Distribution: {', '.join([f'{crop}: {count}' for crop, count in crop_counts.items()])}
            - Total Sum Insured: ${total_sum_insured:,.2f}
            - Total Premium: ${total_premium:,.2f}
            - Average Premium Rate: {avg_premium_rate:.2f}%

            **KEY INSIGHTS:**
            Write a concise summary highlighting:
            1. Portfolio diversification across crops
            2. Overall risk profile and premium efficiency
            3. Coverage highlights and risk protection
            4. Recommendations for the agricultural portfolio
            
            Keep it professional and focused on the agricultural insurance value proposition.
            """
            
            return self._call_openai(prompt, "bulk")
            
        except Exception as e:
            print(f"Error generating bulk summary: {e}")
            return f"Bulk summary error: {str(e)}"
