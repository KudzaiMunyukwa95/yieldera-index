"""
Enhanced AI Summary Generator with Improved Executive Summary Templates
Addresses repetition issues and adds more personalization
"""

import openai
from typing import Dict, Any, Optional
from datetime import datetime

class EnhancedAISummaryGenerator:
    """Enhanced AI-powered summary generator with improved templates"""
    
    def __init__(self, openai_api_key: str = None):
        """Initialize with OpenAI API key"""
        if openai_api_key:
            openai.api_key = openai_api_key
        
        # Enhanced prompt templates for different scenarios
        self.executive_summary_templates = {
            "prospective": {
                "opening_lines": [
                    "This Weather Index Insurance (WII) policy for {crop} represents a cutting-edge agricultural risk management solution",
                    "The proposed {crop} Weather Index Insurance offers comprehensive drought protection",
                    "This institutional-grade WII product for {crop} cultivation provides sophisticated risk mitigation"
                ],
                "technology_descriptions": [
                    "utilizing advanced CHIRPS satellite rainfall monitoring for precise, objective risk assessment",
                    "leveraging Climate Hazards Group satellite precipitation data for transparent claim triggers",
                    "employing state-of-the-art satellite-based rainfall detection technology"
                ],
                "value_propositions": [
                    "eliminating traditional loss assessment delays while ensuring rapid, automatic claim settlement",
                    "providing immediate financial protection through objective, weather-based trigger mechanisms",
                    "offering streamlined claim processing via satellite-verified precipitation data"
                ]
            },
            "historical": {
                "opening_lines": [
                    "This historical Weather Index Insurance analysis for {crop} demonstrates proven risk management capabilities",
                    "The retrospective {crop} insurance evaluation reveals strong protective value",
                    "Historical performance data for this {crop} WII product shows consistent risk mitigation"
                ],
                "technology_descriptions": [
                    "based on comprehensive CHIRPS satellite rainfall analysis spanning multiple growing seasons",
                    "utilizing extensive historical precipitation data from Climate Hazards Group monitoring",
                    "grounded in multi-year satellite rainfall observations for robust risk assessment"
                ],
                "value_propositions": [
                    "demonstrating reliable protection against historical drought events in this specific location",
                    "showing proven effectiveness in mitigating agricultural losses during past seasons",
                    "validating the product's capacity to provide consistent farmer protection over time"
                ]
            }
        }
    
    def generate_comprehensive_quote_report(self, quote_result: Dict[str, Any], 
                                          location_info: Optional[Dict] = None) -> Dict[str, Any]:
        """Generate comprehensive report with improved executive summary"""
        
        try:
            # Extract key quote information
            crop = quote_result.get('crop', 'crop').title()
            quote_type = quote_result.get('quote_type', 'prospective')
            sum_insured = quote_result.get('sum_insured', 0)
            premium_rate = quote_result.get('premium_rate', 0) * 100
            gross_premium = quote_result.get('gross_premium', 0)
            year = quote_result.get('year', datetime.now().year)
            
            # Location-specific information
            location_desc = self._get_location_description(quote_result, location_info)
            risk_profile = self._analyze_risk_profile(quote_result)
            
            # Generate personalized executive summary
            executive_summary = self._generate_executive_summary(
                crop=crop,
                quote_type=quote_type,
                sum_insured=sum_insured,
                premium_rate=premium_rate,
                gross_premium=gross_premium,
                year=year,
                location_desc=location_desc,
                risk_profile=risk_profile,
                quote_result=quote_result
            )
            
            # Generate other report sections
            risk_assessment = self._generate_risk_assessment(quote_result)
            technical_details = self._generate_technical_details(quote_result)
            recommendations = self._generate_recommendations(quote_result)
            
            return {
                "executive_summary": executive_summary,
                "risk_assessment": risk_assessment,
                "technical_details": technical_details,
                "recommendations": recommendations,
                "generation_timestamp": datetime.utcnow().isoformat(),
                "report_version": "2.0-Enhanced"
            }
            
        except Exception as e:
            print(f"Error generating comprehensive report: {e}")
            return {
                "executive_summary": self._generate_fallback_summary(quote_result),
                "error": "Partial report generation due to processing error"
            }
    
    def _generate_executive_summary(self, crop: str, quote_type: str, sum_insured: float,
                                  premium_rate: float, gross_premium: float, year: int,
                                  location_desc: str, risk_profile: Dict, 
                                  quote_result: Dict) -> str:
        """Generate personalized executive summary"""
        
        import random
        
        # Select template based on quote type
        template = self.executive_summary_templates.get(quote_type, self.executive_summary_templates["prospective"])
        
        # Randomly select varied phrases to avoid repetition
        opening = random.choice(template["opening_lines"]).format(crop=crop)
        tech_desc = random.choice(template["technology_descriptions"])
        value_prop = random.choice(template["value_propositions"])
        
        # Build personalized summary
        summary_parts = []
        
        # Opening with specific details
        summary_parts.append(
            f"{opening} for the {year} coverage period. This product provides ${sum_insured:,.2f} "
            f"in coverage at a {premium_rate:.1f}% premium rate, resulting in a total premium of ${gross_premium:,.2f}."
        )
        
        # Location and risk-specific content
        if location_desc:
            summary_parts.append(
                f"Designed specifically for {location_desc}, this policy addresses the unique "
                f"climatic challenges and {risk_profile['primary_risk'].lower()} patterns of the region."
            )
        
        # Technology and methodology
        summary_parts.append(
            f"The insurance mechanism operates through {tech_desc}, ensuring complete objectivity "
            f"and transparency in claim determination. This advanced approach {value_prop}."
        )
        
        # Performance insights (if historical data available)
        if 'field_story' in quote_result and quote_result['field_story']:
            story = quote_result['field_story']
            if 'historical_performance' in story:
                perf = story['historical_performance']
                payout_freq = perf.get('payout_frequency_percent', 0)
                summary_parts.append(
                    f"Historical analysis reveals that similar fields in this area would have "
                    f"received payouts in {payout_freq:.1f}% of seasons, demonstrating the product's "
                    f"responsive protection against local drought conditions."
                )
        
        # Value proposition and competitive advantage
        risk_level = risk_profile.get('risk_category', 'moderate')
        summary_parts.append(
            f"This index-based insurance solution eliminates traditional crop insurance limitations "
            f"such as moral hazard and adverse selection, while providing rapid claim settlement "
            f"crucial for {risk_level}-risk agricultural environments. The satellite-based trigger "
            f"mechanism ensures farmers receive timely financial support precisely when drought "
            f"conditions threaten their {crop.lower()} production."
        )
        
        # Professional closing
        summary_parts.append(
            f"Representing a significant advancement in agricultural risk management, this WII product "
            f"combines cutting-edge technology with sound actuarial principles to deliver institutional-grade "
            f"protection tailored to modern agricultural enterprises."
        )
        
        return " ".join(summary_parts)
    
    def _get_location_description(self, quote_result: Dict, location_info: Optional[Dict]) -> str:
        """Generate location-specific description"""
        
        if location_info and 'name' in location_info:
            return location_info['name']
        
        # Try to extract from field info
        if 'field_info' in quote_result:
            field_info = quote_result['field_info']
            if 'name' in field_info:
                return f"the {field_info['name']} agricultural area"
        
        # Use coordinates if available
        lat = quote_result.get('latitude')
        lon = quote_result.get('longitude')
        if lat and lon:
            # Simple region identification for Southern Africa
            if lat > -18:
                region = "northern Zimbabwe/southern Zambia region"
            elif lat > -20:
                region = "central Zimbabwe highlands"
            else:
                region = "southern Zimbabwe/northern South Africa region"
            return f"farming operations in the {region}"
        
        return "the designated agricultural area"
    
    def _analyze_risk_profile(self, quote_result: Dict) -> Dict[str, str]:
        """Analyze risk profile from quote data"""
        
        premium_rate = quote_result.get('premium_rate', 0) * 100
        zone = quote_result.get('zone', 'standard')
        
        # Determine risk category
        if premium_rate < 8:
            risk_category = "low"
            primary_risk = "Occasional seasonal variation"
        elif premium_rate < 15:
            risk_category = "moderate"
            primary_risk = "Periodic drought stress"
        else:
            risk_category = "elevated"
            primary_risk = "Frequent drought conditions"
        
        # Zone-specific adjustments
        if 'lowveld' in zone.lower():
            primary_risk = "Severe drought and heat stress"
            risk_category = "high"
        elif 'masvingo' in zone.lower():
            primary_risk = "Semi-arid drought conditions"
        
        return {
            "risk_category": risk_category,
            "primary_risk": primary_risk,
            "premium_indication": f"{premium_rate:.1f}% rate reflects {risk_category} risk environment"
        }
    
    def _generate_risk_assessment(self, quote_result: Dict) -> str:
        """Generate detailed risk assessment section"""
        
        simulation_summary = quote_result.get('simulation_summary', {})
        years_analyzed = simulation_summary.get('years_analyzed', 0)
        avg_impact = simulation_summary.get('average_drought_impact', 0)
        payout_freq = simulation_summary.get('payout_frequency', 0)
        
        assessment = f"Risk analysis based on {years_analyzed} years of historical data indicates "
        assessment += f"an average drought impact of {avg_impact:.1f}% with payout events occurring "
        assessment += f"in {payout_freq:.1f}% of analyzed seasons. "
        
        if avg_impact < 10:
            assessment += "This represents a relatively stable growing environment with occasional stress periods."
        elif avg_impact < 20:
            assessment += "This indicates moderate drought risk requiring active risk management."
        else:
            assessment += "This reflects significant drought exposure necessitating comprehensive protection."
        
        return assessment
    
    def _generate_technical_details(self, quote_result: Dict) -> str:
        """Generate technical implementation details"""
        
        crop = quote_result.get('crop', 'crop')
        methodology = quote_result.get('methodology', 'satellite-based')
        
        details = f"The {crop} insurance utilizes CHIRPS satellite precipitation data at 5.5km resolution "
        details += f"with automatic planting date detection based on rainfall triggers. Coverage spans "
        details += f"all critical growth phases including emergence, vegetative development, flowering, "
        details += f"and grain filling periods. Payout calculations employ phase-weighted drought impact "
        details += f"assessment with rolling window analysis for precise risk quantification."
        
        return details
    
    def _generate_recommendations(self, quote_result: Dict) -> str:
        """Generate implementation recommendations"""
        
        premium_rate = quote_result.get('premium_rate', 0) * 100
        
        recs = "Implementation recommendations include: "
        
        if premium_rate > 12:
            recs += "Consider drought-resistant varieties and supplemental irrigation where feasible. "
        
        recs += "Maintain detailed planting records for optimal coverage alignment. "
        recs += "Monitor satellite-based rainfall reports during critical growth phases. "
        recs += "Coordinate with insurance provider for real-time risk updates throughout the season."
        
        return recs
    
    def _generate_fallback_summary(self, quote_result: Dict) -> str:
        """Generate basic fallback summary if AI generation fails"""
        
        crop = quote_result.get('crop', 'crop').title()
        sum_insured = quote_result.get('sum_insured', 0)
        premium_rate = quote_result.get('premium_rate', 0) * 100
        gross_premium = quote_result.get('gross_premium', 0)
        year = quote_result.get('year', datetime.now().year)
        
        return (f"Weather Index Insurance for {crop} - {year} Coverage Year. "
                f"Sum Insured: ${sum_insured:,.2f}, Premium Rate: {premium_rate:.1f}%, "
                f"Total Premium: ${gross_premium:,.2f}. Satellite-based drought protection "
                f"with automatic claim settlement.")

# Enhanced prompt for OpenAI integration (if using AI generation)
ENHANCED_EXECUTIVE_SUMMARY_PROMPT = """
Generate a sophisticated, professional executive summary for a Weather Index Insurance product with these requirements:

1. AVOID repetitive phrases used in previous summaries
2. Personalize content based on specific location and risk profile
3. Include actual financial figures and performance data
4. Vary language and structure from standard templates
5. Focus on unique value proposition for this specific case
6. Use institutional-grade professional tone
7. Highlight satellite technology benefits without overuse of buzzwords

Quote Details: {quote_data}
Location Context: {location_context}
Risk Profile: {risk_analysis}

Generate a 4-5 paragraph executive summary that feels fresh and specific to this particular insurance case.
"""

def generate_ai_powered_summary(quote_result: Dict, openai_client) -> str:
    """Alternative AI-powered summary generation using OpenAI"""
    
    try:
        prompt = ENHANCED_EXECUTIVE_SUMMARY_PROMPT.format(
            quote_data=str(quote_result),
            location_context="Agricultural area with specific climatic conditions",
            risk_analysis="Based on historical drought patterns and satellite data"
        )
        
        response = openai_client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are an expert agricultural insurance analyst writing executive summaries for institutional clients."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=500,
            temperature=0.7
        )
        
        return response.choices[0].message.content
        
    except Exception as e:
        print(f"AI summary generation failed: {e}")
        return "AI-powered summary temporarily unavailable"
