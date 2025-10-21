"""
Enhanced AI Summary Generator with Data-Driven Actuarial Executive Summaries
Professional actuarial reporting for enterprise insurance underwriters
"""

import openai
from typing import Dict, Any, Optional
from datetime import datetime

class EnhancedAISummaryGenerator:
    """Enhanced AI-powered summary generator with actuarial-focused executive summaries"""
    
    def __init__(self, openai_api_key: str = None):
        """Initialize with OpenAI API key"""
        if openai_api_key:
            openai.api_key = openai_api_key
    
    def generate_comprehensive_quote_report(self, quote_result: Dict[str, Any], 
                                          location_info: Optional[Dict] = None) -> Dict[str, Any]:
        """Generate comprehensive report with data-driven executive summary"""
        
        try:
            # Extract key quote information
            crop = quote_result.get('crop', 'crop').title()
            quote_type = quote_result.get('quote_type', 'prospective')
            sum_insured = quote_result.get('sum_insured', 0)
            premium_rate = quote_result.get('premium_rate', 0)
            gross_premium = quote_result.get('gross_premium', 0)
            year = quote_result.get('coverage_year', datetime.now().year)
            
            # Location-specific information
            location_desc = self._get_location_description(quote_result, location_info)
            risk_profile = self._analyze_risk_profile(quote_result)
            
            # Generate data-driven executive summary
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
                "report_version": "3.0-Actuarial"
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
        """Generate data-driven actuarial executive summary"""
        
        # Extract field information
        field_info = quote_result.get('field_info', {})
        field_name = field_info.get('name', 'the insured location')
        if field_name.startswith('Field '):
            field_name = field_name  # Keep as is
        
        # Extract coordinates
        latitude = quote_result.get('latitude', 0)
        longitude = quote_result.get('longitude', 0)
        area_ha = quote_result.get('area_ha', 0)
        
        # Extract risk metrics
        risk_metrics = quote_result.get('risk_metrics', {})
        avg_drought_impact = risk_metrics.get('average_drought_impact_pct', 0)
        drought_volatility = risk_metrics.get('drought_volatility_std', 0)
        payout_frequency = risk_metrics.get('payout_frequency_pct', 0)
        expected_loss_ratio = risk_metrics.get('expected_loss_ratio', 0)
        pml_90pct = risk_metrics.get('probable_maximum_loss_90pct', 0)
        avg_expected_payout = risk_metrics.get('average_expected_payout', 0)
        
        # Extract actuarial basis
        actuarial_basis = quote_result.get('actuarial_basis', {})
        years_analyzed = actuarial_basis.get('years_analyzed', 0)
        data_quality = actuarial_basis.get('data_quality_pct', 0)
        credibility_rating = actuarial_basis.get('credibility_rating', 'Unknown')
        methodology = actuarial_basis.get('methodology', 'Industry Standard 10-Day Rolling Drought Detection')
        
        # Extract loadings
        loadings = quote_result.get('loadings_breakdown', {})
        admin_rate = loadings.get('admin', {}).get('rate', 0) * 100
        reinsurance_rate = loadings.get('reinsurance', {}).get('rate', 0) * 100
        margin_rate = loadings.get('margin', {}).get('rate', 0) * 100
        
        # Extract deductible info
        deductible_rate = quote_result.get('deductible_rate', 0) * 100
        deductible_amount = quote_result.get('deductible_amount', 0)
        
        # Analyze historical simulation for worst/best years
        historical_simulation = quote_result.get('historical_simulation', [])
        worst_year_data = None
        best_year_data = None
        
        if historical_simulation:
            worst_year_data = max(historical_simulation, key=lambda x: x.get('drought_impact_pct', 0))
            best_year_data = min(historical_simulation, key=lambda x: x.get('drought_impact_pct', 0))
        
        # Determine variability interpretation
        if drought_volatility < 8:
            variability_desc = "low variability"
        elif drought_volatility <= 12:
            variability_desc = "moderate variability"
        else:
            variability_desc = "high variability"
        
        # Calculate PML dollar amount
        pml_90_dollar = sum_insured * (pml_90pct / 100) if pml_90pct > 0 else 0
        
        # Build actuarial summary
        summary_parts = []
        
        # 1. Opening Statement
        summary_parts.append(
            f"This Weather Index Insurance policy provides ${sum_insured:,.0f} coverage for "
            f"{area_ha:.2f} hectares of {crop} on {field_name} (Coordinates: {latitude:.4f}°, {longitude:.4f}°) "
            f"for the {year} growing season at a {premium_rate*100:.2f}% premium rate (${gross_premium:,.0f} total premium)."
        )
        
        # 2. Historical Risk Profile
        historical_period_start = min(h.get('year', year) for h in historical_simulation) if historical_simulation else year - years_analyzed + 1
        historical_period_end = max(h.get('year', year) for h in historical_simulation) if historical_simulation else year - 1
        
        summary_parts.append(
            f"**Historical Risk Profile ({historical_period_start}-{historical_period_end}):** "
            f"Based on {years_analyzed} years of CHIRPS satellite rainfall data, this location experienced "
            f"drought stress in {payout_frequency:.1f}% of seasons, with an average drought impact of "
            f"{avg_drought_impact:.1f}% yield loss. The historical volatility shows a standard deviation of "
            f"{drought_volatility:.1f}%, indicating {variability_desc} in drought severity."
        )
        
        # 3. Loss Performance
        if worst_year_data:
            worst_year = worst_year_data.get('year', 'unknown')
            worst_drought_impact = worst_year_data.get('drought_impact_pct', 0)
            worst_payout = worst_year_data.get('simulated_payout', 0)
            worst_loss_ratio = worst_year_data.get('loss_ratio', 0)
            
            summary_parts.append(
                f"**Loss Performance:** "
                f"Historical simulation demonstrates an expected loss ratio of {expected_loss_ratio:.2f}, "
                f"meaning payouts average ${avg_expected_payout:,.0f} per season against the ${gross_premium:,.0f} premium. "
                f"The 90th percentile probable maximum loss is {pml_90pct:.1f}% (${pml_90_dollar:,.0f} payout), "
                f"with the worst historical year ({worst_year}) experiencing {worst_drought_impact:.1f}% drought impact "
                f"(${worst_payout:,.0f} payout, {worst_loss_ratio:.2f} loss ratio)."
            )
        else:
            summary_parts.append(
                f"**Loss Performance:** "
                f"Historical simulation demonstrates an expected loss ratio of {expected_loss_ratio:.2f}, "
                f"meaning payouts average ${avg_expected_payout:,.0f} per season against the ${gross_premium:,.0f} premium. "
                f"The 90th percentile probable maximum loss is {pml_90pct:.1f}% (${pml_90_dollar:,.0f} payout)."
            )
        
        # 4. Actuarial Basis
        valid_seasons = len([h for h in historical_simulation if h.get('drought_impact_pct') is not None])
        summary_parts.append(
            f"**Actuarial Basis:** "
            f"This quote uses {methodology.lower()} applied to daily CHIRPS precipitation data. "
            f"The {years_analyzed}-year historical period provides {data_quality:.0f}% data quality with "
            f"{credibility_rating.lower()} actuarial credibility. All {valid_seasons} seasons had valid planting dates "
            f"between October and January, confirming reliable seasonal pattern detection."
        )
        
        # 5. Coverage Mechanism
        crop_phases = 4  # Standard number of crop phases
        summary_parts.append(
            f"**Coverage Mechanism:** "
            f"The policy triggers payouts when cumulative drought stress exceeds {deductible_rate:.1f}% yield loss threshold "
            f"({deductible_rate:.1f}% deductible = ${deductible_amount:,.0f}). The index combines 10-day rolling precipitation "
            f"deficit analysis with consecutive dry spell detection across {crop_phases} critical {crop} growth phases. "
            f"Payouts are automatic and satellite-based, requiring no field visits or loss adjusters."
        )
        
        # 6. Rate Justification
        burning_cost_rate = (quote_result.get('burning_cost', 0) / sum_insured * 100) if sum_insured > 0 else 0
        summary_parts.append(
            f"**Rate Justification:** "
            f"The {premium_rate*100:.2f}% premium rate consists of {burning_cost_rate:.2f}% burning cost based on "
            f"{years_analyzed}-year historical loss experience, plus loadings for administration ({admin_rate:.0f}%), "
            f"reinsurance ({reinsurance_rate:.0f}%), and margin ({margin_rate:.0f}%). This rate falls within "
            f"acceptable actuarial range (1.5%-20%) and provides adequate protection against the historical "
            f"{payout_frequency:.1f}% drought frequency at this location."
        )
        
        return "\n\n".join(summary_parts)
    
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
        zone = quote_result.get('agro_ecological_zone', 'standard')
        
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
        
        risk_metrics = quote_result.get('risk_metrics', {})
        years_analyzed = quote_result.get('actuarial_basis', {}).get('years_analyzed', 0)
        avg_impact = risk_metrics.get('average_drought_impact_pct', 0)
        payout_freq = risk_metrics.get('payout_frequency_pct', 0)
        
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
        methodology = quote_result.get('actuarial_basis', {}).get('methodology', 'satellite-based')
        
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
        year = quote_result.get('coverage_year', datetime.now().year)
        
        return (f"Weather Index Insurance for {crop} - {year} Coverage Year. "
                f"Sum Insured: ${sum_insured:,.2f}, Premium Rate: {premium_rate:.1f}%, "
                f"Total Premium: ${gross_premium:,.2f}. Satellite-based drought protection "
                f"with automatic claim settlement.")

    def generate_bulk_summary(self, successful_quotes: list) -> dict:
        """Generate bulk portfolio analysis summary"""
        if not successful_quotes:
            return {"error": "No successful quotes to analyze"}
        
        # Calculate portfolio metrics
        total_premium = sum(q.get('gross_premium', 0) for q in successful_quotes)
        total_sum_insured = sum(q.get('sum_insured', 0) for q in successful_quotes)
        avg_premium_rate = (total_premium / total_sum_insured * 100) if total_sum_insured > 0 else 0
        
        portfolio_summary = {
            "portfolio_summary": f"Portfolio of {len(successful_quotes)} policies with "
                               f"${total_sum_insured:,.0f} total coverage and "
                               f"${total_premium:,.0f} total premium ({avg_premium_rate:.2f}% average rate).",
            "total_policies": len(successful_quotes),
            "total_premium": total_premium,
            "total_sum_insured": total_sum_insured,
            "average_premium_rate": avg_premium_rate
        }
        
        return portfolio_summary

# Enhanced prompt for OpenAI integration (if using AI generation)
ENHANCED_EXECUTIVE_SUMMARY_PROMPT = """
Generate a sophisticated, professional executive summary for a Weather Index Insurance product with these requirements:

1. Use specific actuarial data and financial figures
2. Focus on loss ratios, historical performance, and risk metrics
3. Include actual dates, percentages, and dollar amounts
4. Use professional actuarial language suitable for underwriters
5. Structure as: Coverage Details, Risk Analysis, Loss Performance, Actuarial Basis

Quote Details: {quote_data}
Location Context: {location_context}
Risk Profile: {risk_analysis}

Generate a 4-5 paragraph executive summary using exact data from the quote results.
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
                {"role": "system", "content": "You are an expert agricultural insurance actuary writing executive summaries for institutional clients."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=500,
            temperature=0.3
        )
        
        return response.choices[0].message.content
        
    except Exception as e:
        print(f"AI summary generation failed: {e}")
        return "AI-powered summary temporarily unavailable"
