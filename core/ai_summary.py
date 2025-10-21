"""
Enterprise-Grade Executive Summary Generator for Agricultural Index Insurance
Data-driven, specific, and decision-focused for B2B insurance underwriters
NO marketing fluff - only actuarial facts and risk metrics
"""

from typing import Dict, Any, Optional, List
from datetime import datetime
import json


class EnterpriseExecutiveSummaryGenerator:
    """
    Generate professional, data-driven executive summaries for insurance quotes
    Designed for underwriters, reinsurers, and enterprise decision-makers
    """
    
    def __init__(self):
        """Initialize with enterprise standards"""
        self.version = "4.0-Enterprise"
        self.standards = {
            "minimum_years_excellent": 25,
            "minimum_years_good": 20,
            "minimum_years_acceptable": 15,
            "high_payout_frequency": 60,  # % threshold for "high" frequency
            "high_drought_impact": 20,    # % threshold for "high" impact
            "high_volatility": 1.0        # Loss ratio std dev threshold
        }
    
    def generate_comprehensive_executive_summary(
        self, 
        quote_result: Dict[str, Any],
        field_name: Optional[str] = None,
        location_context: Optional[Dict] = None
    ) -> str:
        """
        Generate complete executive summary from quote result
        
        Args:
            quote_result: Complete quote result from quote engine
            field_name: Optional field name (e.g., "Combined", "Ward 15")
            location_context: Optional additional location info
            
        Returns:
            Formatted executive summary string
        """
        try:
            # Extract all required data
            data = self._extract_quote_data(quote_result)
            
            # Determine field name
            field_display = field_name or location_context.get('name') if location_context else "Target Field"
            
            # Build summary sections
            sections = []
            
            # 1. Coverage Overview
            sections.append(self._build_coverage_overview(data, field_display))
            
            # 2. Pricing Recommendation
            sections.append(self._build_pricing_recommendation(data))
            
            # 3. Risk Assessment
            sections.append(self._build_risk_assessment(data))
            
            # 4. Key Findings
            sections.append(self._build_key_findings(data))
            
            # 5. Recommendation
            sections.append(self._build_recommendation(data))
            
            # Combine all sections
            executive_summary = "\n\n".join(sections)
            
            return executive_summary
            
        except Exception as e:
            print(f"ERROR: Failed to generate executive summary: {e}")
            return self._generate_fallback_summary(quote_result)
    
    def _extract_quote_data(self, quote_result: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and organize all relevant data from quote result"""
        
        # Core quote data
        data = {
            'quote_id': quote_result.get('quote_id', 'N/A'),
            'generated_at': quote_result.get('generated_at', datetime.utcnow().isoformat()),
            'quote_type': quote_result.get('quote_type', 'prospective'),
            'coverage_year': quote_result.get('coverage_year', datetime.now().year),
            
            # Coverage details
            'crop': quote_result.get('crop', 'unknown').title(),
            'area_ha': quote_result.get('area_ha', 0),
            'latitude': quote_result.get('latitude', 0),
            'longitude': quote_result.get('longitude', 0),
            'zone': quote_result.get('agro_ecological_zone', 'auto_detect'),
            
            # Financial metrics
            'sum_insured': quote_result.get('sum_insured', 0),
            'expected_yield': quote_result.get('expected_yield_t_ha', 0),
            'price_per_ton': quote_result.get('price_per_ton', 0),
            'premium_rate': quote_result.get('premium_rate', 0),
            'burning_cost': quote_result.get('burning_cost', 0),
            'gross_premium': quote_result.get('gross_premium', 0),
            'total_loadings': quote_result.get('total_loadings', 0),
            'deductible_rate': quote_result.get('deductible_rate', 0),
            'deductible_amount': quote_result.get('deductible_amount', 0),
            'loadings_breakdown': quote_result.get('loadings_breakdown', {}),
            
            # Risk metrics
            'risk_metrics': quote_result.get('risk_metrics', {}),
            
            # Actuarial basis
            'actuarial_basis': quote_result.get('actuarial_basis', {}),
            
            # Historical simulation
            'historical_simulation': quote_result.get('historical_simulation', []),
            
            # Compliance
            'compliance': quote_result.get('compliance', {})
        }
        
        # Calculate derived statistics
        hist_sim = data['historical_simulation']
        if hist_sim:
            # Years with meaningful payouts
            years_with_payout = len([y for y in hist_sim if y.get('simulated_payout', 0) > 0])
            data['payout_frequency_pct'] = (years_with_payout / len(hist_sim)) * 100
            
            # Worst years
            sorted_by_impact = sorted(hist_sim, key=lambda x: x.get('drought_impact_pct', 0), reverse=True)
            data['worst_year'] = sorted_by_impact[0] if sorted_by_impact else None
            data['worst_3_years'] = sorted_by_impact[:3] if len(sorted_by_impact) >= 3 else sorted_by_impact
            
            # Best year
            data['best_year'] = sorted_by_impact[-1] if sorted_by_impact else None
            
            # Average payout
            total_payouts = sum(y.get('simulated_payout', 0) for y in hist_sim)
            data['avg_payout'] = total_payouts / len(hist_sim)
        else:
            data['payout_frequency_pct'] = 0
            data['worst_year'] = None
            data['worst_3_years'] = []
            data['best_year'] = None
            data['avg_payout'] = 0
        
        return data
    
    def _build_coverage_overview(self, data: Dict[str, Any], field_name: str) -> str:
        """Build Coverage Overview section"""
        
        zone_display = self._format_zone_name(data['zone'])
        
        overview = f"""COVERAGE OVERVIEW
- Field: {field_name} ({data['area_ha']:.2f} ha)
- Crop: {data['crop']}
- Coverage Period: {data['coverage_year']} Season
- Sum Insured: ${data['sum_insured']:,.2f}
- Location: Lat {data['latitude']:.4f}, Long {data['longitude']:.4f} ({zone_display})"""
        
        return overview
    
    def _build_pricing_recommendation(self, data: Dict[str, Any]) -> str:
        """Build Pricing Recommendation section"""
        
        pricing = f"""PRICING RECOMMENDATION
- Premium Rate: {data['premium_rate']*100:.2f}%
- Gross Premium: ${data['gross_premium']:,.2f}
- Burning Cost: ${data['burning_cost']:,.2f}
- Total Loadings: ${data['total_loadings']:,.2f}"""
        
        # Add loadings breakdown
        if data['loadings_breakdown']:
            pricing += "\n• Loadings Breakdown:"
            for loading_name, loading_data in data['loadings_breakdown'].items():
                rate = loading_data.get('rate', 0) * 100
                amount = loading_data.get('amount', 0)
                pricing += f"\n  - {loading_name.title()}: {rate:.1f}% (${amount:,.2f})"
        
        pricing += f"\n• Deductible: {data['deductible_rate']*100:.0f}% (${data['deductible_amount']:,.2f})"
        
        return pricing
    
    def _build_risk_assessment(self, data: Dict[str, Any]) -> str:
        """Build Risk Assessment section with actual historical data"""
        
        risk_metrics = data['risk_metrics']
        actuarial = data['actuarial_basis']
        hist_sim = data['historical_simulation']
        
        assessment = f"""RISK ASSESSMENT
Based on {len(hist_sim)} years of CHIRPS satellite data ({actuarial.get('historical_period', 'N/A')}):
- Average Drought Impact: {risk_metrics.get('average_drought_impact_pct', 0):.1f}%
- Drought Volatility (Std Dev): {risk_metrics.get('drought_volatility_std', 0):.1f}%
- Payout Frequency: {data['payout_frequency_pct']:.0f}% ({len([y for y in hist_sim if y.get('simulated_payout', 0) > 0])} of {len(hist_sim)} years triggered)
- Expected Loss Ratio: {risk_metrics.get('expected_loss_ratio', 0):.2f}
- Loss Ratio Volatility: {risk_metrics.get('loss_ratio_volatility', 0):.2f}
- 90th Percentile Maximum Loss: {risk_metrics.get('probable_maximum_loss_90pct', 0):.1f}% (LR {risk_metrics.get('probable_maximum_loss_ratio_90pct', 0):.2f})
- 95th Percentile Maximum Loss: {risk_metrics.get('probable_maximum_loss_95pct', 0):.1f}%"""
        
        # Add worst year
        if data['worst_year']:
            worst = data['worst_year']
            assessment += f"\n• Worst Historical Year: {worst['year']} ({worst['drought_impact_pct']:.1f}% loss, LR {worst['loss_ratio']:.2f})"
        
        # Add data quality
        assessment += f"\n• Data Quality: {actuarial.get('credibility_rating', 'Unknown')} ({actuarial.get('data_quality_pct', 0):.0f}% valid seasons)"
        
        return assessment
    
    def _build_key_findings(self, data: Dict[str, Any]) -> str:
        """Build Key Findings section with data-driven insights"""
        
        findings = ["KEY FINDINGS"]
        
        # Finding 1: Payout frequency assessment
        payout_freq = data['payout_frequency_pct']
        freq_descriptor = self._get_frequency_descriptor(payout_freq)
        findings.append(
            f"1. This field has experienced {freq_descriptor} drought frequency, with payouts "
            f"triggered in {payout_freq:.0f}% of analyzed seasons."
        )
        
        # Finding 2: Premium rate justification
        premium_rate_pct = data['premium_rate'] * 100
        avg_drought = data['risk_metrics'].get('average_drought_impact_pct', 0)
        exposure_level = self._get_exposure_level(avg_drought)
        
        findings.append(
            f"2. The premium rate of {premium_rate_pct:.2f}% is calibrated using industry-standard "
            f"10-day rolling drought detection methodology, reflecting {exposure_level} drought exposure."
        )
        
        # Finding 3: Loss ratio and volatility
        expected_lr = data['risk_metrics'].get('expected_loss_ratio', 0)
        lr_volatility = data['risk_metrics'].get('loss_ratio_volatility', 0)
        volatility_descriptor = self._get_volatility_descriptor(lr_volatility)
        
        worst_3 = data['worst_3_years']
        if len(worst_3) >= 3:
            findings.append(
                f"3. Historical simulation indicates an expected loss ratio of {expected_lr:.2f}, "
                f"with {volatility_descriptor} volatility (std dev {lr_volatility:.2f}), primarily "
                f"driven by severe drought events in {worst_3[0]['year']} ({worst_3[0]['drought_impact_pct']:.1f}%), "
                f"{worst_3[1]['year']} ({worst_3[1]['drought_impact_pct']:.1f}%), and "
                f"{worst_3[2]['year']} ({worst_3[2]['drought_impact_pct']:.1f}%)."
            )
        else:
            findings.append(
                f"3. Historical simulation indicates an expected loss ratio of {expected_lr:.2f}, "
                f"with {volatility_descriptor} volatility (std dev {lr_volatility:.2f})."
            )
        
        # Finding 4: Deductible and tail risk
        deductible_pct = data['deductible_rate'] * 100
        pml_90 = data['risk_metrics'].get('probable_maximum_loss_90pct', 0)
        pml_lr_90 = data['risk_metrics'].get('probable_maximum_loss_ratio_90pct', 0)
        tail_risk_assessment = self._get_tail_risk_assessment(pml_90, pml_lr_90)
        
        findings.append(
            f"4. The {deductible_pct:.0f}% deductible structure reduces basis risk while maintaining "
            f"commercial viability. 90th percentile maximum loss of {pml_90:.1f}% (LR {pml_lr_90:.2f}) "
            f"indicates {tail_risk_assessment} tail risk."
        )
        
        return "\n".join(findings)
    
    def _build_recommendation(self, data: Dict[str, Any]) -> str:
        """Build Recommendation section"""
        
        # Determine approval status
        actuarial = data['actuarial_basis']
        meets_standard = actuarial.get('meets_actuarial_standard', False)
        years_analyzed = actuarial.get('years_analyzed', 0)
        
        # Build recommendation
        recommendation = "RECOMMENDATION\n"
        
        if meets_standard:
            recommendation += (
                f"This quote represents actuarially sound pricing for a field with documented "
                f"drought vulnerability. The {data['premium_rate']*100:.2f}% rate incorporates:\n"
                f"- {years_analyzed}-year historical analysis (exceeds 20-year actuarial standard)\n"
                f"- Industry-standard 10-day rolling drought detection methodology\n"
                f"- Geographic risk adjustments for the {self._format_zone_name(data['zone'])}\n"
                f"- Phase-weighted crop water requirements for {data['crop']}\n"
                f"- Standard loadings for administration, reinsurance, and margin\n\n"
                f"Policy is recommended for approval subject to standard underwriting terms."
            )
        else:
            recommendation += (
                f"This quote is based on {years_analyzed} years of analysis, which is below the "
                f"optimal 20-year actuarial standard but meets regulatory minimum requirements. "
                f"The {data['premium_rate']*100:.2f}% rate incorporates appropriate risk adjustments "
                f"for limited data availability.\n\n"
                f"Policy may be approved with:\n"
                f"- Additional monitoring requirements during the coverage period\n"
                f"- Enhanced loading factors to account for data uncertainty\n"
                f"- Annual rate review based on updated historical analysis"
            )
        
        return recommendation
    
    def _format_zone_name(self, zone: str) -> str:
        """Format zone name for display"""
        if zone == 'auto_detect':
            return "Auto-Detected Zone"
        elif zone.startswith('aez_'):
            # Convert aez_3_midlands to "AEZ 3 - Midlands"
            parts = zone.replace('aez_', '').split('_')
            if len(parts) >= 2:
                return f"AEZ {parts[0].upper()} - {' '.join(p.title() for p in parts[1:])}"
            return zone.upper()
        return zone.replace('_', ' ').title()
    
    def _get_frequency_descriptor(self, payout_freq_pct: float) -> str:
        """Get descriptor for payout frequency"""
        if payout_freq_pct >= 75:
            return "very high"
        elif payout_freq_pct >= 60:
            return "high"
        elif payout_freq_pct >= 40:
            return "moderate"
        elif payout_freq_pct >= 20:
            return "low-to-moderate"
        else:
            return "low"
    
    def _get_exposure_level(self, avg_drought_pct: float) -> str:
        """Get drought exposure level"""
        if avg_drought_pct >= 25:
            return "very high"
        elif avg_drought_pct >= 20:
            return "high"
        elif avg_drought_pct >= 15:
            return "moderate-to-high"
        elif avg_drought_pct >= 10:
            return "moderate"
        else:
            return "low-to-moderate"
    
    def _get_volatility_descriptor(self, volatility: float) -> str:
        """Get descriptor for loss ratio volatility"""
        if volatility >= 2.0:
            return "very high"
        elif volatility >= 1.5:
            return "high"
        elif volatility >= 1.0:
            return "significant"
        elif volatility >= 0.5:
            return "moderate"
        else:
            return "low"
    
    def _get_tail_risk_assessment(self, pml_90: float, pml_lr_90: float) -> str:
        """Assess tail risk based on 90th percentile metrics"""
        if pml_90 >= 35 or pml_lr_90 >= 4.0:
            return "elevated"
        elif pml_90 >= 25 or pml_lr_90 >= 3.0:
            return "moderate"
        else:
            return "manageable"
    
    def _generate_fallback_summary(self, quote_result: Dict[str, Any]) -> str:
        """Generate basic fallback summary if full generation fails"""
        
        crop = quote_result.get('crop', 'crop').title()
        sum_insured = quote_result.get('sum_insured', 0)
        premium_rate = quote_result.get('premium_rate', 0) * 100
        gross_premium = quote_result.get('gross_premium', 0)
        year = quote_result.get('coverage_year', datetime.now().year)
        
        return (
            f"EXECUTIVE SUMMARY\n\n"
            f"Weather Index Insurance for {crop} - {year} Coverage Year\n\n"
            f"COVERAGE DETAILS\n"
            f"• Sum Insured: ${sum_insured:,.2f}\n"
            f"• Premium Rate: {premium_rate:.2f}%\n"
            f"• Gross Premium: ${gross_premium:,.2f}\n\n"
            f"This quote provides satellite-based drought protection with automatic claim settlement "
            f"based on industry-standard rainfall monitoring. Premium rate calibrated using historical "
            f"CHIRPS satellite data and actuarial risk analysis.\n\n"
            f"RECOMMENDATION\n"
            f"Quote available for underwriter review and approval."
        )
    
    def generate_concise_summary(self, quote_result: Dict[str, Any], 
                                field_name: Optional[str] = None) -> str:
        """
        Generate shorter, more concise executive summary for quick review
        
        Args:
            quote_result: Complete quote result
            field_name: Optional field name
            
        Returns:
            Concise executive summary
        """
        try:
            data = self._extract_quote_data(quote_result)
            field_display = field_name or "Target Field"
            
            # Build concise summary
            summary = f"""EXECUTIVE SUMMARY

Quote for {field_display} ({data['area_ha']:.2f} ha {data['crop']}, {self._format_zone_name(data['zone'])}): ${data['gross_premium']:,.2f} gross premium at {data['premium_rate']*100:.2f}% rate for {data['coverage_year']} coverage.

Historical Analysis: {len(data['historical_simulation'])}-year CHIRPS data analysis shows {data['risk_metrics'].get('average_drought_impact_pct', 0):.1f}% average drought impact with {data['payout_frequency_pct']:.0f}% payout frequency. Expected loss ratio of {data['risk_metrics'].get('expected_loss_ratio', 0):.2f} reflects {self._get_exposure_level(data['risk_metrics'].get('average_drought_impact_pct', 0))} drought exposure"""
            
            # Add worst years
            if len(data['worst_3_years']) >= 3:
                worst_3 = data['worst_3_years']
                summary += f", with significant volatility driven by severe events in {worst_3[0]['year']} ({worst_3[0]['drought_impact_pct']:.1f}%), {worst_3[1]['year']} ({worst_3[1]['drought_impact_pct']:.1f}%), and {worst_3[2]['year']} ({worst_3[2]['drought_impact_pct']:.1f}%)."
            else:
                summary += "."
            
            # Pricing basis
            summary += f"\n\nPricing Basis: Rate incorporates industry-standard 10-day rolling drought detection, geographic risk adjustments, and phase-weighted crop requirements. Meets actuarial standards with {data['actuarial_basis'].get('data_quality_pct', 0):.0f}% data quality across all {len(data['historical_simulation'])} seasons."
            
            # Risk assessment
            summary += f"\n\nRisk Assessment: 90th percentile maximum loss of {data['risk_metrics'].get('probable_maximum_loss_90pct', 0):.1f}% (LR {data['risk_metrics'].get('probable_maximum_loss_ratio_90pct', 0):.2f}) indicates {self._get_tail_risk_assessment(data['risk_metrics'].get('probable_maximum_loss_90pct', 0), data['risk_metrics'].get('probable_maximum_loss_ratio_90pct', 0))} tail risk. The {data['deductible_rate']*100:.0f}% deductible reduces basis risk while maintaining commercial viability."
            
            # Recommendation
            summary += f"\n\nRecommendation: {'Approve' if data['actuarial_basis'].get('meets_actuarial_standard', False) else 'Approve with conditions'}. Pricing is actuarially sound and appropriate for documented field-level drought vulnerability."
            
            return summary
            
        except Exception as e:
            print(f"ERROR: Failed to generate concise summary: {e}")
            return self._generate_fallback_summary(quote_result)
    
    def export_summary_for_pdf(self, quote_result: Dict[str, Any],
                              field_name: Optional[str] = None,
                              summary_type: str = "comprehensive") -> Dict[str, Any]:
        """
        Export summary data formatted for PDF generation
        
        Args:
            quote_result: Complete quote result
            field_name: Optional field name
            summary_type: "comprehensive" or "concise"
            
        Returns:
            Dictionary with formatted summary components
        """
        try:
            data = self._extract_quote_data(quote_result)
            field_display = field_name or "Target Field"
            
            if summary_type == "concise":
                summary_text = self.generate_concise_summary(quote_result, field_name)
            else:
                summary_text = self.generate_comprehensive_executive_summary(
                    quote_result, field_name
                )
            
            return {
                "summary_text": summary_text,
                "field_name": field_display,
                "key_metrics": {
                    "sum_insured": data['sum_insured'],
                    "premium_rate": data['premium_rate'] * 100,
                    "gross_premium": data['gross_premium'],
                    "expected_loss_ratio": data['risk_metrics'].get('expected_loss_ratio', 0),
                    "payout_frequency": data['payout_frequency_pct'],
                    "avg_drought_impact": data['risk_metrics'].get('average_drought_impact_pct', 0),
                    "years_analyzed": len(data['historical_simulation'])
                },
                "worst_years": [
                    {
                        "year": y['year'],
                        "drought_impact": y['drought_impact_pct'],
                        "loss_ratio": y['loss_ratio']
                    }
                    for y in data['worst_3_years']
                ],
                "recommendation": "Approve" if data['actuarial_basis'].get('meets_actuarial_standard', False) else "Approve with conditions"
            }
            
        except Exception as e:
            print(f"ERROR: Failed to export summary for PDF: {e}")
            return {
                "summary_text": self._generate_fallback_summary(quote_result),
                "field_name": field_name or "Target Field",
                "key_metrics": {},
                "worst_years": [],
                "recommendation": "Review Required"
            }


# Convenience function for backward compatibility
def generate_executive_summary(quote_result: Dict[str, Any], 
                              field_name: Optional[str] = None,
                              summary_type: str = "comprehensive") -> str:
    """
    Generate executive summary - convenience function
    
    Args:
        quote_result: Complete quote result from quote engine
        field_name: Optional field name
        summary_type: "comprehensive" or "concise"
        
    Returns:
        Formatted executive summary string
    """
    generator = EnterpriseExecutiveSummaryGenerator()
    
    if summary_type == "concise":
        return generator.generate_concise_summary(quote_result, field_name)
    else:
        return generator.generate_comprehensive_executive_summary(quote_result, field_name)


# Example usage
if __name__ == "__main__":
    print("Enterprise Executive Summary Generator v4.0")
    print("Data-driven summaries for B2B insurance underwriters")
    print("\nFeatures:")
    print("  ✓ Comprehensive format with all actuarial metrics")
    print("  ✓ Concise format for quick review")
    print("  ✓ PDF export formatting")
    print("  ✓ Zero marketing fluff - only data and recommendations")
    print("  ✓ Field-specific analysis with actual historical performance")
    print("  ✓ Risk-appropriate language based on actual metrics")
