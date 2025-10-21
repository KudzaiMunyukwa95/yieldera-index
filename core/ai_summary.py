"""
Enterprise-Grade Executive Summary Generator for Agricultural Index Insurance
HTML-formatted output for direct frontend display
Data-driven, specific, and decision-focused for B2B insurance underwriters
"""

from typing import Dict, Any, Optional, List
from datetime import datetime
import json


class EnterpriseExecutiveSummaryGenerator:
    """
    Generate professional, HTML-formatted executive summaries for insurance quotes
    Designed for underwriters, reinsurers, and enterprise decision-makers
    """
    
    def __init__(self):
        """Initialize with enterprise standards"""
        self.version = "4.0-Enterprise-HTML"
        self.standards = {
            "minimum_years_excellent": 25,
            "minimum_years_good": 20,
            "minimum_years_acceptable": 15,
            "high_payout_frequency": 60,
            "high_drought_impact": 20,
            "high_volatility": 1.0
        }
    
    def generate_comprehensive_executive_summary(
        self, 
        quote_result: Dict[str, Any],
        field_name: Optional[str] = None,
        location_context: Optional[Dict] = None
    ) -> str:
        """
        Generate complete executive summary formatted as HTML
        
        Args:
            quote_result: Complete quote result from quote engine
            field_name: Optional field name (e.g., "Combined", "Ward 15")
            location_context: Optional additional location info
            
        Returns:
            HTML-formatted executive summary string
        """
        try:
            # Extract all required data
            data = self._extract_quote_data(quote_result)
            
            # Determine field name
            field_display = field_name or (location_context.get('name') if location_context else "Target Field")
            
            # Build HTML summary
            html = '<div class="executive-summary-content space-y-4">'
            
            # Coverage Overview
            html += self._build_html_coverage_overview(data, field_display)
            
            # Pricing Recommendation
            html += self._build_html_pricing_recommendation(data)
            
            # Risk Assessment
            html += self._build_html_risk_assessment(data)
            
            # Key Findings
            html += self._build_html_key_findings(data)
            
            # Recommendation
            html += self._build_html_recommendation(data)
            
            html += '</div>'
            
            return html
            
        except Exception as e:
            print(f"ERROR: Failed to generate executive summary: {e}")
            return self._generate_fallback_html_summary(quote_result)
    
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
    
    def _build_html_coverage_overview(self, data: Dict[str, Any], field_name: str) -> str:
        """Build Coverage Overview section as HTML"""
        
        zone_display = self._format_zone_name(data['zone'])
        
        html = f'''
        <div class="summary-section">
            <h3 class="text-base font-semibold text-gray-900 dark:text-white mb-3 uppercase tracking-wide">Coverage Overview</h3>
            <div class="grid grid-cols-1 md:grid-cols-2 gap-2 text-sm">
                <div class="flex justify-between">
                    <span class="text-gray-600 dark:text-gray-400 font-medium">Field:</span>
                    <span class="text-gray-900 dark:text-white">{field_name} ({data['area_ha']:.2f} ha)</span>
                </div>
                <div class="flex justify-between">
                    <span class="text-gray-600 dark:text-gray-400 font-medium">Crop:</span>
                    <span class="text-gray-900 dark:text-white">{data['crop']}</span>
                </div>
                <div class="flex justify-between">
                    <span class="text-gray-600 dark:text-gray-400 font-medium">Coverage Period:</span>
                    <span class="text-gray-900 dark:text-white">{data['coverage_year']} Season</span>
                </div>
                <div class="flex justify-between">
                    <span class="text-gray-600 dark:text-gray-400 font-medium">Sum Insured:</span>
                    <span class="text-gray-900 dark:text-white font-semibold">${data['sum_insured']:,.2f}</span>
                </div>
                <div class="flex justify-between md:col-span-2">
                    <span class="text-gray-600 dark:text-gray-400 font-medium">Location:</span>
                    <span class="text-gray-900 dark:text-white">Lat {data['latitude']:.4f}, Long {data['longitude']:.4f} ({zone_display})</span>
                </div>
            </div>
        </div>
        '''
        
        return html
    
    def _build_html_pricing_recommendation(self, data: Dict[str, Any]) -> str:
        """Build Pricing Recommendation section as HTML"""
        
        html = f'''
        <div class="summary-section">
            <h3 class="text-base font-semibold text-gray-900 dark:text-white mb-3 uppercase tracking-wide">Pricing Recommendation</h3>
            <div class="space-y-2">
                <div class="grid grid-cols-2 gap-4 text-sm">
                    <div class="flex justify-between">
                        <span class="text-gray-600 dark:text-gray-400 font-medium">Premium Rate:</span>
                        <span class="text-gray-900 dark:text-white font-semibold">{data['premium_rate']*100:.2f}%</span>
                    </div>
                    <div class="flex justify-between">
                        <span class="text-gray-600 dark:text-gray-400 font-medium">Gross Premium:</span>
                        <span class="text-primary font-bold">${data['gross_premium']:,.2f}</span>
                    </div>
                    <div class="flex justify-between">
                        <span class="text-gray-600 dark:text-gray-400 font-medium">Burning Cost:</span>
                        <span class="text-gray-900 dark:text-white">${data['burning_cost']:,.2f}</span>
                    </div>
                    <div class="flex justify-between">
                        <span class="text-gray-600 dark:text-gray-400 font-medium">Total Loadings:</span>
                        <span class="text-gray-900 dark:text-white">${data['total_loadings']:,.2f}</span>
                    </div>
                </div>
        '''
        
        # Add loadings breakdown if available
        if data['loadings_breakdown']:
            html += '''
                <div class="mt-3 pt-3 border-t border-gray-200 dark:border-gray-700">
                    <div class="text-xs font-medium text-gray-700 dark:text-gray-300 mb-2">Loadings Breakdown:</div>
                    <div class="grid grid-cols-2 gap-2 text-xs">
            '''
            
            for loading_name, loading_data in data['loadings_breakdown'].items():
                rate = loading_data.get('rate', 0) * 100
                amount = loading_data.get('amount', 0)
                html += f'''
                        <div class="flex justify-between pl-3">
                            <span class="text-gray-600 dark:text-gray-400">{loading_name.title()}:</span>
                            <span class="text-gray-900 dark:text-white">{rate:.1f}% (${amount:,.2f})</span>
                        </div>
                '''
            
            html += '''
                    </div>
                </div>
            '''
        
        html += f'''
                <div class="flex justify-between text-sm pt-2">
                    <span class="text-gray-600 dark:text-gray-400 font-medium">Deductible:</span>
                    <span class="text-gray-900 dark:text-white">{data['deductible_rate']*100:.0f}% (${data['deductible_amount']:,.2f})</span>
                </div>
            </div>
        </div>
        '''
        
        return html
    
    def _build_html_risk_assessment(self, data: Dict[str, Any]) -> str:
        """Build Risk Assessment section as HTML"""
        
        risk_metrics = data['risk_metrics']
        actuarial = data['actuarial_basis']
        hist_sim = data['historical_simulation']
        
        years_with_payout = len([y for y in hist_sim if y.get('simulated_payout', 0) > 0])
        
        html = f'''
        <div class="summary-section">
            <h3 class="text-base font-semibold text-gray-900 dark:text-white mb-3 uppercase tracking-wide">Risk Assessment</h3>
            <div class="text-xs text-gray-600 dark:text-gray-400 mb-3">
                Based on {len(hist_sim)} years of CHIRPS satellite data ({actuarial.get('historical_period', 'N/A')}):
            </div>
            <div class="grid grid-cols-1 md:grid-cols-2 gap-2 text-sm">
                <div class="flex justify-between">
                    <span class="text-gray-600 dark:text-gray-400 font-medium">Average Drought Impact:</span>
                    <span class="text-gray-900 dark:text-white">{risk_metrics.get('average_drought_impact_pct', 0):.1f}%</span>
                </div>
                <div class="flex justify-between">
                    <span class="text-gray-600 dark:text-gray-400 font-medium">Drought Volatility:</span>
                    <span class="text-gray-900 dark:text-white">{risk_metrics.get('drought_volatility_std', 0):.1f}%</span>
                </div>
                <div class="flex justify-between">
                    <span class="text-gray-600 dark:text-gray-400 font-medium">Payout Frequency:</span>
                    <span class="text-gray-900 dark:text-white font-semibold">{data['payout_frequency_pct']:.0f}% ({years_with_payout} of {len(hist_sim)} years)</span>
                </div>
                <div class="flex justify-between">
                    <span class="text-gray-600 dark:text-gray-400 font-medium">Expected Loss Ratio:</span>
                    <span class="text-gray-900 dark:text-white font-semibold">{risk_metrics.get('expected_loss_ratio', 0):.2f}</span>
                </div>
                <div class="flex justify-between">
                    <span class="text-gray-600 dark:text-gray-400 font-medium">Loss Ratio Volatility:</span>
                    <span class="text-gray-900 dark:text-white">{risk_metrics.get('loss_ratio_volatility', 0):.2f}</span>
                </div>
                <div class="flex justify-between">
                    <span class="text-gray-600 dark:text-gray-400 font-medium">90th %ile PML:</span>
                    <span class="text-gray-900 dark:text-white">{risk_metrics.get('probable_maximum_loss_90pct', 0):.1f}% (LR {risk_metrics.get('probable_maximum_loss_ratio_90pct', 0):.2f})</span>
                </div>
        '''
        
        if data['worst_year']:
            worst = data['worst_year']
            html += f'''
                <div class="flex justify-between md:col-span-2 pt-2 border-t border-gray-200 dark:border-gray-700">
                    <span class="text-gray-600 dark:text-gray-400 font-medium">Worst Historical Year:</span>
                    <span class="text-red-600 dark:text-red-400 font-semibold">{worst['year']} ({worst['drought_impact_pct']:.1f}% loss, LR {worst['loss_ratio']:.2f})</span>
                </div>
            '''
        
        html += f'''
                <div class="flex justify-between md:col-span-2">
                    <span class="text-gray-600 dark:text-gray-400 font-medium">Data Quality:</span>
                    <span class="text-green-600 dark:text-green-400 font-medium">{actuarial.get('credibility_rating', 'Unknown')} ({actuarial.get('data_quality_pct', 0):.0f}% valid seasons)</span>
                </div>
            </div>
        </div>
        '''
        
        return html
    
    def _build_html_key_findings(self, data: Dict[str, Any]) -> str:
        """Build Key Findings section as HTML"""
        
        payout_freq = data['payout_frequency_pct']
        freq_descriptor = self._get_frequency_descriptor(payout_freq)
        
        premium_rate_pct = data['premium_rate'] * 100
        avg_drought = data['risk_metrics'].get('average_drought_impact_pct', 0)
        exposure_level = self._get_exposure_level(avg_drought)
        
        expected_lr = data['risk_metrics'].get('expected_loss_ratio', 0)
        lr_volatility = data['risk_metrics'].get('loss_ratio_volatility', 0)
        volatility_descriptor = self._get_volatility_descriptor(lr_volatility)
        
        worst_3 = data['worst_3_years']
        
        deductible_pct = data['deductible_rate'] * 100
        pml_90 = data['risk_metrics'].get('probable_maximum_loss_90pct', 0)
        pml_lr_90 = data['risk_metrics'].get('probable_maximum_loss_ratio_90pct', 0)
        tail_risk_assessment = self._get_tail_risk_assessment(pml_90, pml_lr_90)
        
        html = f'''
        <div class="summary-section">
            <h3 class="text-base font-semibold text-gray-900 dark:text-white mb-3 uppercase tracking-wide">Key Findings</h3>
            <div class="space-y-3 text-sm text-gray-700 dark:text-gray-300">
                <div class="flex items-start">
                    <span class="font-bold text-primary mr-2 flex-shrink-0">1.</span>
                    <span>This field has experienced <strong class="text-gray-900 dark:text-white">{freq_descriptor} drought frequency</strong>, with payouts triggered in <strong class="text-gray-900 dark:text-white">{payout_freq:.0f}%</strong> of analyzed seasons.</span>
                </div>
                
                <div class="flex items-start">
                    <span class="font-bold text-primary mr-2 flex-shrink-0">2.</span>
                    <span>The premium rate of <strong class="text-gray-900 dark:text-white">{premium_rate_pct:.2f}%</strong> is calibrated using industry-standard 10-day rolling drought detection methodology, reflecting <strong class="text-gray-900 dark:text-white">{exposure_level}</strong> drought exposure.</span>
                </div>
                
                <div class="flex items-start">
                    <span class="font-bold text-primary mr-2 flex-shrink-0">3.</span>
                    <span>Historical simulation indicates an expected loss ratio of <strong class="text-gray-900 dark:text-white">{expected_lr:.2f}</strong>, with <strong class="text-gray-900 dark:text-white">{volatility_descriptor} volatility</strong> (std dev {lr_volatility:.2f})'''
        
        if len(worst_3) >= 3:
            html += f''', primarily driven by severe drought events in <strong class="text-red-600 dark:text-red-400">{worst_3[0]['year']}</strong> ({worst_3[0]['drought_impact_pct']:.1f}%), <strong class="text-red-600 dark:text-red-400">{worst_3[1]['year']}</strong> ({worst_3[1]['drought_impact_pct']:.1f}%), and <strong class="text-red-600 dark:text-red-400">{worst_3[2]['year']}</strong> ({worst_3[2]['drought_impact_pct']:.1f}%)'''
        
        html += '''.</span>
                </div>
                
                <div class="flex items-start">
                    <span class="font-bold text-primary mr-2 flex-shrink-0">4.</span>
        '''
        
        html += f'''<span>The <strong class="text-gray-900 dark:text-white">{deductible_pct:.0f}%</strong> deductible structure reduces basis risk while maintaining commercial viability. 90th percentile maximum loss of <strong class="text-gray-900 dark:text-white">{pml_90:.1f}%</strong> (LR {pml_lr_90:.2f}) indicates <strong class="text-gray-900 dark:text-white">{tail_risk_assessment} tail risk</strong>.</span>
                </div>
            </div>
        </div>
        '''
        
        return html
    
    def _build_html_recommendation(self, data: Dict[str, Any]) -> str:
        """Build Recommendation section as HTML"""
        
        actuarial = data['actuarial_basis']
        meets_standard = actuarial.get('meets_actuarial_standard', False)
        years_analyzed = actuarial.get('years_analyzed', 0)
        
        html = '''
        <div class="summary-section">
            <h3 class="text-base font-semibold text-gray-900 dark:text-white mb-3 uppercase tracking-wide">Recommendation</h3>
        '''
        
        if meets_standard:
            html += f'''
            <div class="space-y-3 text-sm text-gray-700 dark:text-gray-300">
                <p class="leading-relaxed">
                    This quote represents <strong class="text-gray-900 dark:text-white">actuarially sound pricing</strong> for a field with documented drought vulnerability. The <strong class="text-primary">{data['premium_rate']*100:.2f}%</strong> rate incorporates:
                </p>
                <ul class="space-y-1 ml-5 list-disc">
                    <li>{years_analyzed}-year historical analysis (exceeds 20-year actuarial standard)</li>
                    <li>Industry-standard 10-day rolling drought detection methodology</li>
                    <li>Geographic risk adjustments for the {self._format_zone_name(data['zone'])}</li>
                    <li>Phase-weighted crop water requirements for {data['crop']}</li>
                    <li>Standard loadings for administration, reinsurance, and margin</li>
                </ul>
                <p class="pt-2 leading-relaxed">
                    <strong class="text-green-600 dark:text-green-400">Policy is recommended for approval</strong> subject to standard underwriting terms.
                </p>
            </div>
            '''
        else:
            html += f'''
            <div class="space-y-3 text-sm text-gray-700 dark:text-gray-300">
                <p class="leading-relaxed">
                    This quote is based on <strong class="text-gray-900 dark:text-white">{years_analyzed} years</strong> of analysis, which is below the optimal 20-year actuarial standard but meets regulatory minimum requirements. The <strong class="text-primary">{data['premium_rate']*100:.2f}%</strong> rate incorporates appropriate risk adjustments for limited data availability.
                </p>
                <p class="leading-relaxed">
                    <strong class="text-yellow-600 dark:text-yellow-400">Policy may be approved with:</strong>
                </p>
                <ul class="space-y-1 ml-5 list-disc">
                    <li>Additional monitoring requirements during the coverage period</li>
                    <li>Enhanced loading factors to account for data uncertainty</li>
                    <li>Annual rate review based on updated historical analysis</li>
                </ul>
            </div>
            '''
        
        html += '''
        </div>
        '''
        
        return html
    
    def _format_zone_name(self, zone: str) -> str:
        """Format zone name for display"""
        if zone == 'auto_detect':
            return "Auto-Detected Zone"
        elif zone.startswith('aez_'):
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
    
    def _generate_fallback_html_summary(self, quote_result: Dict[str, Any]) -> str:
        """Generate basic fallback summary as HTML if full generation fails"""
        
        crop = quote_result.get('crop', 'crop').title()
        sum_insured = quote_result.get('sum_insured', 0)
        premium_rate = quote_result.get('premium_rate', 0) * 100
        gross_premium = quote_result.get('gross_premium', 0)
        year = quote_result.get('coverage_year', datetime.now().year)
        
        return f'''
        <div class="executive-summary-content space-y-4">
            <div class="summary-section">
                <h3 class="text-base font-semibold text-gray-900 dark:text-white mb-3">Executive Summary</h3>
                <p class="text-sm text-gray-700 dark:text-gray-300 mb-3">
                    Weather Index Insurance for <strong>{crop}</strong> - {year} Coverage Year
                </p>
                <div class="grid grid-cols-2 gap-2 text-sm">
                    <div class="flex justify-between">
                        <span class="text-gray-600 dark:text-gray-400">Sum Insured:</span>
                        <span class="text-gray-900 dark:text-white">${sum_insured:,.2f}</span>
                    </div>
                    <div class="flex justify-between">
                        <span class="text-gray-600 dark:text-gray-400">Premium Rate:</span>
                        <span class="text-gray-900 dark:text-white">{premium_rate:.2f}%</span>
                    </div>
                    <div class="flex justify-between md:col-span-2">
                        <span class="text-gray-600 dark:text-gray-400">Gross Premium:</span>
                        <span class="text-primary font-bold">${gross_premium:,.2f}</span>
                    </div>
                </div>
                <p class="text-sm text-gray-700 dark:text-gray-300 mt-3">
                    This quote provides satellite-based drought protection with automatic claim settlement based on industry-standard rainfall monitoring.
                </p>
            </div>
        </div>
        '''
    
    def generate_concise_summary(self, quote_result: Dict[str, Any], 
                                field_name: Optional[str] = None) -> str:
        """
        Generate shorter, concise executive summary as HTML for bulk quotes
        
        Args:
            quote_result: Complete quote result
            field_name: Optional field name
            
        Returns:
            Concise HTML-formatted executive summary
        """
        try:
            data = self._extract_quote_data(quote_result)
            field_display = field_name or "Target Field"
            
            html = f'''
            <div class="executive-summary-content space-y-3">
                <p class="text-sm text-gray-700 dark:text-gray-300 leading-relaxed">
                    Quote for <strong class="text-gray-900 dark:text-white">{field_display}</strong> ({data['area_ha']:.2f} ha {data['crop']}): 
                    <strong class="text-primary">${data['gross_premium']:,.2f}</strong> gross premium at 
                    <strong class="text-gray-900 dark:text-white">{data['premium_rate']*100:.2f}%</strong> rate for {data['coverage_year']} coverage.
                </p>
                
                <p class="text-sm text-gray-700 dark:text-gray-300 leading-relaxed">
                    <strong class="text-gray-900 dark:text-white">Risk Assessment:</strong> {len(data['historical_simulation'])}-year analysis shows 
                    <strong>{data['risk_metrics'].get('average_drought_impact_pct', 0):.1f}%</strong> average drought impact with 
                    <strong>{data['payout_frequency_pct']:.0f}%</strong> payout frequency. Expected loss ratio: 
                    <strong>{data['risk_metrics'].get('expected_loss_ratio', 0):.2f}</strong>.
            '''
            
            if len(data['worst_3_years']) >= 3:
                worst_3 = data['worst_3_years']
                html += f''' Severe events in <strong class="text-red-600 dark:text-red-400">{worst_3[0]['year']}</strong> ({worst_3[0]['drought_impact_pct']:.1f}%), 
                    <strong class="text-red-600 dark:text-red-400">{worst_3[1]['year']}</strong> ({worst_3[1]['drought_impact_pct']:.1f}
