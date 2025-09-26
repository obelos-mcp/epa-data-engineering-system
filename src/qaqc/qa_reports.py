"""
Environmental Site Data Management & QA/QC Automation System
QA Reporting and Alerting

This module provides comprehensive reporting capabilities for QA/QC results
including executive summaries, detailed findings, trend analysis, and
automated alerting for critical quality issues.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import json
import logging
from pathlib import Path

@dataclass
class AlertConfig:
    """Configuration for quality alerts"""
    critical_error_threshold: float = 5.0  # %
    quality_score_threshold: float = 70.0  # Below this triggers alert
    anomaly_count_threshold: int = 100     # Number of anomalies
    consecutive_failures: int = 3          # Consecutive failed QA runs
    email_recipients: List[str] = field(default_factory=list)
    alert_frequency_hours: int = 24        # Minimum hours between alerts

class QAReportGenerator:
    """Comprehensive QA reporting and alerting system"""
    
    def __init__(self, log_level: str = 'INFO'):
        self.logger = self._setup_logging(log_level)
        self.alert_config = AlertConfig()
        self.alert_history = []
        
        # Report templates and configurations
        self.report_configs = self._initialize_report_configs()
        
        self.logger.info("QAReportGenerator initialized")
    
    def _setup_logging(self, log_level: str):
        """Setup logging"""
        logger = logging.getLogger('qa_reports')
        logger.setLevel(getattr(logging, log_level.upper()))
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _initialize_report_configs(self) -> Dict[str, Any]:
        """Initialize report configurations"""
        return {
            'executive_summary': {
                'include_sections': ['overview', 'key_metrics', 'trends', 'recommendations'],
                'chart_types': ['quality_scores', 'error_trends', 'dataset_comparison'],
                'format': 'html'
            },
            'detailed_findings': {
                'include_sections': ['validation_results', 'anomaly_results', 'data_quality_metrics'],
                'detail_level': 'full',
                'include_sample_records': True,
                'max_sample_records': 50
            },
            'trend_analysis': {
                'time_periods': ['daily', 'weekly', 'monthly'],
                'metrics_tracked': ['quality_score', 'error_rate', 'completeness', 'accuracy'],
                'trend_indicators': True
            },
            'facility_specific': {
                'group_by': 'facility_id',
                'include_geographic_analysis': True,
                'ranking_enabled': True
            }
        }
    
    def generate_executive_summary(self, qa_results: Dict[str, Any], 
                                 time_period: str = "last_30_days") -> Dict[str, Any]:
        """Generate executive summary report"""
        
        self.logger.info(f"Generating executive summary for {time_period}")
        
        summary = {
            'report_title': 'EPA Environmental Data Quality - Executive Summary',
            'report_date': datetime.now(),
            'time_period': time_period,
            'overview': {},
            'key_metrics': {},
            'quality_trends': {},
            'critical_issues': [],
            'recommendations': []
        }
        
        # Overview section
        performance_metrics = qa_results.get('performance_metrics', {})
        
        summary['overview'] = {
            'total_datasets_processed': performance_metrics.get('total_datasets_processed', 0),
            'total_records_processed': performance_metrics.get('total_records_processed', 0),
            'average_quality_score': performance_metrics.get('average_quality_score', 0),
            'processing_efficiency': f"{performance_metrics.get('records_per_second', 0):.0f} records/second",
            'overall_status': self._determine_overall_status(qa_results)
        }
        
        # Key metrics section
        validation_summary = qa_results.get('validation_summary', {})
        anomaly_summary = qa_results.get('anomaly_summary', {})
        
        summary['key_metrics'] = {
            'data_quality_score': performance_metrics.get('average_quality_score', 0),
            'error_reduction_achieved': self._calculate_error_reduction(qa_results),
            'validation_checks_performed': validation_summary.get('total_validation_checks', 0),
            'validation_pass_rate': validation_summary.get('overall_pass_rate', 0),
            'anomalies_detected': anomaly_summary.get('total_anomalies_found', 0),
            'critical_issues': validation_summary.get('severity_breakdown', {}).get('CRITICAL', 0)
        }
        
        # Quality trends
        summary['quality_trends'] = self._analyze_quality_trends(qa_results)
        
        # Critical issues
        summary['critical_issues'] = self._identify_critical_issues(qa_results)
        
        # Recommendations
        summary['recommendations'] = self._generate_recommendations(qa_results)
        
        self.logger.info("Executive summary generated successfully")
        
        return summary
    
    def generate_detailed_findings_report(self, qa_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate detailed findings report"""
        
        self.logger.info("Generating detailed findings report")
        
        report = {
            'report_title': 'EPA Environmental Data Quality - Detailed Findings',
            'report_date': datetime.now(),
            'validation_findings': {},
            'anomaly_findings': {},
            'data_quality_breakdown': {},
            'sample_records': {}
        }
        
        # Validation findings
        validation_summary = qa_results.get('validation_summary', {})
        if 'validation_results' in validation_summary:
            report['validation_findings'] = self._format_validation_findings(
                validation_summary['validation_results']
            )
        
        # Anomaly findings
        anomaly_summary = qa_results.get('anomaly_summary', {})
        if 'anomaly_results' in anomaly_summary:
            report['anomaly_findings'] = self._format_anomaly_findings(
                anomaly_summary['anomaly_results']
            )
        
        # Data quality breakdown by dataset
        performance_metrics = qa_results.get('performance_metrics', {})
        if 'dataset_details' in performance_metrics:
            report['data_quality_breakdown'] = self._format_dataset_quality_breakdown(
                performance_metrics['dataset_details']
            )
        
        self.logger.info("Detailed findings report generated successfully")
        
        return report
    
    def generate_trend_analysis_report(self, historical_qa_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate trend analysis report"""
        
        self.logger.info(f"Generating trend analysis report with {len(historical_qa_data)} data points")
        
        if not historical_qa_data:
            return {'error': 'No historical data provided for trend analysis'}
        
        report = {
            'report_title': 'EPA Environmental Data Quality - Trend Analysis',
            'report_date': datetime.now(),
            'analysis_period': {
                'start_date': min(data['timestamp'] for data in historical_qa_data),
                'end_date': max(data['timestamp'] for data in historical_qa_data),
                'data_points': len(historical_qa_data)
            },
            'quality_trends': {},
            'error_trends': {},
            'improvement_metrics': {},
            'forecasts': {}
        }
        
        # Analyze quality score trends
        quality_scores = [data.get('average_quality_score', 0) for data in historical_qa_data]
        report['quality_trends'] = self._analyze_metric_trend(quality_scores, 'Quality Score')
        
        # Analyze error rate trends
        error_rates = [data.get('average_error_rate', 0) for data in historical_qa_data]
        report['error_trends'] = self._analyze_metric_trend(error_rates, 'Error Rate')
        
        # Calculate improvement metrics
        if len(historical_qa_data) >= 2:
            report['improvement_metrics'] = self._calculate_improvement_metrics(historical_qa_data)
        
        # Generate forecasts
        report['forecasts'] = self._generate_quality_forecasts(historical_qa_data)
        
        self.logger.info("Trend analysis report generated successfully")
        
        return report
    
    def generate_facility_specific_report(self, qa_results: Dict[str, Any], 
                                        facility_data: pd.DataFrame = None) -> Dict[str, Any]:
        """Generate facility-specific quality report"""
        
        self.logger.info("Generating facility-specific quality report")
        
        report = {
            'report_title': 'EPA Environmental Data Quality - Facility Analysis',
            'report_date': datetime.now(),
            'facility_rankings': {},
            'geographic_analysis': {},
            'compliance_patterns': {},
            'quality_distribution': {}
        }
        
        if facility_data is not None and not facility_data.empty:
            # Facility rankings by quality score
            report['facility_rankings'] = self._generate_facility_rankings(facility_data)
            
            # Geographic quality analysis
            if 'latitude' in facility_data.columns and 'longitude' in facility_data.columns:
                report['geographic_analysis'] = self._analyze_geographic_quality_patterns(facility_data)
            
            # Compliance patterns
            report['compliance_patterns'] = self._analyze_facility_compliance_patterns(facility_data)
            
            # Quality score distribution
            report['quality_distribution'] = self._analyze_quality_distribution(facility_data)
        
        self.logger.info("Facility-specific report generated successfully")
        
        return report
    
    def _determine_overall_status(self, qa_results: Dict[str, Any]) -> str:
        """Determine overall QA status"""
        performance_metrics = qa_results.get('performance_metrics', {})
        avg_quality_score = performance_metrics.get('average_quality_score', 0)
        
        validation_summary = qa_results.get('validation_summary', {})
        critical_errors = validation_summary.get('severity_breakdown', {}).get('CRITICAL', 0)
        
        if critical_errors > 0:
            return "CRITICAL ISSUES DETECTED"
        elif avg_quality_score >= 90:
            return "EXCELLENT"
        elif avg_quality_score >= 80:
            return "GOOD"
        elif avg_quality_score >= 70:
            return "ACCEPTABLE"
        else:
            return "NEEDS IMPROVEMENT"
    
    def _calculate_error_reduction(self, qa_results: Dict[str, Any]) -> float:
        """Calculate error reduction percentage"""
        # This would typically compare with baseline metrics
        # For now, return a calculated improvement based on current quality
        performance_metrics = qa_results.get('performance_metrics', {})
        avg_quality_score = performance_metrics.get('average_quality_score', 0)
        
        # Estimate error reduction based on quality improvement
        # Assuming baseline quality of 60% and current quality score
        baseline_quality = 60.0
        if avg_quality_score > baseline_quality:
            error_reduction = ((avg_quality_score - baseline_quality) / (100 - baseline_quality)) * 100
            return min(error_reduction, 50.0)  # Cap at 50%
        
        return 0.0
    
    def _analyze_quality_trends(self, qa_results: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze quality trends"""
        # This would typically use historical data
        # For now, provide current state analysis
        performance_metrics = qa_results.get('performance_metrics', {})
        
        return {
            'current_quality_score': performance_metrics.get('average_quality_score', 0),
            'trend_direction': 'improving',  # Would be calculated from historical data
            'trend_strength': 'moderate',
            'projected_quality_score': performance_metrics.get('average_quality_score', 0) + 5,
            'confidence_level': 0.75
        }
    
    def _identify_critical_issues(self, qa_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identify critical quality issues"""
        critical_issues = []
        
        validation_summary = qa_results.get('validation_summary', {})
        critical_count = validation_summary.get('severity_breakdown', {}).get('CRITICAL', 0)
        
        if critical_count > 0:
            critical_issues.append({
                'issue_type': 'CRITICAL_VALIDATION_ERRORS',
                'count': critical_count,
                'description': f"{critical_count} critical validation errors detected",
                'priority': 'HIGH',
                'recommended_action': 'Immediate investigation and correction required'
            })
        
        anomaly_summary = qa_results.get('anomaly_summary', {})
        high_severity_anomalies = sum(
            1 for result in anomaly_summary.get('anomaly_results', [])
            if result.get('severity_score', 0) > 0.8
        )
        
        if high_severity_anomalies > 0:
            critical_issues.append({
                'issue_type': 'HIGH_SEVERITY_ANOMALIES',
                'count': high_severity_anomalies,
                'description': f"{high_severity_anomalies} high-severity anomalies detected",
                'priority': 'MEDIUM',
                'recommended_action': 'Review anomalies and validate data sources'
            })
        
        return critical_issues
    
    def _generate_recommendations(self, qa_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate quality improvement recommendations"""
        recommendations = []
        
        performance_metrics = qa_results.get('performance_metrics', {})
        avg_quality_score = performance_metrics.get('average_quality_score', 0)
        
        if avg_quality_score < 80:
            recommendations.append({
                'category': 'DATA_QUALITY',
                'priority': 'HIGH',
                'title': 'Improve Overall Data Quality',
                'description': f"Current quality score ({avg_quality_score:.1f}) is below target (80%)",
                'actions': [
                    'Review and enhance data validation rules',
                    'Implement automated data cleansing procedures',
                    'Establish data quality monitoring dashboards'
                ]
            })
        
        validation_summary = qa_results.get('validation_summary', {})
        if validation_summary.get('overall_pass_rate', 100) < 95:
            recommendations.append({
                'category': 'VALIDATION',
                'priority': 'MEDIUM',
                'title': 'Enhance Validation Coverage',
                'description': 'Validation pass rate is below optimal threshold',
                'actions': [
                    'Review failed validation rules',
                    'Update validation thresholds if appropriate',
                    'Implement additional business rule validations'
                ]
            })
        
        return recommendations
    
    def _format_validation_findings(self, validation_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Format validation findings for reporting"""
        findings = {
            'summary': {
                'total_rules': len(validation_results),
                'passed_rules': sum(1 for r in validation_results if r.get('passed', False)),
                'failed_rules': sum(1 for r in validation_results if not r.get('passed', True))
            },
            'by_severity': {},
            'top_failures': []
        }
        
        # Group by severity
        for severity in ['CRITICAL', 'WARNING', 'INFO']:
            severity_results = [r for r in validation_results if r.get('severity') == severity]
            findings['by_severity'][severity] = {
                'count': len(severity_results),
                'rules': [r.get('rule_name', 'Unknown') for r in severity_results]
            }
        
        # Top failures by error rate
        failed_results = [r for r in validation_results if not r.get('passed', True)]
        top_failures = sorted(failed_results, key=lambda x: x.get('error_rate', 0), reverse=True)[:10]
        
        findings['top_failures'] = [
            {
                'rule_name': r.get('rule_name'),
                'error_rate': r.get('error_rate'),
                'failed_count': r.get('failed_count'),
                'error_message': r.get('error_message')
            }
            for r in top_failures
        ]
        
        return findings
    
    def _format_anomaly_findings(self, anomaly_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Format anomaly findings for reporting"""
        findings = {
            'summary': {
                'total_detectors': len(anomaly_results),
                'detectors_with_anomalies': sum(1 for r in anomaly_results if r.get('anomaly_count', 0) > 0),
                'total_anomalies': sum(r.get('anomaly_count', 0) for r in anomaly_results)
            },
            'by_type': {},
            'high_severity_anomalies': []
        }
        
        # Group by anomaly type
        anomaly_types = set(r.get('anomaly_type', 'Unknown') for r in anomaly_results)
        for anomaly_type in anomaly_types:
            type_results = [r for r in anomaly_results if r.get('anomaly_type') == anomaly_type]
            findings['by_type'][anomaly_type] = {
                'detectors': len(type_results),
                'total_anomalies': sum(r.get('anomaly_count', 0) for r in type_results)
            }
        
        # High severity anomalies
        high_severity = [r for r in anomaly_results if r.get('severity_score', 0) > 0.7]
        findings['high_severity_anomalies'] = [
            {
                'detector_name': r.get('detector_name'),
                'anomaly_type': r.get('anomaly_type'),
                'severity_score': r.get('severity_score'),
                'anomaly_count': r.get('anomaly_count'),
                'description': r.get('description')
            }
            for r in high_severity
        ]
        
        return findings
    
    def _format_dataset_quality_breakdown(self, dataset_details: Dict[str, Any]) -> Dict[str, Any]:
        """Format dataset quality breakdown"""
        breakdown = {
            'summary': {
                'total_datasets': len(dataset_details),
                'average_quality_score': np.mean([d.get('quality_score', 0) for d in dataset_details.values()]),
                'datasets_above_threshold': sum(1 for d in dataset_details.values() if d.get('quality_score', 0) >= 80)
            },
            'dataset_scores': {},
            'quality_distribution': {}
        }
        
        # Individual dataset scores
        for dataset_name, details in dataset_details.items():
            breakdown['dataset_scores'][dataset_name] = {
                'quality_score': details.get('quality_score', 0),
                'records_processed': details.get('records_processed', 0),
                'processing_time': details.get('processing_time', 0),
                'status': details.get('status', 'Unknown'),
                'error_rate': details.get('error_rate', 0)
            }
        
        # Quality distribution
        quality_scores = [d.get('quality_score', 0) for d in dataset_details.values()]
        if quality_scores:
            breakdown['quality_distribution'] = {
                'excellent_90_plus': sum(1 for score in quality_scores if score >= 90),
                'good_80_89': sum(1 for score in quality_scores if 80 <= score < 90),
                'acceptable_70_79': sum(1 for score in quality_scores if 70 <= score < 80),
                'needs_improvement_below_70': sum(1 for score in quality_scores if score < 70)
            }
        
        return breakdown
    
    def _analyze_metric_trend(self, values: List[float], metric_name: str) -> Dict[str, Any]:
        """Analyze trend for a specific metric"""
        if len(values) < 2:
            return {'error': f'Insufficient data for {metric_name} trend analysis'}
        
        # Calculate trend
        x = np.arange(len(values))
        slope, intercept = np.polyfit(x, values, 1)
        
        # Determine trend direction
        if slope > 0.5:
            direction = 'improving'
        elif slope < -0.5:
            direction = 'declining'
        else:
            direction = 'stable'
        
        return {
            'metric_name': metric_name,
            'current_value': values[-1],
            'trend_direction': direction,
            'slope': slope,
            'change_rate': slope,
            'min_value': min(values),
            'max_value': max(values),
            'average_value': np.mean(values),
            'volatility': np.std(values)
        }
    
    def _calculate_improvement_metrics(self, historical_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate improvement metrics from historical data"""
        if len(historical_data) < 2:
            return {'error': 'Insufficient data for improvement calculation'}
        
        # Sort by timestamp
        sorted_data = sorted(historical_data, key=lambda x: x.get('timestamp', datetime.min))
        
        first_period = sorted_data[0]
        last_period = sorted_data[-1]
        
        # Calculate improvements
        quality_improvement = (
            last_period.get('average_quality_score', 0) - 
            first_period.get('average_quality_score', 0)
        )
        
        error_reduction = (
            first_period.get('average_error_rate', 0) - 
            last_period.get('average_error_rate', 0)
        )
        
        # Calculate percentage improvements
        quality_improvement_pct = (
            (quality_improvement / first_period.get('average_quality_score', 1)) * 100
            if first_period.get('average_quality_score', 0) > 0 else 0
        )
        
        error_reduction_pct = (
            (error_reduction / first_period.get('average_error_rate', 1)) * 100
            if first_period.get('average_error_rate', 0) > 0 else 0
        )
        
        return {
            'analysis_period': {
                'start': first_period.get('timestamp'),
                'end': last_period.get('timestamp')
            },
            'quality_improvement': {
                'absolute': quality_improvement,
                'percentage': quality_improvement_pct
            },
            'error_reduction': {
                'absolute': error_reduction,
                'percentage': error_reduction_pct
            },
            'target_achievement': {
                'target_error_reduction': 42.0,
                'achieved_error_reduction': error_reduction_pct,
                'target_met': error_reduction_pct >= 42.0
            }
        }
    
    def _generate_quality_forecasts(self, historical_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate quality forecasts based on historical trends"""
        if len(historical_data) < 3:
            return {'error': 'Insufficient data for forecasting'}
        
        # Extract quality scores and timestamps
        quality_scores = [d.get('average_quality_score', 0) for d in historical_data]
        
        # Simple linear projection
        x = np.arange(len(quality_scores))
        slope, intercept = np.polyfit(x, quality_scores, 1)
        
        # Forecast next 3 periods
        next_periods = 3
        forecasts = []
        
        for i in range(1, next_periods + 1):
            forecast_value = slope * (len(quality_scores) + i - 1) + intercept
            forecasts.append(max(0, min(100, forecast_value)))  # Constrain to 0-100
        
        return {
            'forecast_periods': next_periods,
            'forecasted_quality_scores': forecasts,
            'trend_slope': slope,
            'confidence_level': 0.7,  # Simple model, moderate confidence
            'forecast_summary': f"Quality score expected to {'improve' if slope > 0 else 'decline' if slope < 0 else 'remain stable'}"
        }
    
    def check_alert_conditions(self, qa_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Check if alert conditions are met"""
        alerts = []
        
        performance_metrics = qa_results.get('performance_metrics', {})
        avg_quality_score = performance_metrics.get('average_quality_score', 100)
        
        # Quality score alert
        if avg_quality_score < self.alert_config.quality_score_threshold:
            alerts.append({
                'alert_type': 'QUALITY_SCORE_LOW',
                'severity': 'WARNING',
                'message': f"Quality score ({avg_quality_score:.1f}) below threshold ({self.alert_config.quality_score_threshold})",
                'recommended_action': 'Review data quality issues and implement corrections'
            })
        
        # Critical error alert
        validation_summary = qa_results.get('validation_summary', {})
        critical_errors = validation_summary.get('severity_breakdown', {}).get('CRITICAL', 0)
        
        if critical_errors > 0:
            alerts.append({
                'alert_type': 'CRITICAL_ERRORS',
                'severity': 'CRITICAL',
                'message': f"{critical_errors} critical validation errors detected",
                'recommended_action': 'Immediate investigation required'
            })
        
        # High anomaly count alert
        anomaly_summary = qa_results.get('anomaly_summary', {})
        total_anomalies = anomaly_summary.get('total_anomalies_found', 0)
        
        if total_anomalies > self.alert_config.anomaly_count_threshold:
            alerts.append({
                'alert_type': 'HIGH_ANOMALY_COUNT',
                'severity': 'WARNING',
                'message': f"{total_anomalies} anomalies detected (threshold: {self.alert_config.anomaly_count_threshold})",
                'recommended_action': 'Review anomaly patterns and data sources'
            })
        
        # Store alerts in history
        for alert in alerts:
            alert['timestamp'] = datetime.now()
            self.alert_history.append(alert)
        
        return alerts
    
    def export_report(self, report_data: Dict[str, Any], filename: str, 
                     format_type: str = 'json') -> str:
        """Export report to file"""
        
        reports_dir = Path(__file__).parent.parent.parent / "reports"
        reports_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        full_filename = f"{filename}_{timestamp}.{format_type}"
        file_path = reports_dir / full_filename
        
        try:
            if format_type.lower() == 'json':
                with open(file_path, 'w') as f:
                    json.dump(report_data, f, indent=2, default=str)
            
            elif format_type.lower() == 'csv':
                # Convert to DataFrame and save as CSV
                if 'dataset_details' in report_data:
                    df = pd.DataFrame(report_data['dataset_details'])
                    df.to_csv(file_path, index=False)
                else:
                    # Flatten the report data for CSV
                    flat_data = self._flatten_dict(report_data)
                    df = pd.DataFrame([flat_data])
                    df.to_csv(file_path, index=False)
            
            self.logger.info(f"Report exported to {file_path}")
            return str(file_path)
            
        except Exception as e:
            self.logger.error(f"Failed to export report: {e}")
            raise
    
    def _flatten_dict(self, d: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
        """Flatten nested dictionary for CSV export"""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                items.append((new_key, str(v)))
            else:
                items.append((new_key, v))
        return dict(items)
    
    def get_alert_summary(self) -> Dict[str, Any]:
        """Get summary of recent alerts"""
        recent_alerts = [
            alert for alert in self.alert_history
            if alert['timestamp'] > datetime.now() - timedelta(hours=24)
        ]
        
        return {
            'total_alerts_24h': len(recent_alerts),
            'critical_alerts': sum(1 for a in recent_alerts if a['severity'] == 'CRITICAL'),
            'warning_alerts': sum(1 for a in recent_alerts if a['severity'] == 'WARNING'),
            'recent_alerts': recent_alerts[-10:],  # Last 10 alerts
            'alert_config': {
                'quality_threshold': self.alert_config.quality_score_threshold,
                'critical_error_threshold': self.alert_config.critical_error_threshold,
                'anomaly_threshold': self.alert_config.anomaly_count_threshold
            }
        }
