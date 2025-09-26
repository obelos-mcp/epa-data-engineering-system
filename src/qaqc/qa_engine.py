"""
Environmental Site Data Management & QA/QC Automation System
QA Engine Orchestrator

This module orchestrates comprehensive quality assurance across all datasets,
integrating validation rules and anomaly detection with database storage
and performance monitoring.
"""

import pandas as pd
import numpy as np
import logging
import time
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

# Import project modules
import sys
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.qaqc.validation_rules import ValidationRulesEngine, ValidationResult, ValidationSeverity
from src.qaqc.anomaly_detection import AnomalyDetectionEngine, AnomalyResult
from src.database import get_database_manager, DatabaseConfig

class QAStatus(Enum):
    """QA processing status"""
    PASSED = "PASSED"
    WARNING = "WARNING" 
    FAILED = "FAILED"
    ERROR = "ERROR"

@dataclass
class QAThresholds:
    """QA processing thresholds"""
    critical_error_threshold: float = 5.0  # % - stops pipeline
    warning_error_threshold: float = 15.0  # % - logged but continues
    anomaly_severity_threshold: float = 0.7  # 0-1 scale
    max_validation_time_seconds: int = 300  # 5 minutes max per dataset
    
@dataclass
class QAExecutionStats:
    """Statistics for QA execution"""
    dataset_name: str
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    records_processed: int = 0
    validation_checks_run: int = 0
    anomaly_checks_run: int = 0
    critical_errors: int = 0
    warnings: int = 0
    anomalies_found: int = 0
    overall_status: QAStatus = QAStatus.PASSED
    processing_time_seconds: float = 0.0
    error_rate: float = 0.0
    quality_score: float = 100.0
    
    def finalize(self):
        """Finalize statistics calculation"""
        if self.end_time and self.start_time:
            self.processing_time_seconds = (self.end_time - self.start_time).total_seconds()
        
        total_checks = self.validation_checks_run + self.anomaly_checks_run
        if total_checks > 0:
            self.error_rate = ((self.critical_errors + self.warnings) / total_checks) * 100
            
        # Calculate quality score (100 - error_rate, minimum 0)
        self.quality_score = max(100 - self.error_rate, 0.0)

class QAEngineOrchestrator:
    """Main QA/QC orchestrator for systematic data validation"""
    
    def __init__(self, db_config: Optional[DatabaseConfig] = None, 
                 qa_thresholds: Optional[QAThresholds] = None,
                 log_level: str = 'INFO'):
        
        self.db_manager = get_database_manager(db_config)
        self.qa_thresholds = qa_thresholds or QAThresholds()
        
        # Initialize validation and anomaly detection engines
        self.validation_engine = ValidationRulesEngine(log_level)
        self.anomaly_engine = AnomalyDetectionEngine(log_level)
        
        # Setup logging
        self._setup_logging(log_level)
        
        # Execution tracking
        self.execution_stats: Dict[str, QAExecutionStats] = {}
        self.current_batch_id: Optional[int] = None
        
        self.logger.info("QAEngineOrchestrator initialized")
    
    def _setup_logging(self, log_level: str):
        """Setup comprehensive logging"""
        log_dir = Path(__file__).parent.parent.parent / "logs"
        log_dir.mkdir(exist_ok=True)
        
        # Create logger
        self.logger = logging.getLogger('qa_engine')
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # File handler
        log_file = log_dir / f"qa_engine_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(getattr(logging, log_level.upper()))
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def set_batch_id(self, batch_id: int):
        """Set current ETL batch ID for tracking"""
        self.current_batch_id = batch_id
        self.logger.info(f"QA Engine batch ID set to: {batch_id}")
    
    def run_comprehensive_qa(self, df: pd.DataFrame, dataset_name: str, 
                           table_type: str) -> Tuple[QAExecutionStats, List[Dict[str, Any]]]:
        """Run comprehensive QA including validation and anomaly detection"""
        
        self.logger.info(f"Starting comprehensive QA for {dataset_name} ({len(df)} records)")
        
        # Initialize execution stats
        stats = QAExecutionStats(dataset_name=dataset_name, records_processed=len(df))
        
        # Store findings for database insertion
        qa_findings = []
        
        try:
            # Clear previous results
            self.validation_engine.clear_results()
            self.anomaly_engine.clear_results()
            
            # Run validation rules
            validation_start = time.time()
            validation_results = self.validation_engine.run_all_validations(df, table_type)
            validation_time = time.time() - validation_start
            
            stats.validation_checks_run = len(validation_results)
            self.logger.info(f"Validation completed: {len(validation_results)} checks in {validation_time:.2f}s")
            
            # Process validation results
            for result in validation_results:
                finding = self._convert_validation_to_finding(result, dataset_name)
                qa_findings.append(finding)
                
                if result.severity == ValidationSeverity.CRITICAL:
                    stats.critical_errors += 1
                elif result.severity == ValidationSeverity.WARNING:
                    stats.warnings += 1
            
            # Run anomaly detection
            anomaly_start = time.time()
            anomaly_results = self.anomaly_engine.run_all_anomaly_detection(df, table_type)
            anomaly_time = time.time() - anomaly_start
            
            stats.anomaly_checks_run = len(anomaly_results)
            self.logger.info(f"Anomaly detection completed: {len(anomaly_results)} checks in {anomaly_time:.2f}s")
            
            # Process anomaly results
            for result in anomaly_results:
                if result.anomaly_count > 0:
                    finding = self._convert_anomaly_to_finding(result, dataset_name)
                    qa_findings.append(finding)
                    stats.anomalies_found += result.anomaly_count
            
            # Determine overall status
            stats.overall_status = self._determine_qa_status(stats)
            
            # Finalize stats
            stats.end_time = datetime.now()
            stats.finalize()
            
            # Store execution stats
            self.execution_stats[dataset_name] = stats
            
            self.logger.info(
                f"QA completed for {dataset_name}: "
                f"Status={stats.overall_status.value}, "
                f"Quality Score={stats.quality_score:.1f}, "
                f"Errors={stats.critical_errors}, "
                f"Warnings={stats.warnings}, "
                f"Anomalies={stats.anomalies_found}"
            )
            
            return stats, qa_findings
            
        except Exception as e:
            stats.end_time = datetime.now()
            stats.overall_status = QAStatus.ERROR
            self.logger.error(f"QA execution failed for {dataset_name}: {e}")
            raise
    
    def _convert_validation_to_finding(self, result: ValidationResult, dataset_name: str) -> Dict[str, Any]:
        """Convert validation result to QA finding for database storage"""
        severity_mapping = {
            ValidationSeverity.CRITICAL: 'CRITICAL',
            ValidationSeverity.WARNING: 'MEDIUM', 
            ValidationSeverity.INFO: 'LOW'
        }
        
        finding_type = 'BUSINESS_RULE' if 'business' in result.rule_name else 'DATA_VALIDATION'
        
        return {
            'batch_id': self.current_batch_id,
            'table_name': dataset_name,
            'column_name': result.details.get('column') if result.details else None,
            'finding_type': finding_type,
            'severity': severity_mapping.get(result.severity, 'LOW'),
            'finding_description': f"{result.rule_name}: {result.error_message}",
            'affected_records': result.failed_count,
            'sample_values': result.failed_records[:10] if result.failed_records else [],  # First 10 record IDs
            'resolution_status': 'OPEN',
            'found_date': datetime.now()
        }
    
    def _convert_anomaly_to_finding(self, result: AnomalyResult, dataset_name: str) -> Dict[str, Any]:
        """Convert anomaly result to QA finding for database storage"""
        severity = 'CRITICAL' if result.severity_score > 0.8 else 'MEDIUM' if result.severity_score > 0.4 else 'LOW'
        
        return {
            'batch_id': self.current_batch_id,
            'table_name': dataset_name,
            'column_name': result.details.get('column') if result.details else None,
            'finding_type': 'DATA_ANOMALY',
            'severity': severity,
            'finding_description': f"{result.detector_name}: {result.description}",
            'affected_records': result.anomaly_count,
            'sample_values': result.anomalous_records[:10] if result.anomalous_records else [],
            'resolution_status': 'OPEN',
            'found_date': datetime.now()
        }
    
    def _determine_qa_status(self, stats: QAExecutionStats) -> QAStatus:
        """Determine overall QA status based on thresholds"""
        total_checks = stats.validation_checks_run + stats.anomaly_checks_run
        
        if total_checks == 0:
            return QAStatus.ERROR
        
        critical_rate = (stats.critical_errors / total_checks) * 100
        warning_rate = (stats.warnings / total_checks) * 100
        
        if critical_rate > self.qa_thresholds.critical_error_threshold:
            return QAStatus.FAILED
        elif warning_rate > self.qa_thresholds.warning_error_threshold:
            return QAStatus.WARNING
        else:
            return QAStatus.PASSED
    
    def store_qa_findings(self, qa_findings: List[Dict[str, Any]]) -> int:
        """Store QA findings in database"""
        if not qa_findings:
            self.logger.info("No QA findings to store")
            return 0
        
        try:
            findings_df = pd.DataFrame(qa_findings)
            
            # Store in qa_findings table
            rows_inserted = self.db_manager.bulk_insert_dataframe(
                findings_df,
                'qa_findings',
                schema='core',
                if_exists='append'
            )
            
            self.logger.info(f"Stored {rows_inserted} QA findings in database")
            return rows_inserted
            
        except Exception as e:
            self.logger.error(f"Failed to store QA findings: {e}")
            raise
    
    def check_error_thresholds(self, stats: QAExecutionStats) -> Tuple[bool, str]:
        """Check if error thresholds are exceeded"""
        total_checks = stats.validation_checks_run + stats.anomaly_checks_run
        
        if total_checks == 0:
            return False, "No checks performed"
        
        critical_rate = (stats.critical_errors / total_checks) * 100
        
        if critical_rate > self.qa_thresholds.critical_error_threshold:
            message = (f"Critical error threshold exceeded: {critical_rate:.1f}% "
                      f"(threshold: {self.qa_thresholds.critical_error_threshold}%)")
            return False, message
        
        return True, f"Error thresholds passed (critical: {critical_rate:.1f}%)"
    
    def run_etl_stage_qa(self, df: pd.DataFrame, dataset_name: str, 
                        table_type: str, stage: str) -> Dict[str, Any]:
        """Run QA for specific ETL stage with appropriate checks"""
        
        self.logger.info(f"Running {stage} stage QA for {dataset_name}")
        
        stage_start_time = time.time()
        
        try:
            if stage.upper() == 'EXTRACT':
                # Focus on data completeness and format validation
                results = self._run_extract_stage_qa(df, dataset_name, table_type)
            elif stage.upper() == 'TRANSFORM':
                # Focus on transformation validation and business rules
                results = self._run_transform_stage_qa(df, dataset_name, table_type)
            elif stage.upper() == 'LOAD':
                # Focus on data integrity and relationship validation
                results = self._run_load_stage_qa(df, dataset_name, table_type)
            else:
                # Run comprehensive QA
                stats, findings = self.run_comprehensive_qa(df, dataset_name, table_type)
                results = {
                    'stage': stage,
                    'stats': stats,
                    'findings': findings,
                    'threshold_check': self.check_error_thresholds(stats)
                }
            
            stage_time = time.time() - stage_start_time
            results['processing_time'] = stage_time
            
            self.logger.info(f"{stage} stage QA completed for {dataset_name} in {stage_time:.2f}s")
            
            return results
            
        except Exception as e:
            self.logger.error(f"{stage} stage QA failed for {dataset_name}: {e}")
            raise
    
    def _run_extract_stage_qa(self, df: pd.DataFrame, dataset_name: str, table_type: str) -> Dict[str, Any]:
        """Run QA checks specific to Extract stage"""
        stats = QAExecutionStats(dataset_name=f"{dataset_name}_extract", records_processed=len(df))
        findings = []
        
        # Run basic validation rules (required fields, data types)
        validation_results = []
        
        # Required fields check
        required_result = self.validation_engine.validate_required_fields(df, table_type)
        validation_results.append(required_result)
        
        # Data type validation
        type_results = self.validation_engine.validate_data_types(df)
        validation_results.extend(type_results)
        
        # Process results
        for result in validation_results:
            finding = self._convert_validation_to_finding(result, f"{dataset_name}_extract")
            findings.append(finding)
            
            if result.severity == ValidationSeverity.CRITICAL:
                stats.critical_errors += 1
            elif result.severity == ValidationSeverity.WARNING:
                stats.warnings += 1
        
        stats.validation_checks_run = len(validation_results)
        stats.overall_status = self._determine_qa_status(stats)
        stats.end_time = datetime.now()
        stats.finalize()
        
        return {
            'stage': 'EXTRACT',
            'stats': stats,
            'findings': findings,
            'threshold_check': self.check_error_thresholds(stats)
        }
    
    def _run_transform_stage_qa(self, df: pd.DataFrame, dataset_name: str, table_type: str) -> Dict[str, Any]:
        """Run QA checks specific to Transform stage"""
        stats = QAExecutionStats(dataset_name=f"{dataset_name}_transform", records_processed=len(df))
        findings = []
        
        # Run business rules validation
        business_results = self.validation_engine.validate_business_rules(df)
        cross_field_results = self.validation_engine.validate_cross_field_rules(df)
        
        all_results = business_results + cross_field_results
        
        # Process results
        for result in all_results:
            finding = self._convert_validation_to_finding(result, f"{dataset_name}_transform")
            findings.append(finding)
            
            if result.severity == ValidationSeverity.CRITICAL:
                stats.critical_errors += 1
            elif result.severity == ValidationSeverity.WARNING:
                stats.warnings += 1
        
        # Run statistical outlier detection
        numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()[:3]  # Limit for performance
        if numeric_columns:
            outlier_results = self.anomaly_engine.detect_statistical_outliers(df, numeric_columns)
            
            for result in outlier_results:
                if result.anomaly_count > 0:
                    finding = self._convert_anomaly_to_finding(result, f"{dataset_name}_transform")
                    findings.append(finding)
                    stats.anomalies_found += result.anomaly_count
            
            stats.anomaly_checks_run = len(outlier_results)
        
        stats.validation_checks_run = len(all_results)
        stats.overall_status = self._determine_qa_status(stats)
        stats.end_time = datetime.now()
        stats.finalize()
        
        return {
            'stage': 'TRANSFORM',
            'stats': stats,
            'findings': findings,
            'threshold_check': self.check_error_thresholds(stats)
        }
    
    def _run_load_stage_qa(self, df: pd.DataFrame, dataset_name: str, table_type: str) -> Dict[str, Any]:
        """Run QA checks specific to Load stage"""
        stats = QAExecutionStats(dataset_name=f"{dataset_name}_load", records_processed=len(df))
        findings = []
        
        # Run comprehensive validation (all rules)
        validation_results = self.validation_engine.run_all_validations(df, table_type)
        
        # Process validation results
        for result in validation_results:
            finding = self._convert_validation_to_finding(result, f"{dataset_name}_load")
            findings.append(finding)
            
            if result.severity == ValidationSeverity.CRITICAL:
                stats.critical_errors += 1
            elif result.severity == ValidationSeverity.WARNING:
                stats.warnings += 1
        
        # Run compliance and consistency checks
        compliance_results = self.anomaly_engine.detect_compliance_anomalies(df)
        
        for result in compliance_results:
            if result.anomaly_count > 0:
                finding = self._convert_anomaly_to_finding(result, f"{dataset_name}_load")
                findings.append(finding)
                stats.anomalies_found += result.anomaly_count
        
        stats.validation_checks_run = len(validation_results)
        stats.anomaly_checks_run = len(compliance_results)
        stats.overall_status = self._determine_qa_status(stats)
        stats.end_time = datetime.now()
        stats.finalize()
        
        return {
            'stage': 'LOAD',
            'stats': stats,
            'findings': findings,
            'threshold_check': self.check_error_thresholds(stats)
        }
    
    def get_qa_performance_metrics(self) -> Dict[str, Any]:
        """Get QA processing performance metrics"""
        if not self.execution_stats:
            return {'error': 'No QA execution statistics available'}
        
        total_records = sum(stats.records_processed for stats in self.execution_stats.values())
        total_time = sum(stats.processing_time_seconds for stats in self.execution_stats.values())
        
        avg_quality_score = np.mean([stats.quality_score for stats in self.execution_stats.values()])
        avg_error_rate = np.mean([stats.error_rate for stats in self.execution_stats.values()])
        
        total_checks = sum(
            stats.validation_checks_run + stats.anomaly_checks_run 
            for stats in self.execution_stats.values()
        )
        
        return {
            'total_datasets_processed': len(self.execution_stats),
            'total_records_processed': total_records,
            'total_processing_time_seconds': total_time,
            'records_per_second': total_records / total_time if total_time > 0 else 0,
            'total_qa_checks_performed': total_checks,
            'checks_per_second': total_checks / total_time if total_time > 0 else 0,
            'average_quality_score': avg_quality_score,
            'average_error_rate': avg_error_rate,
            'qa_thresholds': {
                'critical_threshold': self.qa_thresholds.critical_error_threshold,
                'warning_threshold': self.qa_thresholds.warning_error_threshold,
                'anomaly_threshold': self.qa_thresholds.anomaly_severity_threshold
            },
            'dataset_details': {
                name: {
                    'records_processed': stats.records_processed,
                    'processing_time': stats.processing_time_seconds,
                    'quality_score': stats.quality_score,
                    'error_rate': stats.error_rate,
                    'status': stats.overall_status.value,
                    'critical_errors': stats.critical_errors,
                    'warnings': stats.warnings,
                    'anomalies': stats.anomalies_found
                }
                for name, stats in self.execution_stats.items()
            }
        }
    
    def calculate_quality_improvement(self, before_stats: Dict[str, Any], 
                                    after_stats: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate quality improvement metrics"""
        
        before_error_rate = before_stats.get('average_error_rate', 0)
        after_error_rate = after_stats.get('average_error_rate', 0)
        
        before_quality_score = before_stats.get('average_quality_score', 0)
        after_quality_score = after_stats.get('average_quality_score', 0)
        
        # Calculate improvement percentages
        error_reduction = ((before_error_rate - after_error_rate) / before_error_rate * 100) if before_error_rate > 0 else 0
        quality_improvement = ((after_quality_score - before_quality_score) / before_quality_score * 100) if before_quality_score > 0 else 0
        
        return {
            'error_reduction_percentage': error_reduction,
            'quality_improvement_percentage': quality_improvement,
            'before_metrics': {
                'error_rate': before_error_rate,
                'quality_score': before_quality_score
            },
            'after_metrics': {
                'error_rate': after_error_rate,
                'quality_score': after_quality_score
            },
            'improvement_achieved': error_reduction >= 40.0,  # Target 42% reduction
            'improvement_summary': f"Achieved {error_reduction:.1f}% error reduction and {quality_improvement:.1f}% quality improvement"
        }
    
    def get_comprehensive_qa_report(self) -> Dict[str, Any]:
        """Generate comprehensive QA report"""
        performance_metrics = self.get_qa_performance_metrics()
        
        # Get validation and anomaly summaries
        validation_summary = self.validation_engine.get_validation_summary()
        anomaly_summary = self.anomaly_engine.get_anomaly_summary()
        
        return {
            'report_timestamp': datetime.now(),
            'performance_metrics': performance_metrics,
            'validation_summary': validation_summary,
            'anomaly_summary': anomaly_summary,
            'overall_assessment': {
                'total_datasets': len(self.execution_stats),
                'datasets_passed': sum(1 for stats in self.execution_stats.values() if stats.overall_status == QAStatus.PASSED),
                'datasets_with_warnings': sum(1 for stats in self.execution_stats.values() if stats.overall_status == QAStatus.WARNING),
                'datasets_failed': sum(1 for stats in self.execution_stats.values() if stats.overall_status == QAStatus.FAILED),
                'overall_success_rate': (sum(1 for stats in self.execution_stats.values() if stats.overall_status in [QAStatus.PASSED, QAStatus.WARNING]) / len(self.execution_stats) * 100) if self.execution_stats else 0
            }
        }
    
    def clear_results(self):
        """Clear all QA results"""
        self.execution_stats.clear()
        self.validation_engine.clear_results()
        self.anomaly_engine.clear_results()
        self.logger.info("All QA results cleared")
