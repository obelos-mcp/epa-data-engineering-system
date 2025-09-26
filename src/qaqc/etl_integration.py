"""
Environmental Site Data Management & QA/QC Automation System
ETL Integration Module

This module integrates the QA/QC system with the existing ETL pipeline,
providing validation checkpoints, quality flag generation, and
performance monitoring throughout the data processing workflow.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple, Generator
from dataclasses import dataclass
from datetime import datetime
import logging
from pathlib import Path

# Import project modules
import sys
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.qaqc.qa_engine import QAEngineOrchestrator, QAThresholds, QAStatus
from src.qaqc.quality_metrics import QualityMetricsCalculator, QualityMetrics
from src.database import get_database_manager, DatabaseConfig

@dataclass
class ETLQACheckpoint:
    """QA checkpoint configuration for ETL stages"""
    stage_name: str
    validation_enabled: bool = True
    anomaly_detection_enabled: bool = True
    quality_metrics_enabled: bool = True
    store_findings: bool = True
    enforce_thresholds: bool = True
    checkpoint_id: str = ""
    
    def __post_init__(self):
        if not self.checkpoint_id:
            self.checkpoint_id = f"{self.stage_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

class ETLQAIntegrator:
    """Integrates QA/QC system with ETL pipeline"""
    
    def __init__(self, db_config: Optional[DatabaseConfig] = None,
                 qa_thresholds: Optional[QAThresholds] = None,
                 log_level: str = 'INFO'):
        
        # Initialize QA components
        self.qa_orchestrator = QAEngineOrchestrator(db_config, qa_thresholds, log_level)
        self.quality_calculator = QualityMetricsCalculator(log_level)
        self.db_manager = get_database_manager(db_config)
        
        # Setup logging
        self._setup_logging(log_level)
        
        # ETL integration state
        self.current_batch_id: Optional[int] = None
        self.checkpoint_results: Dict[str, Any] = {}
        self.quality_flags: Dict[str, List[str]] = {}
        
        # Default checkpoint configurations
        self.default_checkpoints = {
            'extract': ETLQACheckpoint(
                stage_name='extract',
                validation_enabled=True,
                anomaly_detection_enabled=False,  # Skip heavy anomaly detection at extract
                quality_metrics_enabled=True,
                enforce_thresholds=False  # Don't stop pipeline at extract
            ),
            'transform': ETLQACheckpoint(
                stage_name='transform',
                validation_enabled=True,
                anomaly_detection_enabled=True,
                quality_metrics_enabled=True,
                enforce_thresholds=True
            ),
            'load': ETLQACheckpoint(
                stage_name='load',
                validation_enabled=True,
                anomaly_detection_enabled=True,
                quality_metrics_enabled=True,
                enforce_thresholds=True
            )
        }
        
        self.logger.info("ETLQAIntegrator initialized")
    
    def _setup_logging(self, log_level: str):
        """Setup logging"""
        self.logger = logging.getLogger('etl_qa_integration')
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def set_etl_batch_id(self, batch_id: int):
        """Set ETL batch ID for tracking"""
        self.current_batch_id = batch_id
        self.qa_orchestrator.set_batch_id(batch_id)
        self.logger.info(f"ETL QA Integration batch ID set to: {batch_id}")
    
    def run_qa_checkpoint(self, df: pd.DataFrame, dataset_name: str, table_type: str,
                         stage: str, checkpoint_config: Optional[ETLQACheckpoint] = None) -> Tuple[bool, Dict[str, Any]]:
        """Run QA checkpoint during ETL processing"""
        
        # Use default checkpoint config if not provided
        if checkpoint_config is None:
            checkpoint_config = self.default_checkpoints.get(stage.lower(), 
                                                           self.default_checkpoints['transform'])
        
        self.logger.info(f"Running QA checkpoint: {checkpoint_config.checkpoint_id} for {dataset_name}")
        
        checkpoint_start_time = datetime.now()
        
        try:
            # Run QA for the specific ETL stage
            qa_results = self.qa_orchestrator.run_etl_stage_qa(df, dataset_name, table_type, stage)
            
            # Calculate quality metrics if enabled
            quality_metrics = None
            if checkpoint_config.quality_metrics_enabled:
                quality_metrics = self.quality_calculator.calculate_comprehensive_quality_metrics(
                    df, f"{dataset_name}_{stage}"
                )
            
            # Store findings if enabled
            if checkpoint_config.store_findings and 'findings' in qa_results:
                self.qa_orchestrator.store_qa_findings(qa_results['findings'])
            
            # Generate quality flags
            quality_flags = self._generate_quality_flags(qa_results, quality_metrics)
            self.quality_flags[checkpoint_config.checkpoint_id] = quality_flags
            
            # Check thresholds and determine if pipeline should continue
            should_continue = True
            threshold_message = ""
            
            if checkpoint_config.enforce_thresholds:
                should_continue, threshold_message = qa_results['threshold_check']
            
            # Prepare checkpoint results
            checkpoint_results = {
                'checkpoint_id': checkpoint_config.checkpoint_id,
                'stage': stage,
                'dataset_name': dataset_name,
                'timestamp': checkpoint_start_time,
                'processing_time': (datetime.now() - checkpoint_start_time).total_seconds(),
                'qa_results': qa_results,
                'quality_metrics': quality_metrics.overall_quality_score if quality_metrics else None,
                'quality_flags': quality_flags,
                'should_continue': should_continue,
                'threshold_message': threshold_message,
                'records_processed': len(df)
            }
            
            # Store checkpoint results
            self.checkpoint_results[checkpoint_config.checkpoint_id] = checkpoint_results
            
            # Log checkpoint completion
            status_msg = "PASSED" if should_continue else "FAILED"
            self.logger.info(
                f"QA checkpoint {checkpoint_config.checkpoint_id} completed: "
                f"{status_msg} ({len(df)} records, {checkpoint_results['processing_time']:.2f}s)"
            )
            
            return should_continue, checkpoint_results
            
        except Exception as e:
            self.logger.error(f"QA checkpoint {checkpoint_config.checkpoint_id} failed: {e}")
            
            # Return failure results
            error_results = {
                'checkpoint_id': checkpoint_config.checkpoint_id,
                'stage': stage,
                'dataset_name': dataset_name,
                'timestamp': checkpoint_start_time,
                'error': str(e),
                'should_continue': False,
                'records_processed': len(df)
            }
            
            return False, error_results
    
    def _generate_quality_flags(self, qa_results: Dict[str, Any], 
                               quality_metrics: Optional[QualityMetrics]) -> List[str]:
        """Generate quality flags based on QA results"""
        flags = []
        
        # QA status flags
        qa_stats = qa_results.get('stats')
        if qa_stats:
            if qa_stats.overall_status == QAStatus.FAILED:
                flags.append('QA_FAILED')
            elif qa_stats.overall_status == QAStatus.WARNING:
                flags.append('QA_WARNING')
            
            if qa_stats.critical_errors > 0:
                flags.append('CRITICAL_ERRORS')
            
            if qa_stats.anomalies_found > 0:
                flags.append('ANOMALIES_DETECTED')
        
        # Quality metrics flags
        if quality_metrics:
            if quality_metrics.overall_quality_score < 70:
                flags.append('LOW_QUALITY_SCORE')
            elif quality_metrics.overall_quality_score < 80:
                flags.append('MODERATE_QUALITY_SCORE')
            
            if quality_metrics.completeness_score < 90:
                flags.append('INCOMPLETE_DATA')
            
            if quality_metrics.accuracy_score < 95:
                flags.append('ACCURACY_ISSUES')
            
            if quality_metrics.consistency_score < 90:
                flags.append('CONSISTENCY_ISSUES')
        
        return flags
    
    def add_quality_flags_to_dataframe(self, df: pd.DataFrame, 
                                     checkpoint_id: str) -> pd.DataFrame:
        """Add quality flags as columns to the dataframe"""
        df_with_flags = df.copy()
        
        # Add checkpoint ID
        df_with_flags['qa_checkpoint_id'] = checkpoint_id
        
        # Add quality flags
        quality_flags = self.quality_flags.get(checkpoint_id, [])
        df_with_flags['qa_quality_flags'] = ','.join(quality_flags) if quality_flags else 'CLEAN'
        
        # Add QA timestamp
        df_with_flags['qa_processed_date'] = datetime.now()
        
        # Add batch ID if available
        if self.current_batch_id:
            df_with_flags['qa_batch_id'] = self.current_batch_id
        
        return df_with_flags
    
    def create_enhanced_transform_generator(self, original_transform_generator: Generator,
                                          dataset_name: str, table_type: str) -> Generator:
        """Create enhanced transform generator with QA checkpoints"""
        
        self.logger.info(f"Creating enhanced transform generator for {dataset_name}")
        
        for chunk_data, transform_stats in original_transform_generator:
            try:
                # Run transform stage QA checkpoint
                should_continue, checkpoint_results = self.run_qa_checkpoint(
                    chunk_data, dataset_name, table_type, 'transform'
                )
                
                if not should_continue:
                    error_msg = checkpoint_results.get('threshold_message', 'QA checkpoint failed')
                    raise Exception(f"Transform QA checkpoint failed: {error_msg}")
                
                # Add quality flags to the data
                checkpoint_id = checkpoint_results['checkpoint_id']
                enhanced_data = self.add_quality_flags_to_dataframe(chunk_data, checkpoint_id)
                
                # Enhance transform stats with QA information
                enhanced_stats = transform_stats.copy()
                enhanced_stats.update({
                    'qa_checkpoint_id': checkpoint_id,
                    'qa_quality_score': checkpoint_results.get('quality_metrics'),
                    'qa_flags': checkpoint_results.get('quality_flags', []),
                    'qa_processing_time': checkpoint_results.get('processing_time', 0)
                })
                
                yield enhanced_data, enhanced_stats
                
            except Exception as e:
                self.logger.error(f"Enhanced transform generator failed: {e}")
                raise
    
    def validate_etl_pipeline_readiness(self, source_configs: Dict[str, Any]) -> Dict[str, Any]:
        """Validate ETL pipeline readiness with QA/QC integration"""
        
        self.logger.info("Validating ETL pipeline readiness")
        
        readiness_results = {
            'timestamp': datetime.now(),
            'overall_ready': True,
            'qa_system_status': {},
            'database_connectivity': {},
            'validation_rules_status': {},
            'source_configurations': {},
            'recommendations': []
        }
        
        try:
            # Check QA system components
            readiness_results['qa_system_status'] = {
                'qa_orchestrator': True,
                'quality_calculator': True,
                'database_manager': self.db_manager.health_check(),
                'validation_engine': True,
                'anomaly_engine': True
            }
            
            # Check database connectivity and schema
            readiness_results['database_connectivity'] = {
                'connection_healthy': self.db_manager.health_check(),
                'qa_findings_table_exists': self._check_qa_tables_exist(),
                'connection_stats': self.db_manager.get_connection_stats()
            }
            
            # Validate source configurations
            for source_name, config in source_configs.items():
                validation_result = self._validate_source_config(source_name, config)
                readiness_results['source_configurations'][source_name] = validation_result
                
                if not validation_result['valid']:
                    readiness_results['overall_ready'] = False
            
            # Check if any critical issues
            if not readiness_results['qa_system_status']['database_manager']:
                readiness_results['overall_ready'] = False
                readiness_results['recommendations'].append(
                    "Database connectivity issues detected - check database configuration"
                )
            
            if not readiness_results['database_connectivity']['qa_findings_table_exists']:
                readiness_results['recommendations'].append(
                    "QA findings table not found - run database schema creation"
                )
            
            self.logger.info(f"ETL pipeline readiness: {'READY' if readiness_results['overall_ready'] else 'NOT READY'}")
            
            return readiness_results
            
        except Exception as e:
            self.logger.error(f"Pipeline readiness validation failed: {e}")
            readiness_results['overall_ready'] = False
            readiness_results['error'] = str(e)
            return readiness_results
    
    def _check_qa_tables_exist(self) -> bool:
        """Check if QA-related database tables exist"""
        try:
            # Check for qa_findings table
            query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'core' 
                AND table_name = 'qa_findings'
            )
            """
            
            result = self.db_manager.execute_query(query)
            return result.iloc[0, 0] if not result.empty else False
            
        except Exception as e:
            self.logger.warning(f"Could not check QA tables: {e}")
            return False
    
    def _validate_source_config(self, source_name: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Validate source configuration for QA integration"""
        validation_result = {
            'source_name': source_name,
            'valid': True,
            'issues': [],
            'recommendations': []
        }
        
        # Check required configuration fields
        required_fields = ['name', 'staging_table', 'required_columns']
        for field in required_fields:
            if field not in config:
                validation_result['valid'] = False
                validation_result['issues'].append(f"Missing required field: {field}")
        
        # Check validation rules configuration
        if 'validation_rules' in config:
            rules = config['validation_rules']
            if not isinstance(rules, dict):
                validation_result['issues'].append("validation_rules should be a dictionary")
            else:
                # Check for common validation rule types
                expected_rule_types = ['required_fields', 'data_types', 'ranges']
                missing_rules = [rule for rule in expected_rule_types if rule not in rules]
                if missing_rules:
                    validation_result['recommendations'].append(
                        f"Consider adding validation rules for: {missing_rules}"
                    )
        
        return validation_result
    
    def get_etl_qa_performance_report(self) -> Dict[str, Any]:
        """Generate ETL QA performance report"""
        
        if not self.checkpoint_results:
            return {'error': 'No checkpoint results available'}
        
        # Calculate performance metrics
        total_checkpoints = len(self.checkpoint_results)
        passed_checkpoints = sum(1 for r in self.checkpoint_results.values() if r.get('should_continue', False))
        failed_checkpoints = total_checkpoints - passed_checkpoints
        
        total_processing_time = sum(r.get('processing_time', 0) for r in self.checkpoint_results.values())
        total_records = sum(r.get('records_processed', 0) for r in self.checkpoint_results.values())
        
        # Quality metrics summary
        quality_scores = [r.get('quality_metrics') for r in self.checkpoint_results.values() if r.get('quality_metrics')]
        avg_quality_score = np.mean(quality_scores) if quality_scores else 0
        
        # Quality flags summary
        all_flags = []
        for checkpoint_flags in self.quality_flags.values():
            all_flags.extend(checkpoint_flags)
        
        flag_counts = {}
        for flag in all_flags:
            flag_counts[flag] = flag_counts.get(flag, 0) + 1
        
        return {
            'report_timestamp': datetime.now(),
            'batch_id': self.current_batch_id,
            'performance_summary': {
                'total_checkpoints': total_checkpoints,
                'passed_checkpoints': passed_checkpoints,
                'failed_checkpoints': failed_checkpoints,
                'success_rate': (passed_checkpoints / total_checkpoints * 100) if total_checkpoints > 0 else 0,
                'total_processing_time': total_processing_time,
                'total_records_processed': total_records,
                'records_per_second': total_records / total_processing_time if total_processing_time > 0 else 0
            },
            'quality_summary': {
                'average_quality_score': avg_quality_score,
                'quality_flag_counts': flag_counts,
                'total_quality_issues': len(all_flags)
            },
            'checkpoint_details': {
                checkpoint_id: {
                    'stage': result.get('stage'),
                    'dataset_name': result.get('dataset_name'),
                    'timestamp': result.get('timestamp'),
                    'processing_time': result.get('processing_time'),
                    'records_processed': result.get('records_processed'),
                    'quality_score': result.get('quality_metrics'),
                    'should_continue': result.get('should_continue'),
                    'quality_flags': result.get('quality_flags', [])
                }
                for checkpoint_id, result in self.checkpoint_results.items()
            },
            'qa_orchestrator_metrics': self.qa_orchestrator.get_qa_performance_metrics()
        }
    
    def calculate_etl_quality_improvement(self, before_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate quality improvement achieved through ETL QA integration"""
        
        current_metrics = self.get_etl_qa_performance_report()
        
        # Calculate improvements
        before_quality = before_metrics.get('quality_summary', {}).get('average_quality_score', 0)
        current_quality = current_metrics.get('quality_summary', {}).get('average_quality_score', 0)
        
        before_success_rate = before_metrics.get('performance_summary', {}).get('success_rate', 0)
        current_success_rate = current_metrics.get('performance_summary', {}).get('success_rate', 0)
        
        quality_improvement = current_quality - before_quality
        success_improvement = current_success_rate - before_success_rate
        
        # Calculate error reduction
        before_error_rate = 100 - before_success_rate
        current_error_rate = 100 - current_success_rate
        error_reduction = before_error_rate - current_error_rate
        error_reduction_pct = (error_reduction / before_error_rate * 100) if before_error_rate > 0 else 0
        
        return {
            'improvement_analysis': {
                'quality_score_improvement': quality_improvement,
                'success_rate_improvement': success_improvement,
                'error_reduction_percentage': error_reduction_pct,
                'target_achievement': {
                    'target_error_reduction': 42.0,
                    'achieved_error_reduction': error_reduction_pct,
                    'target_met': error_reduction_pct >= 42.0
                }
            },
            'before_metrics': before_metrics,
            'current_metrics': current_metrics,
            'improvement_summary': f"Achieved {error_reduction_pct:.1f}% error reduction through ETL QA integration"
        }
    
    def clear_checkpoint_results(self):
        """Clear checkpoint results and quality flags"""
        self.checkpoint_results.clear()
        self.quality_flags.clear()
        self.qa_orchestrator.clear_results()
        self.logger.info("ETL QA checkpoint results cleared")
