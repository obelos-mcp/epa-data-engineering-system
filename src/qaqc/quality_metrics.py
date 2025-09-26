"""
Environmental Site Data Management & QA/QC Automation System
Data Quality Metrics

This module provides comprehensive data quality metrics calculation,
quality scoring, trend analysis, and quality improvement tracking.
"""

import pandas as pd
import numpy as np
import re
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import logging

@dataclass
class QualityMetrics:
    """Comprehensive quality metrics for a dataset"""
    dataset_name: str
    timestamp: datetime = field(default_factory=datetime.now)
    
    # Completeness metrics
    total_records: int = 0
    complete_records: int = 0
    completeness_score: float = 0.0
    
    # Accuracy metrics
    valid_records: int = 0
    accuracy_score: float = 0.0
    
    # Consistency metrics
    consistent_records: int = 0
    consistency_score: float = 0.0
    
    # Uniqueness metrics
    unique_records: int = 0
    uniqueness_score: float = 0.0
    
    # Timeliness metrics
    current_records: int = 0
    timeliness_score: float = 0.0
    
    # Overall quality score (weighted average)
    overall_quality_score: float = 0.0
    
    # Quality grade (A-F)
    quality_grade: str = "F"
    
    # Detailed metrics
    column_quality_scores: Dict[str, float] = field(default_factory=dict)
    validation_results: Dict[str, Any] = field(default_factory=dict)
    anomaly_results: Dict[str, Any] = field(default_factory=dict)
    
    def calculate_overall_score(self, weights: Optional[Dict[str, float]] = None):
        """Calculate overall quality score using weighted average"""
        if weights is None:
            weights = {
                'completeness': 0.25,
                'accuracy': 0.30,
                'consistency': 0.20,
                'uniqueness': 0.15,
                'timeliness': 0.10
            }
        
        self.overall_quality_score = (
            self.completeness_score * weights['completeness'] +
            self.accuracy_score * weights['accuracy'] +
            self.consistency_score * weights['consistency'] +
            self.uniqueness_score * weights['uniqueness'] +
            self.timeliness_score * weights['timeliness']
        )
        
        # Assign quality grade
        if self.overall_quality_score >= 90:
            self.quality_grade = "A"
        elif self.overall_quality_score >= 80:
            self.quality_grade = "B"
        elif self.overall_quality_score >= 70:
            self.quality_grade = "C"
        elif self.overall_quality_score >= 60:
            self.quality_grade = "D"
        else:
            self.quality_grade = "F"

@dataclass
class QualityTrend:
    """Quality trend analysis over time"""
    dataset_name: str
    time_period: str  # 'daily', 'weekly', 'monthly'
    trend_data: List[Dict[str, Any]] = field(default_factory=list)
    improvement_rate: float = 0.0
    trend_direction: str = "stable"  # 'improving', 'declining', 'stable'
    
    def calculate_trend(self):
        """Calculate quality trend direction and rate"""
        if len(self.trend_data) < 2:
            return
        
        scores = [point['quality_score'] for point in self.trend_data]
        
        # Calculate linear trend
        x = np.arange(len(scores))
        slope, _ = np.polyfit(x, scores, 1)
        
        self.improvement_rate = slope
        
        if slope > 1.0:
            self.trend_direction = "improving"
        elif slope < -1.0:
            self.trend_direction = "declining"
        else:
            self.trend_direction = "stable"

class QualityMetricsCalculator:
    """Calculator for comprehensive data quality metrics"""
    
    def __init__(self, log_level: str = 'INFO'):
        self.logger = self._setup_logging(log_level)
        
        # Quality calculation parameters
        self.quality_params = {
            'completeness_threshold': 95.0,  # % complete for full score
            'accuracy_threshold': 98.0,      # % accurate for full score
            'consistency_threshold': 95.0,   # % consistent for full score
            'uniqueness_threshold': 99.0,    # % unique for full score
            'timeliness_days': 30,           # Days for current data
            'column_weight_decay': 0.1       # Weight decay for less important columns
        }
        
        self.logger.info("QualityMetricsCalculator initialized")
    
    def _setup_logging(self, log_level: str):
        """Setup logging"""
        logger = logging.getLogger('quality_metrics')
        logger.setLevel(getattr(logging, log_level.upper()))
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def calculate_completeness_metrics(self, df: pd.DataFrame, required_columns: List[str] = None) -> Dict[str, Any]:
        """Calculate data completeness metrics"""
        if df.empty:
            return {
                'total_records': 0,
                'complete_records': 0,
                'completeness_score': 0.0,
                'column_completeness': {}
            }
        
        total_records = len(df)
        
        # Calculate column-level completeness
        column_completeness = {}
        for column in df.columns:
            non_null_count = df[column].notna().sum()
            completeness_pct = (non_null_count / total_records) * 100
            column_completeness[column] = completeness_pct
        
        # Calculate overall completeness
        if required_columns:
            # Focus on required columns
            required_completeness = [column_completeness[col] for col in required_columns if col in column_completeness]
            overall_completeness = np.mean(required_completeness) if required_completeness else 0.0
        else:
            # Use all columns
            overall_completeness = np.mean(list(column_completeness.values()))
        
        # Count complete records (all required fields present)
        if required_columns:
            required_cols_in_df = [col for col in required_columns if col in df.columns]
            complete_records = df[required_cols_in_df].notna().all(axis=1).sum()
        else:
            complete_records = df.notna().all(axis=1).sum()
        
        # Calculate completeness score (0-100)
        completeness_score = min(overall_completeness, 100.0)
        
        return {
            'total_records': total_records,
            'complete_records': int(complete_records),
            'completeness_score': completeness_score,
            'column_completeness': column_completeness,
            'overall_completeness_rate': overall_completeness
        }
    
    def calculate_accuracy_metrics(self, df: pd.DataFrame, validation_results: List[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Calculate data accuracy metrics based on validation results"""
        if df.empty:
            return {
                'total_records': 0,
                'valid_records': 0,
                'accuracy_score': 0.0,
                'validation_errors': 0
            }
        
        total_records = len(df)
        
        if validation_results:
            # Use validation results to determine accuracy
            total_errors = sum(result.get('failed_count', 0) for result in validation_results)
            total_validations = sum(result.get('total_count', 0) for result in validation_results)
            
            if total_validations > 0:
                accuracy_rate = ((total_validations - total_errors) / total_validations) * 100
            else:
                accuracy_rate = 100.0
            
            valid_records = total_records - (total_errors // len(validation_results)) if validation_results else total_records
        else:
            # Basic accuracy check - non-null numeric values in numeric columns
            numeric_columns = df.select_dtypes(include=[np.number]).columns
            
            if len(numeric_columns) > 0:
                valid_numeric = df[numeric_columns].apply(lambda x: pd.to_numeric(x, errors='coerce').notna()).all(axis=1).sum()
                accuracy_rate = (valid_numeric / total_records) * 100
                valid_records = int(valid_numeric)
            else:
                accuracy_rate = 100.0
                valid_records = total_records
        
        accuracy_score = min(accuracy_rate, 100.0)
        
        return {
            'total_records': total_records,
            'valid_records': valid_records,
            'accuracy_score': accuracy_score,
            'validation_errors': total_records - valid_records,
            'accuracy_rate': accuracy_rate
        }
    
    def calculate_consistency_metrics(self, df: pd.DataFrame, consistency_rules: List[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Calculate data consistency metrics"""
        if df.empty:
            return {
                'total_records': 0,
                'consistent_records': 0,
                'consistency_score': 0.0,
                'consistency_violations': 0
            }
        
        total_records = len(df)
        consistency_violations = 0
        
        if consistency_rules:
            # Apply custom consistency rules
            for rule in consistency_rules:
                rule_type = rule.get('type')
                columns = rule.get('columns', [])
                
                if rule_type == 'cross_field' and len(columns) >= 2:
                    # Example: effective_date < expiration_date
                    col1, col2 = columns[0], columns[1]
                    if col1 in df.columns and col2 in df.columns:
                        violations = (df[col1] >= df[col2]).sum()
                        consistency_violations += violations
                
                elif rule_type == 'format_consistency':
                    # Example: consistent date formats
                    for col in columns:
                        if col in df.columns:
                            # Check for format inconsistencies
                            date_formats = df[col].dropna().astype(str).apply(self._detect_date_format)
                            unique_formats = date_formats.nunique()
                            if unique_formats > 1:
                                consistency_violations += len(df) - date_formats.value_counts().iloc[0]
        else:
            # Basic consistency checks
            
            # Check coordinate consistency (lat/lon both present or both null)
            lat_cols = [col for col in df.columns if 'lat' in col.lower()]
            lon_cols = [col for col in df.columns if 'lon' in col.lower() or 'long' in col.lower()]
            
            if lat_cols and lon_cols:
                lat_col, lon_col = lat_cols[0], lon_cols[0]
                lat_present = df[lat_col].notna()
                lon_present = df[lon_col].notna()
                inconsistent_coords = (lat_present != lon_present).sum()
                consistency_violations += inconsistent_coords
            
            # Check state code consistency (uppercase)
            state_cols = [col for col in df.columns if 'state' in col.lower()]
            for col in state_cols:
                if df[col].dtype == 'object':
                    mixed_case = df[col].dropna().apply(lambda x: str(x) != str(x).upper()).sum()
                    consistency_violations += mixed_case
        
        consistent_records = max(0, total_records - consistency_violations)
        consistency_score = (consistent_records / total_records) * 100 if total_records > 0 else 0.0
        
        return {
            'total_records': total_records,
            'consistent_records': consistent_records,
            'consistency_score': consistency_score,
            'consistency_violations': consistency_violations
        }
    
    def _detect_date_format(self, date_str: str) -> str:
        """Detect date format pattern"""
        date_str = str(date_str).strip()
        
        if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
            return 'YYYY-MM-DD'
        elif re.match(r'\d{2}/\d{2}/\d{4}', date_str):
            return 'MM/DD/YYYY'
        elif re.match(r'\d{2}-\d{2}-\d{4}', date_str):
            return 'MM-DD-YYYY'
        elif re.match(r'\d{4}/\d{2}/\d{2}', date_str):
            return 'YYYY/MM/DD'
        else:
            return 'OTHER'
    
    def calculate_uniqueness_metrics(self, df: pd.DataFrame, key_columns: List[str] = None) -> Dict[str, Any]:
        """Calculate data uniqueness metrics"""
        if df.empty:
            return {
                'total_records': 0,
                'unique_records': 0,
                'uniqueness_score': 0.0,
                'duplicate_count': 0
            }
        
        total_records = len(df)
        
        if key_columns:
            # Check uniqueness based on key columns
            key_cols_in_df = [col for col in key_columns if col in df.columns]
            if key_cols_in_df:
                unique_records = df[key_cols_in_df].drop_duplicates().shape[0]
                duplicate_count = total_records - unique_records
            else:
                unique_records = total_records
                duplicate_count = 0
        else:
            # Check overall record uniqueness
            unique_records = df.drop_duplicates().shape[0]
            duplicate_count = total_records - unique_records
        
        uniqueness_score = (unique_records / total_records) * 100 if total_records > 0 else 0.0
        
        return {
            'total_records': total_records,
            'unique_records': unique_records,
            'uniqueness_score': uniqueness_score,
            'duplicate_count': duplicate_count
        }
    
    def calculate_timeliness_metrics(self, df: pd.DataFrame, date_columns: List[str] = None) -> Dict[str, Any]:
        """Calculate data timeliness metrics"""
        if df.empty:
            return {
                'total_records': 0,
                'current_records': 0,
                'timeliness_score': 0.0,
                'avg_data_age_days': 0
            }
        
        total_records = len(df)
        current_date = datetime.now()
        timeliness_threshold = timedelta(days=self.quality_params['timeliness_days'])
        
        if date_columns:
            date_cols_in_df = [col for col in date_columns if col in df.columns]
        else:
            # Auto-detect date columns
            date_cols_in_df = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
        
        if not date_cols_in_df:
            # No date columns found, assume all data is current
            return {
                'total_records': total_records,
                'current_records': total_records,
                'timeliness_score': 100.0,
                'avg_data_age_days': 0
            }
        
        # Use the most recent date column for timeliness assessment
        date_col = date_cols_in_df[0]
        
        try:
            # Convert to datetime
            dates = pd.to_datetime(df[date_col], errors='coerce')
            valid_dates = dates.dropna()
            
            if valid_dates.empty:
                return {
                    'total_records': total_records,
                    'current_records': 0,
                    'timeliness_score': 0.0,
                    'avg_data_age_days': 0
                }
            
            # Calculate data age
            data_ages = current_date - valid_dates
            current_records = (data_ages <= timeliness_threshold).sum()
            avg_age_days = data_ages.dt.days.mean()
            
            timeliness_score = (current_records / len(valid_dates)) * 100
            
            return {
                'total_records': total_records,
                'current_records': int(current_records),
                'timeliness_score': timeliness_score,
                'avg_data_age_days': avg_age_days,
                'date_column_used': date_col
            }
            
        except Exception as e:
            self.logger.warning(f"Timeliness calculation failed: {e}")
            return {
                'total_records': total_records,
                'current_records': 0,
                'timeliness_score': 0.0,
                'avg_data_age_days': 0,
                'error': str(e)
            }
    
    def calculate_column_quality_scores(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate quality scores for individual columns"""
        column_scores = {}
        
        for column in df.columns:
            # Completeness component (40%)
            completeness = (df[column].notna().sum() / len(df)) * 100
            
            # Validity component (40%)
            if df[column].dtype in ['object']:
                # For string columns, check for non-empty strings
                valid_strings = df[column].dropna().astype(str).apply(lambda x: len(x.strip()) > 0).sum()
                validity = (valid_strings / df[column].notna().sum()) * 100 if df[column].notna().sum() > 0 else 0
            else:
                # For numeric columns, check for valid numeric values
                validity = 100.0  # Assume numeric columns are valid if not null
            
            # Uniqueness component (20%)
            if len(df) > 0:
                uniqueness = (df[column].nunique() / len(df)) * 100
            else:
                uniqueness = 0.0
            
            # Weighted score
            column_score = (completeness * 0.4) + (validity * 0.4) + (uniqueness * 0.2)
            column_scores[column] = min(column_score, 100.0)
        
        return column_scores
    
    def calculate_comprehensive_quality_metrics(self, df: pd.DataFrame, dataset_name: str,
                                              required_columns: List[str] = None,
                                              key_columns: List[str] = None,
                                              date_columns: List[str] = None,
                                              validation_results: List[Dict[str, Any]] = None) -> QualityMetrics:
        """Calculate comprehensive quality metrics for a dataset"""
        
        self.logger.info(f"Calculating quality metrics for {dataset_name} ({len(df)} records)")
        
        # Initialize metrics object
        metrics = QualityMetrics(dataset_name=dataset_name)
        
        # Calculate individual metric components
        completeness_metrics = self.calculate_completeness_metrics(df, required_columns)
        accuracy_metrics = self.calculate_accuracy_metrics(df, validation_results)
        consistency_metrics = self.calculate_consistency_metrics(df)
        uniqueness_metrics = self.calculate_uniqueness_metrics(df, key_columns)
        timeliness_metrics = self.calculate_timeliness_metrics(df, date_columns)
        
        # Populate metrics object
        metrics.total_records = len(df)
        
        # Completeness
        metrics.complete_records = completeness_metrics['complete_records']
        metrics.completeness_score = completeness_metrics['completeness_score']
        
        # Accuracy
        metrics.valid_records = accuracy_metrics['valid_records']
        metrics.accuracy_score = accuracy_metrics['accuracy_score']
        
        # Consistency
        metrics.consistent_records = consistency_metrics['consistent_records']
        metrics.consistency_score = consistency_metrics['consistency_score']
        
        # Uniqueness
        metrics.unique_records = uniqueness_metrics['unique_records']
        metrics.uniqueness_score = uniqueness_metrics['uniqueness_score']
        
        # Timeliness
        metrics.current_records = timeliness_metrics['current_records']
        metrics.timeliness_score = timeliness_metrics['timeliness_score']
        
        # Column-level quality scores
        metrics.column_quality_scores = self.calculate_column_quality_scores(df)
        
        # Store detailed results
        metrics.validation_results = {
            'completeness': completeness_metrics,
            'accuracy': accuracy_metrics,
            'consistency': consistency_metrics,
            'uniqueness': uniqueness_metrics,
            'timeliness': timeliness_metrics
        }
        
        # Calculate overall quality score
        metrics.calculate_overall_score()
        
        self.logger.info(f"Quality metrics calculated for {dataset_name}: Score={metrics.overall_quality_score:.1f} (Grade: {metrics.quality_grade})")
        
        return metrics
    
    def calculate_quality_improvement(self, before_metrics: QualityMetrics, 
                                    after_metrics: QualityMetrics) -> Dict[str, Any]:
        """Calculate quality improvement between two metric snapshots"""
        
        if before_metrics.dataset_name != after_metrics.dataset_name:
            raise ValueError("Metrics must be for the same dataset")
        
        # Calculate improvements
        completeness_improvement = after_metrics.completeness_score - before_metrics.completeness_score
        accuracy_improvement = after_metrics.accuracy_score - before_metrics.accuracy_score
        consistency_improvement = after_metrics.consistency_score - before_metrics.consistency_score
        uniqueness_improvement = after_metrics.uniqueness_score - before_metrics.uniqueness_score
        timeliness_improvement = after_metrics.timeliness_score - before_metrics.timeliness_score
        overall_improvement = after_metrics.overall_quality_score - before_metrics.overall_quality_score
        
        # Calculate improvement percentages
        def safe_percentage(before, improvement):
            return (improvement / before * 100) if before > 0 else 0
        
        return {
            'dataset_name': before_metrics.dataset_name,
            'comparison_period': {
                'before': before_metrics.timestamp,
                'after': after_metrics.timestamp
            },
            'score_improvements': {
                'completeness': {
                    'absolute': completeness_improvement,
                    'percentage': safe_percentage(before_metrics.completeness_score, completeness_improvement)
                },
                'accuracy': {
                    'absolute': accuracy_improvement,
                    'percentage': safe_percentage(before_metrics.accuracy_score, accuracy_improvement)
                },
                'consistency': {
                    'absolute': consistency_improvement,
                    'percentage': safe_percentage(before_metrics.consistency_score, consistency_improvement)
                },
                'uniqueness': {
                    'absolute': uniqueness_improvement,
                    'percentage': safe_percentage(before_metrics.uniqueness_score, uniqueness_improvement)
                },
                'timeliness': {
                    'absolute': timeliness_improvement,
                    'percentage': safe_percentage(before_metrics.timeliness_score, timeliness_improvement)
                },
                'overall': {
                    'absolute': overall_improvement,
                    'percentage': safe_percentage(before_metrics.overall_quality_score, overall_improvement)
                }
            },
            'grade_improvement': {
                'before': before_metrics.quality_grade,
                'after': after_metrics.quality_grade,
                'improved': after_metrics.quality_grade > before_metrics.quality_grade
            },
            'target_achievement': {
                'target_improvement': 42.0,  # Target 42% improvement
                'achieved_improvement': safe_percentage(before_metrics.overall_quality_score, overall_improvement),
                'target_met': safe_percentage(before_metrics.overall_quality_score, overall_improvement) >= 42.0
            }
        }
    
    def generate_quality_dashboard_data(self, metrics_list: List[QualityMetrics]) -> Dict[str, Any]:
        """Generate data for quality dashboard visualization"""
        
        if not metrics_list:
            return {'error': 'No metrics provided'}
        
        # Overall statistics
        avg_quality_score = np.mean([m.overall_quality_score for m in metrics_list])
        grade_distribution = {}
        
        for metric in metrics_list:
            grade = metric.quality_grade
            grade_distribution[grade] = grade_distribution.get(grade, 0) + 1
        
        # Time series data for trending
        time_series = [
            {
                'timestamp': m.timestamp,
                'dataset': m.dataset_name,
                'quality_score': m.overall_quality_score,
                'completeness': m.completeness_score,
                'accuracy': m.accuracy_score,
                'consistency': m.consistency_score
            }
            for m in sorted(metrics_list, key=lambda x: x.timestamp)
        ]
        
        return {
            'summary': {
                'total_datasets': len(metrics_list),
                'average_quality_score': avg_quality_score,
                'grade_distribution': grade_distribution,
                'last_updated': max(m.timestamp for m in metrics_list)
            },
            'time_series': time_series,
            'dataset_details': [
                {
                    'dataset_name': m.dataset_name,
                    'quality_score': m.overall_quality_score,
                    'quality_grade': m.quality_grade,
                    'total_records': m.total_records,
                    'completeness': m.completeness_score,
                    'accuracy': m.accuracy_score,
                    'consistency': m.consistency_score,
                    'uniqueness': m.uniqueness_score,
                    'timeliness': m.timeliness_score
                }
                for m in metrics_list
            ]
        }
