"""
Environmental Site Data Management & QA/QC Automation System
QA/QC Package Initialization

This package provides comprehensive quality assurance and quality control
capabilities for EPA environmental data processing.
"""

from .validation_rules import ValidationRulesEngine, ValidationResult, ValidationSeverity
from .anomaly_detection import AnomalyDetectionEngine, AnomalyResult
from .qa_engine import QAEngineOrchestrator, QAStatus, QAThresholds, QAExecutionStats
from .quality_metrics import QualityMetricsCalculator, QualityMetrics, QualityTrend
from .qa_reports import QAReportGenerator, AlertConfig

__version__ = "1.0.0"
__author__ = "EPA Environmental Data Management System"

__all__ = [
    'ValidationRulesEngine',
    'ValidationResult', 
    'ValidationSeverity',
    'AnomalyDetectionEngine',
    'AnomalyResult',
    'QAEngineOrchestrator',
    'QAStatus',
    'QAThresholds',
    'QAExecutionStats',
    'QualityMetricsCalculator',
    'QualityMetrics',
    'QualityTrend',
    'QAReportGenerator',
    'AlertConfig'
]
