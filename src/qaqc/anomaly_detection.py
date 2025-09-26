"""
Environmental Site Data Management & QA/QC Automation System
Anomaly Detection Engine

This module provides comprehensive anomaly detection for environmental data
including statistical outliers, time series anomalies, geographic clustering,
and cross-dataset consistency checks.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from scipy import stats
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
import logging

@dataclass
class AnomalyResult:
    """Result of an anomaly detection check"""
    detector_name: str
    anomaly_type: str
    anomalous_records: List[int] = field(default_factory=list)
    anomaly_count: int = 0
    total_count: int = 0
    anomaly_rate: float = 0.0
    severity_score: float = 0.0  # 0-1 scale, higher is more severe
    description: str = ""
    details: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Calculate anomaly rate after initialization"""
        if self.total_count > 0:
            self.anomaly_rate = (self.anomaly_count / self.total_count) * 100
        else:
            self.anomaly_rate = 0.0

class AnomalyDetectionEngine:
    """Comprehensive anomaly detection engine for EPA environmental data"""
    
    def __init__(self, log_level: str = 'INFO'):
        self.logger = self._setup_logging(log_level)
        self.anomaly_results: List[AnomalyResult] = []
        
        # Detection parameters
        self.detection_params = self._initialize_detection_params()
        
        self.logger.info("AnomalyDetectionEngine initialized")
    
    def _setup_logging(self, log_level: str):
        """Setup logging for anomaly detection"""
        logger = logging.getLogger('anomaly_detection')
        logger.setLevel(getattr(logging, log_level.upper()))
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _initialize_detection_params(self) -> Dict[str, Any]:
        """Initialize anomaly detection parameters"""
        return {
            'statistical_outliers': {
                'z_score_threshold': 3.0,
                'iqr_multiplier': 1.5,
                'modified_z_score_threshold': 3.5
            },
            'time_series': {
                'spike_threshold': 5.0,  # Standard deviations
                'drop_threshold': -5.0,
                'trend_change_threshold': 0.8,
                'seasonal_deviation_threshold': 3.0
            },
            'geographic': {
                'dbscan_eps': 0.5,  # Degrees
                'dbscan_min_samples': 5,
                'coordinate_precision': 6,  # Decimal places
                'impossible_location_bounds': {
                    'ocean_depth_threshold': -1000,  # Meters below sea level
                    'elevation_threshold': 10000     # Meters above sea level
                }
            },
            'compliance': {
                'violation_to_inspection_ratio_threshold': 10.0,
                'zero_inspection_with_violations_threshold': 1,
                'excessive_penalty_threshold': 1000000  # $1M
            },
            'emission': {
                'facility_type_deviation_threshold': 5.0,
                'zero_emission_threshold': 0.001,
                'emission_spike_threshold': 10.0,
                'unit_conversion_check': True
            }
        }
    
    def detect_statistical_outliers(self, df: pd.DataFrame, numeric_columns: List[str] = None) -> List[AnomalyResult]:
        """Detect statistical outliers using multiple methods"""
        results = []
        
        if numeric_columns is None:
            numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
        
        for column in numeric_columns:
            if column in df.columns and not df[column].empty:
                # Z-score method
                z_score_result = self._detect_z_score_outliers(df, column)
                results.append(z_score_result)
                
                # IQR method
                iqr_result = self._detect_iqr_outliers(df, column)
                results.append(iqr_result)
                
                # Modified Z-score method
                modified_z_result = self._detect_modified_z_score_outliers(df, column)
                results.append(modified_z_result)
        
        return results
    
    def _detect_z_score_outliers(self, df: pd.DataFrame, column: str) -> AnomalyResult:
        """Detect outliers using Z-score method"""
        data = df[column].dropna()
        
        if len(data) < 3:
            return AnomalyResult(
                detector_name="z_score",
                anomaly_type="statistical_outlier",
                total_count=len(df),
                description=f"Insufficient data for Z-score analysis in {column}"
            )
        
        z_scores = np.abs(stats.zscore(data))
        threshold = self.detection_params['statistical_outliers']['z_score_threshold']
        
        outlier_indices = data.index[z_scores > threshold].tolist()
        
        # Calculate severity based on how extreme the outliers are
        max_z_score = z_scores.max() if len(z_scores) > 0 else 0
        severity = min(max_z_score / (threshold * 2), 1.0)
        
        return AnomalyResult(
            detector_name="z_score",
            anomaly_type="statistical_outlier",
            anomalous_records=outlier_indices,
            anomaly_count=len(outlier_indices),
            total_count=len(df),
            severity_score=severity,
            description=f"Z-score outliers in {column} (threshold: {threshold})",
            details={
                'column': column,
                'threshold': threshold,
                'max_z_score': float(max_z_score),
                'mean': float(data.mean()),
                'std': float(data.std())
            }
        )
    
    def _detect_iqr_outliers(self, df: pd.DataFrame, column: str) -> AnomalyResult:
        """Detect outliers using Interquartile Range method"""
        data = df[column].dropna()
        
        if len(data) < 4:
            return AnomalyResult(
                detector_name="iqr",
                anomaly_type="statistical_outlier",
                total_count=len(df),
                description=f"Insufficient data for IQR analysis in {column}"
            )
        
        Q1 = data.quantile(0.25)
        Q3 = data.quantile(0.75)
        IQR = Q3 - Q1
        
        multiplier = self.detection_params['statistical_outliers']['iqr_multiplier']
        lower_bound = Q1 - multiplier * IQR
        upper_bound = Q3 + multiplier * IQR
        
        outlier_mask = (data < lower_bound) | (data > upper_bound)
        outlier_indices = data.index[outlier_mask].tolist()
        
        # Calculate severity based on how far outside the bounds
        if len(outlier_indices) > 0:
            outlier_values = data[outlier_mask]
            max_deviation = max(
                abs(outlier_values.min() - lower_bound) / IQR if outlier_values.min() < lower_bound else 0,
                abs(outlier_values.max() - upper_bound) / IQR if outlier_values.max() > upper_bound else 0
            )
            severity = min(max_deviation / 5.0, 1.0)
        else:
            severity = 0.0
        
        return AnomalyResult(
            detector_name="iqr",
            anomaly_type="statistical_outlier",
            anomalous_records=outlier_indices,
            anomaly_count=len(outlier_indices),
            total_count=len(df),
            severity_score=severity,
            description=f"IQR outliers in {column} (multiplier: {multiplier})",
            details={
                'column': column,
                'Q1': float(Q1),
                'Q3': float(Q3),
                'IQR': float(IQR),
                'lower_bound': float(lower_bound),
                'upper_bound': float(upper_bound)
            }
        )
    
    def _detect_modified_z_score_outliers(self, df: pd.DataFrame, column: str) -> AnomalyResult:
        """Detect outliers using Modified Z-score method (more robust)"""
        data = df[column].dropna()
        
        if len(data) < 3:
            return AnomalyResult(
                detector_name="modified_z_score",
                anomaly_type="statistical_outlier",
                total_count=len(df),
                description=f"Insufficient data for Modified Z-score analysis in {column}"
            )
        
        median = data.median()
        mad = np.median(np.abs(data - median))  # Median Absolute Deviation
        
        if mad == 0:
            return AnomalyResult(
                detector_name="modified_z_score",
                anomaly_type="statistical_outlier",
                total_count=len(df),
                description=f"Zero MAD in {column}, cannot detect outliers"
            )
        
        modified_z_scores = 0.6745 * (data - median) / mad
        threshold = self.detection_params['statistical_outliers']['modified_z_score_threshold']
        
        outlier_indices = data.index[np.abs(modified_z_scores) > threshold].tolist()
        
        # Calculate severity
        max_modified_z = np.abs(modified_z_scores).max() if len(modified_z_scores) > 0 else 0
        severity = min(max_modified_z / (threshold * 2), 1.0)
        
        return AnomalyResult(
            detector_name="modified_z_score",
            anomaly_type="statistical_outlier",
            anomalous_records=outlier_indices,
            anomaly_count=len(outlier_indices),
            total_count=len(df),
            severity_score=severity,
            description=f"Modified Z-score outliers in {column} (threshold: {threshold})",
            details={
                'column': column,
                'threshold': threshold,
                'median': float(median),
                'mad': float(mad),
                'max_modified_z_score': float(max_modified_z)
            }
        )
    
    def detect_time_series_anomalies(self, df: pd.DataFrame, time_column: str, 
                                   value_columns: List[str] = None) -> List[AnomalyResult]:
        """Detect time series anomalies like spikes, drops, and trend changes"""
        results = []
        
        if time_column not in df.columns:
            return [AnomalyResult(
                detector_name="time_series",
                anomaly_type="time_series_anomaly",
                total_count=len(df),
                description=f"Time column {time_column} not found"
            )]
        
        if value_columns is None:
            value_columns = df.select_dtypes(include=[np.number]).columns.tolist()
            if time_column in value_columns:
                value_columns.remove(time_column)
        
        # Sort by time column
        df_sorted = df.sort_values(time_column)
        
        for column in value_columns:
            if column in df_sorted.columns:
                # Spike detection
                spike_result = self._detect_spikes(df_sorted, time_column, column)
                results.append(spike_result)
                
                # Trend change detection
                trend_result = self._detect_trend_changes(df_sorted, time_column, column)
                results.append(trend_result)
        
        return results
    
    def _detect_spikes(self, df: pd.DataFrame, time_column: str, value_column: str) -> AnomalyResult:
        """Detect sudden spikes or drops in time series data"""
        data = df[value_column].dropna()
        
        if len(data) < 3:
            return AnomalyResult(
                detector_name="spike_detection",
                anomaly_type="time_series_spike",
                total_count=len(df),
                description=f"Insufficient data for spike detection in {value_column}"
            )
        
        # Calculate rolling statistics
        window_size = min(7, len(data) // 3)  # Use 7-point window or 1/3 of data
        rolling_mean = data.rolling(window=window_size, center=True).mean()
        rolling_std = data.rolling(window=window_size, center=True).std()
        
        # Detect spikes
        spike_threshold = self.detection_params['time_series']['spike_threshold']
        drop_threshold = self.detection_params['time_series']['drop_threshold']
        
        z_scores = (data - rolling_mean) / rolling_std
        spike_mask = (z_scores > spike_threshold) | (z_scores < drop_threshold)
        
        anomalous_indices = data.index[spike_mask & ~z_scores.isna()].tolist()
        
        # Calculate severity
        if len(anomalous_indices) > 0:
            max_spike = np.abs(z_scores[spike_mask]).max()
            severity = min(max_spike / (spike_threshold * 2), 1.0)
        else:
            severity = 0.0
        
        return AnomalyResult(
            detector_name="spike_detection",
            anomaly_type="time_series_spike",
            anomalous_records=anomalous_indices,
            anomaly_count=len(anomalous_indices),
            total_count=len(df),
            severity_score=severity,
            description=f"Time series spikes/drops in {value_column}",
            details={
                'column': value_column,
                'spike_threshold': spike_threshold,
                'drop_threshold': drop_threshold,
                'window_size': window_size
            }
        )
    
    def _detect_trend_changes(self, df: pd.DataFrame, time_column: str, value_column: str) -> AnomalyResult:
        """Detect sudden trend changes in time series data"""
        data = df[value_column].dropna()
        
        if len(data) < 6:
            return AnomalyResult(
                detector_name="trend_change",
                anomaly_type="trend_change",
                total_count=len(df),
                description=f"Insufficient data for trend change detection in {value_column}"
            )
        
        # Calculate rolling correlation with time index
        window_size = min(10, len(data) // 2)
        time_index = np.arange(len(data))
        
        correlations = []
        for i in range(window_size, len(data) - window_size):
            window_data = data.iloc[i-window_size:i+window_size]
            window_time = time_index[i-window_size:i+window_size]
            
            if len(window_data) > 1 and window_data.std() > 0:
                corr = np.corrcoef(window_time, window_data)[0, 1]
                correlations.append((data.index[i], corr))
        
        # Detect trend changes
        threshold = self.detection_params['time_series']['trend_change_threshold']
        anomalous_indices = []
        
        for i in range(1, len(correlations)):
            prev_corr = correlations[i-1][1]
            curr_corr = correlations[i][1]
            
            if not (np.isnan(prev_corr) or np.isnan(curr_corr)):
                trend_change = abs(curr_corr - prev_corr)
                if trend_change > threshold:
                    anomalous_indices.append(correlations[i][0])
        
        # Calculate severity
        severity = min(len(anomalous_indices) / len(data), 1.0) if len(data) > 0 else 0.0
        
        return AnomalyResult(
            detector_name="trend_change",
            anomaly_type="trend_change",
            anomalous_records=anomalous_indices,
            anomaly_count=len(anomalous_indices),
            total_count=len(df),
            severity_score=severity,
            description=f"Trend changes in {value_column}",
            details={
                'column': value_column,
                'threshold': threshold,
                'window_size': window_size
            }
        )
    
    def detect_geographic_anomalies(self, df: pd.DataFrame, lat_column: str = 'latitude', 
                                  lon_column: str = 'longitude') -> List[AnomalyResult]:
        """Detect geographic anomalies including impossible locations and clustering"""
        results = []
        
        if lat_column not in df.columns or lon_column not in df.columns:
            return [AnomalyResult(
                detector_name="geographic",
                anomaly_type="geographic_anomaly",
                total_count=len(df),
                description=f"Required coordinate columns not found: {lat_column}, {lon_column}"
            )]
        
        # Impossible location detection
        impossible_result = self._detect_impossible_locations(df, lat_column, lon_column)
        results.append(impossible_result)
        
        # Coordinate precision anomalies
        precision_result = self._detect_coordinate_precision_anomalies(df, lat_column, lon_column)
        results.append(precision_result)
        
        # Geographic clustering anomalies
        clustering_result = self._detect_geographic_clustering_anomalies(df, lat_column, lon_column)
        results.append(clustering_result)
        
        return results
    
    def _detect_impossible_locations(self, df: pd.DataFrame, lat_column: str, lon_column: str) -> AnomalyResult:
        """Detect impossible geographic locations"""
        coord_df = df[[lat_column, lon_column]].dropna()
        
        if coord_df.empty:
            return AnomalyResult(
                detector_name="impossible_locations",
                anomaly_type="geographic_impossible",
                total_count=len(df),
                description="No coordinate data available"
            )
        
        anomalous_indices = []
        
        for idx, row in coord_df.iterrows():
            lat, lon = row[lat_column], row[lon_column]
            
            # Check for obviously impossible coordinates
            if (lat == 0 and lon == 0 and 
                'null_island' not in df.columns):  # Null Island check
                anomalous_indices.append(idx)
            elif abs(lat) > 90 or abs(lon) > 180:  # Out of valid range
                anomalous_indices.append(idx)
            elif lat == lon and abs(lat) < 1:  # Suspicious identical coordinates near origin
                anomalous_indices.append(idx)
            # Check for coordinates with too many decimal places (data entry errors)
            elif (len(str(lat).split('.')[-1]) > 8 or 
                  len(str(lon).split('.')[-1]) > 8):
                anomalous_indices.append(idx)
        
        severity = min(len(anomalous_indices) / len(coord_df), 1.0) if len(coord_df) > 0 else 0.0
        
        return AnomalyResult(
            detector_name="impossible_locations",
            anomaly_type="geographic_impossible",
            anomalous_records=anomalous_indices,
            anomaly_count=len(anomalous_indices),
            total_count=len(df),
            severity_score=severity,
            description="Impossible or suspicious geographic coordinates",
            details={
                'lat_column': lat_column,
                'lon_column': lon_column,
                'coord_records_checked': len(coord_df)
            }
        )
    
    def _detect_coordinate_precision_anomalies(self, df: pd.DataFrame, lat_column: str, lon_column: str) -> AnomalyResult:
        """Detect coordinate precision anomalies"""
        coord_df = df[[lat_column, lon_column]].dropna()
        
        if coord_df.empty:
            return AnomalyResult(
                detector_name="coordinate_precision",
                anomaly_type="precision_anomaly",
                total_count=len(df),
                description="No coordinate data available"
            )
        
        expected_precision = self.detection_params['geographic']['coordinate_precision']
        anomalous_indices = []
        
        for idx, row in coord_df.iterrows():
            lat, lon = row[lat_column], row[lon_column]
            
            # Check for rounded coordinates (potential data quality issue)
            lat_str = str(lat)
            lon_str = str(lon)
            
            # Check if coordinates are suspiciously rounded
            if ('.' not in lat_str or len(lat_str.split('.')[-1]) < 3) and lat != 0:
                anomalous_indices.append(idx)
            elif ('.' not in lon_str or len(lon_str.split('.')[-1]) < 3) and lon != 0:
                anomalous_indices.append(idx)
        
        severity = min(len(anomalous_indices) / len(coord_df) * 2, 1.0) if len(coord_df) > 0 else 0.0
        
        return AnomalyResult(
            detector_name="coordinate_precision",
            anomaly_type="precision_anomaly",
            anomalous_records=anomalous_indices,
            anomaly_count=len(anomalous_indices),
            total_count=len(df),
            severity_score=severity,
            description="Coordinate precision anomalies (overly rounded coordinates)",
            details={
                'expected_precision': expected_precision,
                'coord_records_checked': len(coord_df)
            }
        )
    
    def _detect_geographic_clustering_anomalies(self, df: pd.DataFrame, lat_column: str, lon_column: str) -> AnomalyResult:
        """Detect geographic clustering anomalies using DBSCAN"""
        coord_df = df[[lat_column, lon_column]].dropna()
        
        if len(coord_df) < 5:
            return AnomalyResult(
                detector_name="geographic_clustering",
                anomaly_type="clustering_anomaly",
                total_count=len(df),
                description="Insufficient coordinate data for clustering analysis"
            )
        
        try:
            # Standardize coordinates for clustering
            scaler = StandardScaler()
            coords_scaled = scaler.fit_transform(coord_df[[lat_column, lon_column]])
            
            # Apply DBSCAN clustering
            eps = self.detection_params['geographic']['dbscan_eps']
            min_samples = self.detection_params['geographic']['dbscan_min_samples']
            
            clustering = DBSCAN(eps=eps, min_samples=min_samples).fit(coords_scaled)
            labels = clustering.labels_
            
            # Find outliers (labeled as -1 by DBSCAN)
            outlier_mask = labels == -1
            anomalous_indices = coord_df.index[outlier_mask].tolist()
            
            # Calculate severity based on outlier percentage
            severity = min(len(anomalous_indices) / len(coord_df), 1.0) if len(coord_df) > 0 else 0.0
            
            return AnomalyResult(
                detector_name="geographic_clustering",
                anomaly_type="clustering_anomaly",
                anomalous_records=anomalous_indices,
                anomaly_count=len(anomalous_indices),
                total_count=len(df),
                severity_score=severity,
                description="Geographic clustering outliers",
                details={
                    'eps': eps,
                    'min_samples': min_samples,
                    'clusters_found': len(set(labels)) - (1 if -1 in labels else 0),
                    'coord_records_analyzed': len(coord_df)
                }
            )
            
        except Exception as e:
            return AnomalyResult(
                detector_name="geographic_clustering",
                anomaly_type="clustering_anomaly",
                total_count=len(df),
                description=f"Geographic clustering analysis failed: {str(e)}"
            )
    
    def detect_compliance_anomalies(self, df: pd.DataFrame) -> List[AnomalyResult]:
        """Detect compliance pattern anomalies"""
        results = []
        
        # Violations without inspections
        if all(col in df.columns for col in ['inspection_count', 'violation_count']):
            violations_result = self._detect_violations_without_inspections(df)
            results.append(violations_result)
        
        # Excessive penalties
        if 'penalty_amount' in df.columns:
            penalty_result = self._detect_excessive_penalties(df)
            results.append(penalty_result)
        
        # Compliance status inconsistencies
        if all(col in df.columns for col in ['compliance_status', 'violation_count']):
            consistency_result = self._detect_compliance_inconsistencies(df)
            results.append(consistency_result)
        
        return results
    
    def _detect_violations_without_inspections(self, df: pd.DataFrame) -> AnomalyResult:
        """Detect facilities with violations but no inspections"""
        anomalous_indices = []
        
        for idx, row in df.iterrows():
            inspection_count = row.get('inspection_count', 0)
            violation_count = row.get('violation_count', 0)
            
            # Convert to numeric, handle NaN
            try:
                inspections = float(inspection_count) if pd.notna(inspection_count) else 0
                violations = float(violation_count) if pd.notna(violation_count) else 0
                
                # Flag if violations > 0 but inspections = 0
                if violations > 0 and inspections == 0:
                    anomalous_indices.append(idx)
            except (ValueError, TypeError):
                continue
        
        severity = min(len(anomalous_indices) / len(df), 1.0) if len(df) > 0 else 0.0
        
        return AnomalyResult(
            detector_name="violations_without_inspections",
            anomaly_type="compliance_anomaly",
            anomalous_records=anomalous_indices,
            anomaly_count=len(anomalous_indices),
            total_count=len(df),
            severity_score=severity,
            description="Facilities with violations but no recorded inspections"
        )
    
    def _detect_excessive_penalties(self, df: pd.DataFrame) -> AnomalyResult:
        """Detect excessive penalty amounts"""
        penalty_data = df['penalty_amount'].dropna()
        
        if penalty_data.empty:
            return AnomalyResult(
                detector_name="excessive_penalties",
                anomaly_type="compliance_anomaly",
                total_count=len(df),
                description="No penalty data available"
            )
        
        threshold = self.detection_params['compliance']['excessive_penalty_threshold']
        anomalous_indices = penalty_data.index[penalty_data > threshold].tolist()
        
        severity = min(len(anomalous_indices) / len(penalty_data), 1.0) if len(penalty_data) > 0 else 0.0
        
        return AnomalyResult(
            detector_name="excessive_penalties",
            anomaly_type="compliance_anomaly",
            anomalous_records=anomalous_indices,
            anomaly_count=len(anomalous_indices),
            total_count=len(df),
            severity_score=severity,
            description=f"Excessive penalty amounts (>${threshold:,} threshold)",
            details={'threshold': threshold}
        )
    
    def _detect_compliance_inconsistencies(self, df: pd.DataFrame) -> AnomalyResult:
        """Detect compliance status inconsistencies"""
        anomalous_indices = []
        
        for idx, row in df.iterrows():
            compliance_status = row.get('compliance_status', '')
            violation_count = row.get('violation_count', 0)
            
            try:
                violations = float(violation_count) if pd.notna(violation_count) else 0
                status = str(compliance_status).upper() if pd.notna(compliance_status) else ''
                
                # Flag inconsistencies
                if violations > 0 and status in ['COMPLIANT', 'GOOD', 'CLEAN']:
                    anomalous_indices.append(idx)
                elif violations == 0 and status in ['NON-COMPLIANT', 'VIOLATION', 'SNC']:
                    anomalous_indices.append(idx)
            except (ValueError, TypeError):
                continue
        
        severity = min(len(anomalous_indices) / len(df), 1.0) if len(df) > 0 else 0.0
        
        return AnomalyResult(
            detector_name="compliance_inconsistencies",
            anomaly_type="compliance_anomaly",
            anomalous_records=anomalous_indices,
            anomaly_count=len(anomalous_indices),
            total_count=len(df),
            severity_score=severity,
            description="Compliance status inconsistent with violation counts"
        )
    
    def detect_emission_anomalies(self, df: pd.DataFrame) -> List[AnomalyResult]:
        """Detect emission-related anomalies"""
        results = []
        
        # Zero emissions anomalies
        if 'annual_emission_amount' in df.columns:
            zero_result = self._detect_zero_emission_anomalies(df)
            results.append(zero_result)
        
        # Emission spikes
        if all(col in df.columns for col in ['annual_emission_amount', 'reporting_year']):
            spike_result = self._detect_emission_spikes(df)
            results.append(spike_result)
        
        return results
    
    def _detect_zero_emission_anomalies(self, df: pd.DataFrame) -> AnomalyResult:
        """Detect suspicious zero emissions"""
        emission_data = df['annual_emission_amount'].dropna()
        
        if emission_data.empty:
            return AnomalyResult(
                detector_name="zero_emissions",
                anomaly_type="emission_anomaly",
                total_count=len(df),
                description="No emission data available"
            )
        
        threshold = self.detection_params['emission']['zero_emission_threshold']
        zero_indices = emission_data.index[emission_data <= threshold].tolist()
        
        # High zero rate might indicate data quality issues
        zero_rate = len(zero_indices) / len(emission_data)
        severity = min(zero_rate * 2, 1.0) if zero_rate > 0.5 else 0.0  # Flag if >50% zeros
        
        return AnomalyResult(
            detector_name="zero_emissions",
            anomaly_type="emission_anomaly",
            anomalous_records=zero_indices if zero_rate > 0.5 else [],
            anomaly_count=len(zero_indices) if zero_rate > 0.5 else 0,
            total_count=len(df),
            severity_score=severity,
            description=f"High rate of zero/near-zero emissions ({zero_rate:.1%})",
            details={'threshold': threshold, 'zero_rate': zero_rate}
        )
    
    def _detect_emission_spikes(self, df: pd.DataFrame) -> AnomalyResult:
        """Detect emission spikes by facility over time"""
        if 'facility_id' not in df.columns:
            return AnomalyResult(
                detector_name="emission_spikes",
                anomaly_type="emission_anomaly",
                total_count=len(df),
                description="No facility_id column for spike analysis"
            )
        
        anomalous_indices = []
        threshold = self.detection_params['emission']['emission_spike_threshold']
        
        # Group by facility and analyze time series
        for facility_id, group in df.groupby('facility_id'):
            if len(group) < 3:
                continue
            
            group_sorted = group.sort_values('reporting_year')
            emissions = group_sorted['annual_emission_amount'].dropna()
            
            if len(emissions) < 3:
                continue
            
            # Calculate year-over-year changes
            emission_changes = emissions.pct_change().dropna()
            
            # Flag spikes (large percentage increases)
            spike_mask = emission_changes > threshold
            spike_indices = emissions.index[spike_mask].tolist()
            anomalous_indices.extend(spike_indices)
        
        severity = min(len(anomalous_indices) / len(df), 1.0) if len(df) > 0 else 0.0
        
        return AnomalyResult(
            detector_name="emission_spikes",
            anomaly_type="emission_anomaly",
            anomalous_records=anomalous_indices,
            anomaly_count=len(anomalous_indices),
            total_count=len(df),
            severity_score=severity,
            description=f"Emission spikes (>{threshold*100:.0f}% increase)",
            details={'threshold': threshold}
        )
    
    def run_all_anomaly_detection(self, df: pd.DataFrame, table_type: str) -> List[AnomalyResult]:
        """Run all anomaly detection methods for a dataset"""
        self.logger.info(f"Running anomaly detection for {table_type} with {len(df)} records")
        
        all_results = []
        
        # Statistical outliers
        numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
        if numeric_columns:
            statistical_results = self.detect_statistical_outliers(df, numeric_columns[:5])  # Limit to first 5 numeric columns
            all_results.extend(statistical_results)
        
        # Time series anomalies
        time_columns = [col for col in df.columns if 'year' in col.lower() or 'date' in col.lower()]
        if time_columns and numeric_columns:
            time_results = self.detect_time_series_anomalies(df, time_columns[0], numeric_columns[:3])
            all_results.extend(time_results)
        
        # Geographic anomalies
        lat_columns = [col for col in df.columns if 'lat' in col.lower()]
        lon_columns = [col for col in df.columns if 'lon' in col.lower() or 'long' in col.lower()]
        if lat_columns and lon_columns:
            geo_results = self.detect_geographic_anomalies(df, lat_columns[0], lon_columns[0])
            all_results.extend(geo_results)
        
        # Compliance anomalies
        compliance_results = self.detect_compliance_anomalies(df)
        all_results.extend(compliance_results)
        
        # Emission anomalies
        emission_results = self.detect_emission_anomalies(df)
        all_results.extend(emission_results)
        
        # Store results
        self.anomaly_results.extend(all_results)
        
        self.logger.info(f"Completed {len(all_results)} anomaly detection checks for {table_type}")
        
        return all_results
    
    def get_anomaly_summary(self) -> Dict[str, Any]:
        """Get comprehensive anomaly detection summary"""
        if not self.anomaly_results:
            return {'error': 'No anomaly detection results available'}
        
        total_detectors = len(self.anomaly_results)
        detectors_with_anomalies = sum(1 for r in self.anomaly_results if r.anomaly_count > 0)
        
        total_records = sum(r.total_count for r in self.anomaly_results) // total_detectors if total_detectors > 0 else 0
        total_anomalies = sum(r.anomaly_count for r in self.anomaly_results)
        
        severity_scores = [r.severity_score for r in self.anomaly_results if r.severity_score > 0]
        avg_severity = np.mean(severity_scores) if severity_scores else 0.0
        
        return {
            'total_anomaly_detectors': total_detectors,
            'detectors_with_anomalies': detectors_with_anomalies,
            'total_records_analyzed': total_records,
            'total_anomalies_found': total_anomalies,
            'overall_anomaly_rate': (total_anomalies / total_records * 100) if total_records > 0 else 0,
            'average_severity_score': avg_severity,
            'anomaly_results': [
                {
                    'detector_name': r.detector_name,
                    'anomaly_type': r.anomaly_type,
                    'anomaly_count': r.anomaly_count,
                    'anomaly_rate': r.anomaly_rate,
                    'severity_score': r.severity_score,
                    'description': r.description
                }
                for r in self.anomaly_results
            ]
        }
    
    def clear_results(self):
        """Clear anomaly detection results"""
        self.anomaly_results.clear()
        self.logger.info("Anomaly detection results cleared")
