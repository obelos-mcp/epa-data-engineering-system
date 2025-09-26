"""
Environmental Site Data Management & QA/QC Automation System
Validation Rules Engine

This module provides comprehensive data validation rules based on EPA standards
including required field validation, data type validation, business rules,
and cross-field validation.
import pandas as pd
import numpy as np
import re
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import logging
class ValidationSeverity(Enum):
    """Validation severity levels"""
    CRITICAL = "CRITICAL"
    WARNING = "WARNING" 
    INFO = "INFO"
@dataclass
class ValidationResult:
    """Result of a validation check"""
    rule_name: str
    severity: ValidationSeverity
    passed: bool
    failed_count: int
    total_count: int
    error_rate: float
    failed_records: List[int] = field(default_factory=list)
    error_message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Calculate error rate after initialization"""
        if self.total_count > 0:
            self.error_rate = (self.failed_count / self.total_count) * 100
        else:
            self.error_rate = 0.0
class ValidationRulesEngine:
    """Comprehensive validation rules engine for EPA environmental data"""
    def __init__(self, log_level: str = 'INFO'):
        self.logger = self._setup_logging(log_level)
        self.validation_results: List[ValidationResult] = []
        
        # EPA standards and reference data
        self.epa_standards = self._initialize_epa_standards()
        # Validation rule registry
        self.validation_rules = self._initialize_validation_rules()
        self.logger.info("ValidationRulesEngine initialized")
    def _setup_logging(self, log_level: str):
        """Setup logging for validation engine"""
        logger = logging.getLogger('validation_rules')
        logger.setLevel(getattr(logging, log_level.upper()))
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger
    def _initialize_epa_standards(self) -> Dict[str, Any]:
        """Initialize EPA standards and reference data"""
        return {
            'coordinate_bounds': {
                'latitude': {'min': -90.0, 'max': 90.0},
                'longitude': {'min': -180.0, 'max': 180.0}
            },
            'epa_regions': list(range(1, 11)),  # EPA regions 1-10
            'state_codes': [
                'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 
                'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 
                'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 
                'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 
                'WI', 'WY', 'DC', 'PR', 'VI', 'GU', 'AS', 'MP'
            ],
            'ph_range': {'min': 0.0, 'max': 14.0},
            'permit_status_codes': ['ADC', 'EFF', 'EXP', 'NON', 'TRM', 'RET'],
            'major_minor_flags': ['Y', 'N', 'M'],
            'compliance_status_codes': ['SNC', 'RNC', 'VIO', 'NOV', 'COM'],
            'date_ranges': {
                'historical_start': datetime(1970, 1, 1),
                'future_limit': datetime.now() + timedelta(days=365)  # Allow 1 year in future
            'emission_ranges': {
                'annual_emission_max': 1000000000,  # 1 billion units max
                'flow_rate_max': 10000  # MGD
            'npdes_id_pattern': r'^[A-Z]{2}[0-9A-Z]+$',
            'registry_id_pattern': r'^\d{11,12}$'
        }
    def _initialize_validation_rules(self) -> Dict[str, Dict[str, Any]]:
        """Initialize validation rule definitions"""
            'required_fields': {
                'facilities': ['registry_id', 'facility_name', 'state_code'],
                'permits': ['npdes_id', 'external_permit_number', 'permit_type_code'],
                'water_measurements': ['npdes_id', 'measurement_year'],
                'air_emissions': ['registry_id', 'reporting_year', 'pollutant_name'],
                'compliance_history': ['facility_id', 'program_type']
            'data_type_rules': {
                'numeric_fields': [
                    'registry_id', 'latitude', 'longitude', 'epa_region',
                    'annual_emission_amount', 'total_design_flow_mgd',
                    'measurement_year', 'reporting_year'
                ],
                'date_fields': [
                    'effective_date', 'expiration_date', 'last_inspection_date',
                    'created_date', 'updated_date'
                'string_fields': [
                    'facility_name', 'state_code', 'npdes_id', 'pollutant_name'
                ]
            'range_validations': {
                'coordinate_fields': ['latitude', 'longitude'],
                'epa_region_fields': ['epa_region'],
                'year_fields': ['measurement_year', 'reporting_year'],
                'ph_fields': ['ph_value', 'ph_level'],
                'emission_fields': ['annual_emission_amount'],
                'flow_fields': ['total_design_flow_mgd', 'actual_average_flow_mgd']
            }
    def validate_required_fields(self, df: pd.DataFrame, table_type: str) -> ValidationResult:
        """Validate required fields are present and not null"""
        rule_name = f"required_fields_{table_type}"
        if table_type not in self.validation_rules['required_fields']:
            return ValidationResult(
                rule_name=rule_name,
                severity=ValidationSeverity.WARNING,
                passed=True,
                failed_count=0,
                total_count=len(df),
                error_rate=0.0,
                error_message=f"No required field rules defined for {table_type}"
        required_fields = self.validation_rules['required_fields'][table_type]
        failed_records = []
        total_failures = 0
        for idx, row in df.iterrows():
            row_failures = []
            for field in required_fields:
                if field in df.columns:
                    if pd.isna(row[field]) or str(row[field]).strip() == '':
                        row_failures.append(field)
                else:
                    row_failures.append(f"{field} (missing column)")
            
            if row_failures:
                failed_records.append(idx)
                total_failures += len(row_failures)
        passed = len(failed_records) == 0
        severity = ValidationSeverity.CRITICAL if not passed else ValidationSeverity.INFO
        return ValidationResult(
            rule_name=rule_name,
            severity=severity,
            passed=passed,
            failed_count=len(failed_records),
            total_count=len(df),
            error_rate=0.0,
            failed_records=failed_records,
            error_message=f"Missing required fields: {required_fields}" if not passed else "",
            details={'required_fields': required_fields, 'total_field_failures': total_failures}
        )
    def validate_data_types(self, df: pd.DataFrame) -> List[ValidationResult]:
        """Validate data types for numeric, date, and string fields"""
        results = []
        # Validate numeric fields
        for field in self.validation_rules['data_type_rules']['numeric_fields']:
            if field in df.columns:
                result = self._validate_numeric_field(df, field)
                results.append(result)
        # Validate date fields
        for field in self.validation_rules['data_type_rules']['date_fields']:
                result = self._validate_date_field(df, field)
        # Validate string fields
        for field in self.validation_rules['data_type_rules']['string_fields']:
                result = self._validate_string_field(df, field)
        return results
    def _validate_numeric_field(self, df: pd.DataFrame, field: str) -> ValidationResult:
        """Validate numeric field data type"""
        rule_name = f"numeric_type_{field}"
        for idx, value in df[field].items():
            if pd.notna(value):
                try:
                    float(value)
                except (ValueError, TypeError):
                    failed_records.append(idx)
        severity = ValidationSeverity.WARNING if not passed else ValidationSeverity.INFO
            error_message=f"Invalid numeric values in {field}" if not passed else ""
    def _validate_date_field(self, df: pd.DataFrame, field: str) -> ValidationResult:
        """Validate date field data type and range"""
        rule_name = f"date_type_{field}"
                    parsed_date = pd.to_datetime(value, errors='raise')
                    # Check if date is in reasonable range
                    if parsed_date < self.epa_standards['date_ranges']['historical_start']:
                        failed_records.append(idx)
                    elif parsed_date > self.epa_standards['date_ranges']['future_limit']:
            error_message=f"Invalid dates in {field}" if not passed else ""
    def _validate_string_field(self, df: pd.DataFrame, field: str) -> ValidationResult:
        """Validate string field format and patterns"""
        rule_name = f"string_format_{field}"
                str_value = str(value).strip()
                
                # Special validation for specific fields
                if field == 'npdes_id':
                    if not re.match(self.epa_standards['npdes_id_pattern'], str_value):
                elif field == 'registry_id':
                    if not re.match(self.epa_standards['registry_id_pattern'], str_value):
                elif field == 'state_code':
                    if str_value.upper() not in self.epa_standards['state_codes']:
                elif len(str_value) == 0:  # Empty strings
            error_message=f"Invalid format in {field}" if not passed else ""
    def validate_business_rules(self, df: pd.DataFrame) -> List[ValidationResult]:
        """Validate EPA business rules"""
        # Coordinate bounds validation
        if 'latitude' in df.columns:
            results.append(self._validate_coordinate_bounds(df, 'latitude'))
        if 'longitude' in df.columns:
            results.append(self._validate_coordinate_bounds(df, 'longitude'))
        # EPA region validation
        if 'epa_region' in df.columns:
            results.append(self._validate_epa_regions(df))
        # State code validation
        if 'state_code' in df.columns:
            results.append(self._validate_state_codes(df))
        # Year validation
        for year_field in ['measurement_year', 'reporting_year']:
            if year_field in df.columns:
                results.append(self._validate_year_range(df, year_field))
        # Emission range validation
        if 'annual_emission_amount' in df.columns:
            results.append(self._validate_emission_ranges(df))
        # Flow rate validation
        for flow_field in ['total_design_flow_mgd', 'actual_average_flow_mgd']:
            if flow_field in df.columns:
                results.append(self._validate_flow_ranges(df, flow_field))
    def _validate_coordinate_bounds(self, df: pd.DataFrame, coord_field: str) -> ValidationResult:
        """Validate coordinate bounds"""
        rule_name = f"coordinate_bounds_{coord_field}"
        if coord_field == 'latitude':
            bounds = self.epa_standards['coordinate_bounds']['latitude']
            bounds = self.epa_standards['coordinate_bounds']['longitude']
        for idx, value in df[coord_field].items():
                    coord_value = float(value)
                    if coord_value < bounds['min'] or coord_value > bounds['max']:
            error_message=f"Coordinates out of bounds in {coord_field}" if not passed else "",
            details={'bounds': bounds}
    def _validate_epa_regions(self, df: pd.DataFrame) -> ValidationResult:
        """Validate EPA region codes"""
        rule_name = "epa_regions"
        for idx, value in df['epa_region'].items():
                    region = int(value)
                    if region not in self.epa_standards['epa_regions']:
            error_message="Invalid EPA regions" if not passed else "",
            details={'valid_regions': self.epa_standards['epa_regions']}
    def _validate_state_codes(self, df: pd.DataFrame) -> ValidationResult:
        """Validate state codes"""
        rule_name = "state_codes"
        for idx, value in df['state_code'].items():
                state_code = str(value).strip().upper()
                if state_code not in self.epa_standards['state_codes']:
            error_message="Invalid state codes" if not passed else ""
    def _validate_year_range(self, df: pd.DataFrame, year_field: str) -> ValidationResult:
        """Validate year ranges"""
        rule_name = f"year_range_{year_field}"
        current_year = datetime.now().year
        min_year = 1970
        for idx, value in df[year_field].items():
                    year = int(value)
                    if year < min_year or year > current_year:
            error_message=f"Invalid year range in {year_field}" if not passed else "",
            details={'valid_range': {'min': min_year, 'max': current_year}}
    def _validate_emission_ranges(self, df: pd.DataFrame) -> ValidationResult:
        """Validate emission value ranges"""
        rule_name = "emission_ranges"
        max_emission = self.epa_standards['emission_ranges']['annual_emission_max']
        for idx, value in df['annual_emission_amount'].items():
                    emission = float(value)
                    if emission < 0 or emission > max_emission:
            error_message="Invalid emission ranges" if not passed else "",
            details={'max_emission': max_emission}
    def _validate_flow_ranges(self, df: pd.DataFrame, flow_field: str) -> ValidationResult:
        """Validate flow rate ranges"""
        rule_name = f"flow_ranges_{flow_field}"
        max_flow = self.epa_standards['emission_ranges']['flow_rate_max']
        for idx, value in df[flow_field].items():
                    flow = float(value)
                    if flow < 0 or flow > max_flow:
            error_message=f"Invalid flow ranges in {flow_field}" if not passed else "",
            details={'max_flow': max_flow}
    def validate_cross_field_rules(self, df: pd.DataFrame) -> List[ValidationResult]:
        """Validate cross-field business rules"""
        # Permit date logic: effective_date < expiration_date
        if 'effective_date' in df.columns and 'expiration_date' in df.columns:
            results.append(self._validate_permit_date_logic(df))
        # Coordinate consistency (both lat and lon should be present or both null)
        if 'latitude' in df.columns and 'longitude' in df.columns:
            results.append(self._validate_coordinate_consistency(df))
        # Flow rate consistency (actual <= design flow)
        if 'actual_average_flow_mgd' in df.columns and 'total_design_flow_mgd' in df.columns:
            results.append(self._validate_flow_consistency(df))
    def _validate_permit_date_logic(self, df: pd.DataFrame) -> ValidationResult:
        """Validate permit date logic"""
        rule_name = "permit_date_logic"
            effective_date = row.get('effective_date')
            expiration_date = row.get('expiration_date')
            if pd.notna(effective_date) and pd.notna(expiration_date):
                    effective_dt = pd.to_datetime(effective_date)
                    expiration_dt = pd.to_datetime(expiration_date)
                    
                    if effective_dt >= expiration_dt:
            error_message="Permit effective date >= expiration date" if not passed else ""
    def _validate_coordinate_consistency(self, df: pd.DataFrame) -> ValidationResult:
        """Validate coordinate consistency"""
        rule_name = "coordinate_consistency"
            lat = row.get('latitude')
            lon = row.get('longitude')
            # Both should be present or both should be null
            lat_present = pd.notna(lat)
            lon_present = pd.notna(lon)
            if lat_present != lon_present:
            error_message="Inconsistent coordinate data (lat without lon or vice versa)" if not passed else ""
    def _validate_flow_consistency(self, df: pd.DataFrame) -> ValidationResult:
        """Validate flow rate consistency"""
        rule_name = "flow_consistency"
            actual_flow = row.get('actual_average_flow_mgd')
            design_flow = row.get('total_design_flow_mgd')
            if pd.notna(actual_flow) and pd.notna(design_flow):
                    actual = float(actual_flow)
                    design = float(design_flow)
                    if actual > design:
            error_message="Actual flow exceeds design flow" if not passed else ""
    def run_all_validations(self, df: pd.DataFrame, table_type: str) -> List[ValidationResult]:
        """Run all validation rules for a dataset"""
        self.logger.info(f"Running all validations for {table_type} with {len(df)} records")
        all_results = []
        # Required fields validation
        result = self.validate_required_fields(df, table_type)
        all_results.append(result)
        # Data type validations
        type_results = self.validate_data_types(df)
        all_results.extend(type_results)
        # Business rule validations
        business_results = self.validate_business_rules(df)
        all_results.extend(business_results)
        # Cross-field validations
        cross_field_results = self.validate_cross_field_rules(df)
        all_results.extend(cross_field_results)
        # Store results
        self.validation_results.extend(all_results)
        self.logger.info(f"Completed {len(all_results)} validation checks for {table_type}")
        return all_results
    def get_validation_summary(self) -> Dict[str, Any]:
        """Get comprehensive validation summary"""
        if not self.validation_results:
            return {'error': 'No validation results available'}
        total_checks = len(self.validation_results)
        passed_checks = sum(1 for r in self.validation_results if r.passed)
        failed_checks = total_checks - passed_checks
        severity_counts = {
            'CRITICAL': sum(1 for r in self.validation_results if r.severity == ValidationSeverity.CRITICAL),
            'WARNING': sum(1 for r in self.validation_results if r.severity == ValidationSeverity.WARNING),
            'INFO': sum(1 for r in self.validation_results if r.severity == ValidationSeverity.INFO)
        total_records = sum(r.total_count for r in self.validation_results) // total_checks if total_checks > 0 else 0
        total_failures = sum(r.failed_count for r in self.validation_results)
        overall_error_rate = (failed_checks / total_checks * 100) if total_checks > 0 else 0
            'total_validation_checks': total_checks,
            'passed_checks': passed_checks,
            'failed_checks': failed_checks,
            'overall_pass_rate': (passed_checks / total_checks * 100) if total_checks > 0 else 0,
            'overall_error_rate': overall_error_rate,
            'severity_breakdown': severity_counts,
            'total_records_validated': total_records,
            'total_record_failures': total_failures,
            'record_failure_rate': (total_failures / total_records * 100) if total_records > 0 else 0,
            'validation_results': [
                {
                    'rule_name': r.rule_name,
                    'severity': r.severity.value,
                    'passed': r.passed,
                    'error_rate': r.error_rate,
                    'failed_count': r.failed_count,
                    'total_count': r.total_count,
                    'error_message': r.error_message
                }
                for r in self.validation_results
            ]
    def clear_results(self):
        """Clear validation results"""
        self.validation_results.clear()
        self.logger.info("Validation results cleared")
