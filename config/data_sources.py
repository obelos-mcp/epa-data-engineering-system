"""
Environmental Site Data Management & QA/QC Automation System
Data Sources Configuration

This module contains metadata and configuration for all EPA environmental datasets
including file paths, expected schemas, validation rules, and processing parameters.
"""

from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime
import pandas as pd

@dataclass
class DataSourceConfig:
    """Configuration for a single data source file"""
    name: str
    file_path: str
    sample_file_path: str
    staging_table: str
    description: str
    expected_columns: List[str]
    required_columns: List[str]
    data_types: Dict[str, str]
    date_columns: List[str]
    numeric_columns: List[str]
    coordinate_columns: List[str]
    chunk_size: int
    encoding: str
    delimiter: str
    skip_rows: int
    max_file_size_gb: float
    validation_rules: Dict[str, Any]

class DataSourceRegistry:
    """Registry of all EPA environmental data sources"""
    
    def __init__(self, project_root: str):
        self.project_root = Path(project_root)
        self.data_dir = self.project_root
        self.samples_dir = self.project_root / "data" / "samples"
        
        # Initialize data source configurations
        self._initialize_sources()
    
    def _initialize_sources(self):
        """Initialize all data source configurations"""
        
        self.sources = {
            'echo_facilities': DataSourceConfig(
                name='ECHO_EXPORTER',
                file_path=str(self.data_dir / 'ECHO_EXPORTER.csv'),
                sample_file_path=str(self.samples_dir / 'sample_ECHO_EXPORTER.csv'),
                staging_table='staging.echo_facilities',
                description='EPA ECHO facility master data with comprehensive facility information',
                expected_columns=[
                    'REGISTRY_ID', 'FAC_NAME', 'FAC_STREET', 'FAC_CITY', 'FAC_STATE', 'FAC_ZIP',
                    'FAC_COUNTY', 'FAC_FIPS_CODE', 'FAC_EPA_REGION', 'FAC_LAT', 'FAC_LONG',
                    'FAC_ACTIVE_FLAG', 'FAC_MAJOR_FLAG', 'NPDES_FLAG', 'AIR_FLAG', 'RCRA_FLAG',
                    'TRI_FLAG', 'GHG_FLAG', 'FAC_INSPECTION_COUNT', 'FAC_COMPLIANCE_STATUS'
                ],
                required_columns=[
                    'REGISTRY_ID', 'FAC_NAME', 'FAC_STATE', 'FAC_EPA_REGION'
                ],
                data_types={
                    'REGISTRY_ID': 'numeric',
                    'FAC_NAME': 'string',
                    'FAC_STREET': 'string',
                    'FAC_CITY': 'string', 
                    'FAC_STATE': 'string',
                    'FAC_ZIP': 'string',
                    'FAC_COUNTY': 'string',
                    'FAC_FIPS_CODE': 'numeric',
                    'FAC_EPA_REGION': 'numeric',
                    'FAC_LAT': 'numeric',
                    'FAC_LONG': 'numeric',
                    'FAC_PERCENT_MINORITY': 'numeric',
                    'FAC_POP_DEN': 'numeric',
                    'FAC_INSPECTION_COUNT': 'integer',
                    'FAC_TOTAL_PENALTIES': 'numeric',
                    'FAC_PENALTY_COUNT': 'integer'
                },
                date_columns=[
                    'FAC_DATE_LAST_INSPECTION', 'FAC_DATE_LAST_FORMAL_ACTION',
                    'FAC_DATE_LAST_PENALTY', 'CAA_DATE_LAST_FORMAL_ACTION',
                    'CWA_DATE_LAST_FORMAL_ACTION', 'RCRA_DATE_LAST_FORMAL_ACTION'
                ],
                numeric_columns=[
                    'REGISTRY_ID', 'FAC_FIPS_CODE', 'FAC_EPA_REGION', 'FAC_LAT', 'FAC_LONG',
                    'FAC_PERCENT_MINORITY', 'FAC_POP_DEN', 'FAC_TOTAL_PENALTIES'
                ],
                coordinate_columns=['FAC_LAT', 'FAC_LONG'],
                chunk_size=25000,  # Smaller chunks for large file with many columns
                encoding='utf-8',
                delimiter=',',
                skip_rows=0,
                max_file_size_gb=2.5,
                validation_rules={
                    'coordinate_bounds': {'lat_min': -90, 'lat_max': 90, 'lon_min': -180, 'lon_max': 180},
                    'epa_region_range': {'min': 1, 'max': 10},
                    'state_codes': ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC', 'PR', 'VI', 'GU', 'AS', 'MP'],
                    'boolean_flags': ['Y', 'N', '', None],
                    'max_penalty_amount': 50000000  # $50M reasonable max
                }
            ),
            
            'icis_facilities': DataSourceConfig(
                name='ICIS_FACILITIES',
                file_path=str(self.data_dir / 'ICIS_FACILITIES.csv'),
                sample_file_path=str(self.samples_dir / 'sample_ICIS_FACILITIES.csv'),
                staging_table='staging.icis_facilities',
                description='ICIS facility details for NPDES water discharge facilities',
                expected_columns=[
                    'ICIS_FACILITY_INTEREST_ID', 'NPDES_ID', 'FACILITY_UIN', 'FACILITY_TYPE_CODE',
                    'FACILITY_NAME', 'LOCATION_ADDRESS', 'CITY', 'COUNTY_CODE', 'STATE_CODE', 'ZIP',
                    'GEOCODE_LATITUDE', 'GEOCODE_LONGITUDE', 'IMPAIRED_WATERS'
                ],
                required_columns=[
                    'NPDES_ID', 'FACILITY_NAME', 'STATE_CODE'
                ],
                data_types={
                    'ICIS_FACILITY_INTEREST_ID': 'integer',
                    'NPDES_ID': 'string',
                    'FACILITY_UIN': 'numeric',
                    'FACILITY_TYPE_CODE': 'string',
                    'FACILITY_NAME': 'string',
                    'LOCATION_ADDRESS': 'string',
                    'CITY': 'string',
                    'COUNTY_CODE': 'string',
                    'STATE_CODE': 'string',
                    'ZIP': 'string',
                    'GEOCODE_LATITUDE': 'numeric',
                    'GEOCODE_LONGITUDE': 'numeric',
                    'IMPAIRED_WATERS': 'string'
                },
                date_columns=[],
                numeric_columns=['FACILITY_UIN', 'GEOCODE_LATITUDE', 'GEOCODE_LONGITUDE'],
                coordinate_columns=['GEOCODE_LATITUDE', 'GEOCODE_LONGITUDE'],
                chunk_size=50000,  # Larger chunks for smaller file
                encoding='utf-8',
                delimiter=',',
                skip_rows=0,
                max_file_size_gb=0.5,
                validation_rules={
                    'coordinate_bounds': {'lat_min': -90, 'lat_max': 90, 'lon_min': -180, 'lon_max': 180},
                    'npdes_id_pattern': r'^[A-Z]{2}[0-9A-Z]+$',  # State code + alphanumeric
                    'state_codes': ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC', 'PR', 'VI', 'GU', 'AS', 'MP']
                }
            ),
            
            'icis_permits': DataSourceConfig(
                name='ICIS_PERMITS',
                file_path=str(self.data_dir / 'ICIS_PERMITS.csv'),
                sample_file_path=str(self.samples_dir / 'sample_ICIS_PERMITS.csv'),
                staging_table='staging.icis_permits',
                description='ICIS water discharge permits and authorizations',
                expected_columns=[
                    'ACTIVITY_ID', 'EXTERNAL_PERMIT_NMBR', 'VERSION_NMBR', 'FACILITY_TYPE_INDICATOR',
                    'PERMIT_TYPE_CODE', 'MAJOR_MINOR_STATUS_FLAG', 'PERMIT_STATUS_CODE',
                    'TOTAL_DESIGN_FLOW_NMBR', 'ACTUAL_AVERAGE_FLOW_NMBR', 'STATE_WATER_BODY',
                    'PERMIT_NAME', 'AGENCY_TYPE_CODE', 'ORIGINAL_ISSUE_DATE', 'ISSUE_DATE',
                    'EFFECTIVE_DATE', 'EXPIRATION_DATE', 'RETIREMENT_DATE', 'TERMINATION_DATE'
                ],
                required_columns=[
                    'EXTERNAL_PERMIT_NMBR', 'PERMIT_TYPE_CODE', 'PERMIT_STATUS_CODE'
                ],
                data_types={
                    'ACTIVITY_ID': 'integer',
                    'EXTERNAL_PERMIT_NMBR': 'string',
                    'VERSION_NMBR': 'integer',
                    'FACILITY_TYPE_INDICATOR': 'string',
                    'PERMIT_TYPE_CODE': 'string',
                    'MAJOR_MINOR_STATUS_FLAG': 'string',
                    'PERMIT_STATUS_CODE': 'string',
                    'TOTAL_DESIGN_FLOW_NMBR': 'numeric',
                    'ACTUAL_AVERAGE_FLOW_NMBR': 'numeric',
                    'STATE_WATER_BODY': 'string',
                    'PERMIT_NAME': 'string',
                    'AGENCY_TYPE_CODE': 'string'
                },
                date_columns=[
                    'ORIGINAL_ISSUE_DATE', 'ISSUE_DATE', 'EFFECTIVE_DATE', 
                    'EXPIRATION_DATE', 'RETIREMENT_DATE', 'TERMINATION_DATE'
                ],
                numeric_columns=['TOTAL_DESIGN_FLOW_NMBR', 'ACTUAL_AVERAGE_FLOW_NMBR'],
                coordinate_columns=[],
                chunk_size=40000,
                encoding='utf-8',
                delimiter=',',
                skip_rows=0,
                max_file_size_gb=1.0,
                validation_rules={
                    'permit_status_codes': ['ADC', 'EFF', 'EXP', 'NON', 'TRM', 'RET'],
                    'major_minor_flags': ['Y', 'N', 'M'],
                    'flow_range': {'min': 0, 'max': 10000},  # MGD reasonable range
                    'date_order': 'effective_date <= expiration_date'
                }
            ),
            
            'npdes_measurements': DataSourceConfig(
                name='NPDES_QNCR_HISTORY',
                file_path=str(self.data_dir / 'NPDES_QNCR_HISTORY.csv'),
                sample_file_path=str(self.samples_dir / 'sample_NPDES_QNCR_HISTORY.csv'),
                staging_table='staging.npdes_measurements',
                description='NPDES quarterly water quality measurements and compliance data',
                expected_columns=[
                    'NPDES_ID', 'YEARQTR', 'HLRNC', 'NUME90Q', 'NUMCVDT', 
                    'NUMSVCD', 'NUMPSCH', 'NUMD8090Q'
                ],
                required_columns=[
                    'NPDES_ID', 'YEARQTR'
                ],
                data_types={
                    'NPDES_ID': 'string',
                    'YEARQTR': 'string',
                    'HLRNC': 'string',
                    'NUME90Q': 'integer',
                    'NUMCVDT': 'integer',
                    'NUMSVCD': 'integer',
                    'NUMPSCH': 'integer',
                    'NUMD8090Q': 'integer'
                },
                date_columns=[],
                numeric_columns=['NUME90Q', 'NUMCVDT', 'NUMSVCD', 'NUMPSCH', 'NUMD8090Q'],
                coordinate_columns=[],
                chunk_size=75000,  # Larger chunks for simple structure
                encoding='utf-8',
                delimiter=',',
                skip_rows=0,
                max_file_size_gb=1.0,
                validation_rules={
                    'yearqtr_pattern': r'^(19|20)\d{2}[1-4]$',  # YYYY1-4 format
                    'year_range': {'min': 1970, 'max': datetime.now().year},
                    'quarter_range': {'min': 1, 'max': 4},
                    'violation_counts': {'min': 0, 'max': 999},  # Reasonable violation count range
                    'hlrnc_codes': ['', ' ', 'S', 'C', 'X', 'T', 'D', 'E']  # Known HLRNC status codes
                }
            ),
            
            'air_emissions': DataSourceConfig(
                name='POLL_RPT_COMBINED_EMISSIONS',
                file_path=str(self.data_dir / 'POLL_RPT_COMBINED_EMISSIONS.csv'),
                sample_file_path=str(self.samples_dir / 'sample_POLL_RPT_COMBINED_EMISSIONS.csv'),
                staging_table='staging.air_emissions',
                description='Combined air emissions data from various EPA reporting programs',
                expected_columns=[
                    'REPORTING_YEAR', 'REGISTRY_ID', 'PGM_SYS_ACRNM', 'PGM_SYS_ID',
                    'POLLUTANT_NAME', 'ANNUAL_EMISSION', 'UNIT_OF_MEASURE',
                    'NEI_TYPE', 'NEI_HAP_VOC_FLAG'
                ],
                required_columns=[
                    'REPORTING_YEAR', 'REGISTRY_ID', 'POLLUTANT_NAME', 'ANNUAL_EMISSION'
                ],
                data_types={
                    'REPORTING_YEAR': 'integer',
                    'REGISTRY_ID': 'numeric',
                    'PGM_SYS_ACRNM': 'string',
                    'PGM_SYS_ID': 'string',
                    'POLLUTANT_NAME': 'string',
                    'ANNUAL_EMISSION': 'numeric',
                    'UNIT_OF_MEASURE': 'string',
                    'NEI_TYPE': 'string',
                    'NEI_HAP_VOC_FLAG': 'string'
                },
                date_columns=[],
                numeric_columns=['REPORTING_YEAR', 'REGISTRY_ID', 'ANNUAL_EMISSION'],
                coordinate_columns=[],
                chunk_size=60000,  # Optimized for emissions data structure
                encoding='utf-8',
                delimiter=',',
                skip_rows=0,
                max_file_size_gb=1.5,
                validation_rules={
                    'year_range': {'min': 1990, 'max': datetime.now().year},
                    'emission_range': {'min': 0, 'max': 1000000000},  # Reasonable emission range
                    'unit_measures': ['Pounds', 'Tons', 'Kilograms', 'Grams', 'Short Tons', 'Metric Tons'],
                    'program_systems': ['TRIS', 'NETS', 'GHGP', 'CAMD'],
                    'hap_voc_flags': ['', ' ', 'Y', 'N', 'HAP', 'VOC', 'BOTH']
                }
            )
        }
    
    def get_source(self, source_name: str) -> Optional[DataSourceConfig]:
        """Get configuration for a specific data source"""
        return self.sources.get(source_name)
    
    def get_all_sources(self) -> Dict[str, DataSourceConfig]:
        """Get all data source configurations"""
        return self.sources
    
    def get_source_names(self) -> List[str]:
        """Get list of all configured source names"""
        return list(self.sources.keys())
    
    def validate_source_files(self) -> Dict[str, Dict[str, Any]]:
        """Validate that all configured source files exist and are accessible"""
        validation_results = {}
        
        for name, config in self.sources.items():
            result = {
                'name': name,
                'file_exists': Path(config.file_path).exists(),
                'sample_exists': Path(config.sample_file_path).exists(),
                'file_size_mb': 0,
                'sample_size_mb': 0,
                'accessible': True,
                'errors': []
            }
            
            try:
                if result['file_exists']:
                    result['file_size_mb'] = Path(config.file_path).stat().st_size / (1024 * 1024)
                    if result['file_size_mb'] / 1024 > config.max_file_size_gb:
                        result['errors'].append(f"File size ({result['file_size_mb']:.1f}MB) exceeds maximum ({config.max_file_size_gb}GB)")
                
                if result['sample_exists']:
                    result['sample_size_mb'] = Path(config.sample_file_path).stat().st_size / (1024 * 1024)
                    
            except Exception as e:
                result['accessible'] = False
                result['errors'].append(f"File access error: {str(e)}")
            
            validation_results[name] = result
        
        return validation_results
    
    def get_processing_order(self) -> List[str]:
        """Get recommended processing order based on dependencies and file sizes"""
        # Process smaller, dependency files first, then larger files
        return [
            'icis_facilities',      # Smallest, provides facility details
            'npdes_measurements',   # Medium, needs facility context
            'icis_permits',         # Medium, links to facilities and measurements
            'air_emissions',        # Large, links to facilities
            'echo_facilities'       # Largest, master facility data
        ]
    
    def get_memory_requirements(self) -> Dict[str, float]:
        """Estimate memory requirements for each source in MB"""
        memory_estimates = {}
        
        for name, config in self.sources.items():
            # Estimate based on file size and chunk size
            if Path(config.file_path).exists():
                file_size_mb = Path(config.file_path).stat().st_size / (1024 * 1024)
                # Rough estimate: chunk memory = (chunk_size / total_rows) * file_size * 1.5 (overhead)
                estimated_chunk_memory = (config.chunk_size * len(config.expected_columns) * 100) / (1024 * 1024)  # Rough bytes per row
                memory_estimates[name] = min(estimated_chunk_memory, file_size_mb * 0.1)  # Cap at 10% of file size
            else:
                memory_estimates[name] = (config.chunk_size * len(config.expected_columns) * 50) / (1024 * 1024)
        
        return memory_estimates

# Global registry instance
def get_data_sources(project_root: str) -> DataSourceRegistry:
    """Get the global data sources registry"""
    return DataSourceRegistry(project_root)

# Configuration constants
DEFAULT_CHUNK_SIZE = 50000
DEFAULT_ENCODING = 'utf-8'
DEFAULT_DELIMITER = ','
MAX_MEMORY_USAGE_MB = 2048  # 2GB max memory usage
MAX_ERROR_THRESHOLD = 0.05  # 5% max error rate before stopping
CHECKPOINT_FREQUENCY = 10  # Save checkpoint every N chunks

# Performance tuning parameters
PERFORMANCE_CONFIG = {
    'parallel_workers': 4,
    'io_buffer_size': 65536,  # 64KB buffer
    'progress_update_frequency': 1000,  # Update progress every N rows
    'memory_check_frequency': 5,  # Check memory every N chunks
    'gc_frequency': 20  # Force garbage collection every N chunks
}
