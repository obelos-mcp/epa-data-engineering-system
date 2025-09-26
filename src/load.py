"""
Environmental Site Data Management & QA/QC Automation System
Load Module - Database Loading with Performance Optimization

This module handles loading transformed data into PostgreSQL with:
- Staging and core schema population
- Performance optimization and bulk operations
- Error handling and recovery
- Data quality integration and audit trail
- ETL pipeline integration
"""

import pandas as pd
import numpy as np
import logging
import time
import gc
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Iterator
from dataclasses import dataclass, field
from datetime import datetime
import warnings
warnings.filterwarnings('ignore', category=pd.errors.PerformanceWarning)

# Import project modules
import sys
sys.path.append(str(Path(__file__).parent.parent))

from src.database import DatabaseManager, DatabaseConfig, get_database_manager
from config.data_sources import get_data_sources, DataSourceConfig

@dataclass
class LoadStats:
    """Statistics for load operations"""
    source_name: str
    target_schema: str
    target_table: str
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    input_rows: int = 0
    loaded_rows: int = 0
    updated_rows: int = 0
    failed_rows: int = 0
    duplicate_rows: int = 0
    processing_rate_rows_sec: float = 0.0
    processing_rate_mb_sec: float = 0.0
    memory_usage_mb: float = 0.0
    batch_count: int = 0
    error_count: int = 0
    validation_errors: List[str] = field(default_factory=list)
    
    def calculate_rates(self):
        """Calculate processing rates"""
        if self.end_time and self.start_time:
            duration_seconds = (self.end_time - self.start_time).total_seconds()
            if duration_seconds > 0:
                self.processing_rate_rows_sec = self.loaded_rows / duration_seconds

@dataclass
class LoadConfiguration:
    """Configuration for load operations"""
    staging_batch_size: int = 10000
    core_batch_size: int = 5000
    max_errors_per_batch: int = 100
    enable_parallel_loading: bool = True
    disable_indexes_during_load: bool = True
    validate_constraints: bool = True
    create_load_summary: bool = True
    
    # Table-specific configurations
    table_configs: Dict[str, Dict[str, Any]] = field(default_factory=lambda: {
        'echo_facilities': {'batch_size': 5000, 'timeout': 300},
        'icis_facilities': {'batch_size': 10000, 'timeout': 120},
        'icis_permits': {'batch_size': 8000, 'timeout': 180},
        'npdes_measurements': {'batch_size': 15000, 'timeout': 90},
        'air_emissions': {'batch_size': 12000, 'timeout': 150}
    })

class DataLoader:
    """Main class for loading EPA environmental data into PostgreSQL"""
    
    def __init__(self, db_config: Optional[DatabaseConfig] = None, 
                 load_config: Optional[LoadConfiguration] = None,
                 log_level: str = 'INFO'):
        
        self.db_manager = get_database_manager(db_config)
        self.load_config = load_config or LoadConfiguration()
        self.data_sources = get_data_sources(str(Path(__file__).parent.parent))
        
        # Setup logging
        self._setup_logging(log_level)
        
        # Load statistics tracking
        self.load_stats: Dict[str, LoadStats] = {}
        self.current_batch_id: Optional[int] = None
        
        # Table loading order (respects foreign key dependencies)
        self.core_loading_order = [
            'facilities',           # Master hub
            'permits',             # Links to facilities
            'air_emissions',       # Links to facilities
            'water_measurements',  # Links to permits
            'compliance_history'   # Links to facilities
        ]
        
        # Column mappings for core schema
        self._initialize_column_mappings()
        
        self.logger.info("DataLoader initialized")
    
    def _setup_logging(self, log_level: str):
        """Setup comprehensive logging"""
        log_dir = Path(__file__).parent.parent / "logs"
        log_dir.mkdir(exist_ok=True)
        
        # Create logger
        self.logger = logging.getLogger('data_loader')
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # File handler
        log_file = log_dir / f"loading_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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
    
    def _initialize_column_mappings(self):
        """Initialize column mappings for staging to core transformations"""
        self.column_mappings = {
            'facilities': {
                'staging_table': 'echo_facilities',
                'primary_key': 'facility_id',
                'unique_columns': ['registry_id'],
                'required_columns': ['facility_name', 'state_code'],
                'mapping': {
                    'registry_id': 'registry_id',
                    'fac_name': 'facility_name',
                    'fac_street': 'street_address',
                    'fac_city': 'city',
                    'fac_state': 'state_code',
                    'fac_zip': 'zip_code',
                    'fac_county': 'county',
                    'fac_epa_region': 'epa_region',
                    'fac_lat': 'latitude',
                    'fac_long': 'longitude',
                    'fac_active_flag': 'is_active',
                    'fac_major_flag': 'is_major_facility',
                    'npdes_flag': 'has_water_permits',
                    'air_flag': 'has_air_permits',
                    'rcra_flag': 'has_waste_permits',
                    'tri_flag': 'has_tri_reporting'
                }
            },
            'permits': {
                'staging_table': 'icis_permits',
                'primary_key': 'permit_id',
                'unique_columns': ['npdes_id'],
                'foreign_keys': {'facility_id': 'facilities'},
                'required_columns': ['external_permit_number', 'permit_type_code'],
                'mapping': {
                    'external_permit_nmbr': 'external_permit_number',
                    'permit_type_code': 'permit_type_code',
                    'permit_status_code': 'permit_status_code',
                    'major_minor_status_flag': 'major_minor_status',
                    'total_design_flow_nmbr': 'total_design_flow_mgd',
                    'actual_average_flow_nmbr': 'actual_average_flow_mgd',
                    'effective_date': 'effective_date',
                    'expiration_date': 'expiration_date'
                }
            },
            'water_measurements': {
                'staging_table': 'npdes_measurements',
                'primary_key': 'measurement_id',
                'foreign_keys': {'permit_id': 'permits'},
                'unique_columns': ['npdes_id', 'measurement_year', 'measurement_quarter'],
                'mapping': {
                    'npdes_id': 'npdes_id',
                    'yearqtr': ['measurement_year', 'measurement_quarter'],  # Special handling
                    'nume90_q': 'effluent_violations_90day',
                    'numcvdt': 'compliance_violations',
                    'numsvcd': 'single_event_violations',
                    'numpsch': 'schedule_violations',
                    'hlrnc': 'hlrnc_status'
                }
            },
            'air_emissions': {
                'staging_table': 'air_emissions',
                'primary_key': 'emission_id',
                'foreign_keys': {'facility_id': 'facilities'},
                'unique_columns': ['registry_id', 'reporting_year', 'pollutant_name'],
                'mapping': {
                    'registry_id': 'registry_id',
                    'reporting_year': 'reporting_year',
                    'pollutant_name': 'pollutant_name',
                    'annual_emission': 'annual_emission_amount',
                    'unit_of_measure': 'emission_unit_of_measure',
                    'pgm_sys_acrnm': 'program_system_acronym',
                    'pgm_sys_id': 'program_system_id'
                }
            }
        }
    
    def start_etl_batch(self, process_name: str, source_file: str = None) -> int:
        """Start new ETL batch and return batch ID"""
        try:
            # Generate new batch ID
            self.current_batch_id = int(time.time() * 1000)  # Millisecond timestamp
            
            # Insert ETL run log entry
            with self.db_manager.get_connection() as conn:
                insert_query = """
                INSERT INTO core.etl_run_log 
                (batch_id, process_name, source_file_name, start_time, status, created_by)
                VALUES (:batch_id, :process_name, :source_file, :start_time, 'RUNNING', 'data_loader')
                """
                
                conn.execute(
                    insert_query,
                    {
                        'batch_id': self.current_batch_id,
                        'process_name': process_name,
                        'source_file': source_file,
                        'start_time': datetime.now()
                    }
                )
            
            self.logger.info(f"Started ETL batch {self.current_batch_id} for {process_name}")
            return self.current_batch_id
            
        except Exception as e:
            self.logger.error(f"Failed to start ETL batch: {e}")
            raise
    
    def update_etl_batch(self, status: str, records_processed: int = 0, 
                        records_inserted: int = 0, records_updated: int = 0,
                        records_failed: int = 0, error_message: str = None):
        """Update ETL batch status"""
        if not self.current_batch_id:
            return
        
        try:
            with self.db_manager.get_connection() as conn:
                update_query = """
                UPDATE core.etl_run_log SET
                    end_time = :end_time,
                    status = :status,
                    records_processed = :records_processed,
                    records_inserted = :records_inserted,
                    records_updated = :records_updated,
                    records_failed = :records_failed,
                    error_message = :error_message
                WHERE batch_id = :batch_id
                """
                
                conn.execute(
                    update_query,
                    {
                        'batch_id': self.current_batch_id,
                        'end_time': datetime.now(),
                        'status': status,
                        'records_processed': records_processed,
                        'records_inserted': records_inserted,
                        'records_updated': records_updated,
                        'records_failed': records_failed,
                        'error_message': error_message
                    }
                )
                
        except Exception as e:
            self.logger.error(f"Failed to update ETL batch: {e}")
    
    def load_to_staging(self, df: pd.DataFrame, source_config: DataSourceConfig) -> LoadStats:
        """Load transformed data to staging tables"""
        stats = LoadStats(
            source_name=source_config.name,
            target_schema='staging',
            target_table=source_config.staging_table.split('.')[-1]  # Remove schema prefix
        )
        
        stats.input_rows = len(df)
        
        if df.empty:
            self.logger.warning(f"Empty DataFrame for {source_config.name}")
            stats.end_time = datetime.now()
            return stats
        
        try:
            self.logger.info(f"Loading {stats.input_rows:,} rows to {stats.target_schema}.{stats.target_table}")
            
            # Get table-specific configuration
            table_config = self.load_config.table_configs.get(
                source_config.name.lower(), 
                {'batch_size': self.load_config.staging_batch_size}
            )
            
            batch_size = table_config['batch_size']
            
            # Disable indexes if configured
            if self.load_config.disable_indexes_during_load:
                self.db_manager.disable_indexes(stats.target_table, stats.target_schema)
            
            # Load data in batches
            total_loaded = 0
            batch_count = 0
            
            for start_idx in range(0, len(df), batch_size):
                end_idx = min(start_idx + batch_size, len(df))
                batch_df = df.iloc[start_idx:end_idx].copy()
                
                try:
                    # Add ETL metadata
                    batch_df['etl_batch_id'] = self.current_batch_id
                    batch_df['etl_load_date'] = datetime.now()
                    batch_df['etl_source_file'] = source_config.name
                    batch_df['etl_row_number'] = range(start_idx + 1, end_idx + 1)
                    
                    # Bulk insert batch
                    rows_inserted = self.db_manager.bulk_insert_dataframe(
                        batch_df,
                        stats.target_table,
                        schema=stats.target_schema,
                        if_exists='append',
                        chunksize=min(batch_size, 5000)
                    )
                    
                    total_loaded += len(batch_df)
                    batch_count += 1
                    stats.batch_count = batch_count
                    
                    if batch_count % 10 == 0:
                        self.logger.info(f"Loaded batch {batch_count}: {total_loaded:,}/{stats.input_rows:,} rows")
                    
                except Exception as batch_error:
                    self.logger.error(f"Batch {batch_count} failed: {batch_error}")
                    stats.error_count += 1
                    stats.failed_rows += len(batch_df)
                    
                    if stats.error_count > self.load_config.max_errors_per_batch:
                        raise Exception(f"Too many batch errors: {stats.error_count}")
            
            stats.loaded_rows = total_loaded
            stats.end_time = datetime.now()
            stats.calculate_rates()
            
            # Re-enable indexes
            if self.load_config.disable_indexes_during_load:
                self.db_manager.enable_indexes(stats.target_table, stats.target_schema)
            
            self.logger.info(
                f"Staging load complete: {stats.loaded_rows:,} rows loaded "
                f"in {stats.batch_count} batches ({stats.processing_rate_rows_sec:.0f} rows/sec)"
            )
            
            return stats
            
        except Exception as e:
            stats.end_time = datetime.now()
            stats.error_count += 1
            self.logger.error(f"Staging load failed for {source_config.name}: {e}")
            raise
    
    def transform_for_core_schema(self, df: pd.DataFrame, target_table: str) -> pd.DataFrame:
        """Transform staging data for core schema"""
        if target_table not in self.column_mappings:
            raise ValueError(f"Unknown target table: {target_table}")
        
        mapping_config = self.column_mappings[target_table]
        column_mapping = mapping_config['mapping']
        
        # Create new DataFrame with mapped columns
        core_df = pd.DataFrame()
        
        for staging_col, core_col in column_mapping.items():
            if isinstance(core_col, list):
                # Special handling for complex mappings (e.g., yearqtr -> year, quarter)
                if staging_col == 'yearqtr' and core_col == ['measurement_year', 'measurement_quarter']:
                    if staging_col in df.columns:
                        yearqtr_str = df[staging_col].astype(str)
                        core_df['measurement_year'] = pd.to_numeric(yearqtr_str.str[:4], errors='coerce')
                        core_df['measurement_quarter'] = pd.to_numeric(yearqtr_str.str[4:], errors='coerce')
            else:
                if staging_col in df.columns:
                    core_df[core_col] = df[staging_col]
        
        # Add audit fields
        core_df['created_date'] = datetime.now()
        core_df['updated_date'] = datetime.now()
        core_df['created_by'] = 'etl_system'
        core_df['updated_by'] = 'etl_system'
        
        # Handle boolean conversions
        boolean_columns = [col for col in core_df.columns if col.startswith('is_') or col.startswith('has_')]
        for col in boolean_columns:
            if col in core_df.columns:
                core_df[col] = core_df[col].map({'Y': True, 'N': False, 'y': True, 'n': False}).fillna(False)
        
        return core_df
    
    def load_to_core_schema(self, source_name: str) -> Dict[str, LoadStats]:
        """Load data from staging to core schema"""
        self.logger.info(f"Starting core schema loading for {source_name}")
        
        all_stats = {}
        
        try:
            # Load in dependency order
            for table_name in self.core_loading_order:
                if table_name not in self.column_mappings:
                    continue
                
                mapping_config = self.column_mappings[table_name]
                staging_table = mapping_config['staging_table']
                
                # Skip if not relevant to current source
                if source_name.lower() not in staging_table.lower():
                    continue
                
                stats = LoadStats(
                    source_name=source_name,
                    target_schema='core',
                    target_table=table_name
                )
                
                try:
                    # Read from staging
                    staging_query = f"SELECT * FROM staging.{staging_table} WHERE etl_batch_id = :batch_id"
                    staging_df = self.db_manager.execute_query(
                        staging_query, 
                        {'batch_id': self.current_batch_id}
                    )
                    
                    if staging_df.empty:
                        self.logger.info(f"No data to load for {table_name}")
                        continue
                    
                    stats.input_rows = len(staging_df)
                    
                    # Transform for core schema
                    core_df = self.transform_for_core_schema(staging_df, table_name)
                    
                    # Handle foreign key relationships
                    if 'foreign_keys' in mapping_config:
                        core_df = self._resolve_foreign_keys(core_df, mapping_config['foreign_keys'])
                    
                    # Upsert to core table
                    conflict_columns = mapping_config.get('unique_columns', [mapping_config['primary_key']])
                    rows_affected = self.db_manager.upsert_dataframe(
                        core_df,
                        table_name,
                        schema='core',
                        conflict_columns=conflict_columns
                    )
                    
                    stats.loaded_rows = rows_affected
                    stats.end_time = datetime.now()
                    stats.calculate_rates()
                    
                    all_stats[table_name] = stats
                    
                    self.logger.info(f"Core loading complete for {table_name}: {rows_affected:,} rows affected")
                    
                except Exception as e:
                    stats.error_count += 1
                    self.logger.error(f"Core loading failed for {table_name}: {e}")
                    all_stats[table_name] = stats
                    raise
            
            return all_stats
            
        except Exception as e:
            self.logger.error(f"Core schema loading failed for {source_name}: {e}")
            raise
    
    def _resolve_foreign_keys(self, df: pd.DataFrame, foreign_keys: Dict[str, str]) -> pd.DataFrame:
        """Resolve foreign key relationships"""
        resolved_df = df.copy()
        
        for fk_column, ref_table in foreign_keys.items():
            if ref_table == 'facilities':
                # Resolve facility_id from registry_id
                if 'registry_id' in resolved_df.columns:
                    facility_lookup_query = """
                    SELECT facility_id, registry_id 
                    FROM core.facilities 
                    WHERE registry_id IS NOT NULL
                    """
                    
                    facility_lookup = self.db_manager.execute_query(facility_lookup_query)
                    
                    if not facility_lookup.empty:
                        lookup_dict = dict(zip(facility_lookup['registry_id'], facility_lookup['facility_id']))
                        resolved_df[fk_column] = resolved_df['registry_id'].map(lookup_dict)
            
            elif ref_table == 'permits':
                # Resolve permit_id from npdes_id
                if 'npdes_id' in resolved_df.columns:
                    permit_lookup_query = """
                    SELECT permit_id, npdes_id 
                    FROM core.permits 
                    WHERE npdes_id IS NOT NULL
                    """
                    
                    permit_lookup = self.db_manager.execute_query(permit_lookup_query)
                    
                    if not permit_lookup.empty:
                        lookup_dict = dict(zip(permit_lookup['npdes_id'], permit_lookup['permit_id']))
                        resolved_df[fk_column] = resolved_df['npdes_id'].map(lookup_dict)
        
        return resolved_df
    
    def load_source_complete(self, source_name: str, transformation_generator) -> Dict[str, Any]:
        """Complete loading process for a source"""
        source_config = self.data_sources.get_source(source_name)
        if not source_config:
            raise ValueError(f"Unknown source: {source_name}")
        
        # Start ETL batch
        batch_id = self.start_etl_batch(f"LOAD_{source_name.upper()}", source_config.name)
        
        staging_stats_list = []
        total_input_rows = 0
        total_loaded_rows = 0
        
        try:
            self.logger.info(f"Starting complete load process for {source_name}")
            
            # Process all chunks from transformation
            for transformed_chunk, transform_stats in transformation_generator:
                # Load to staging
                staging_stats = self.load_to_staging(transformed_chunk, source_config)
                staging_stats_list.append(staging_stats)
                
                total_input_rows += staging_stats.input_rows
                total_loaded_rows += staging_stats.loaded_rows
                
                # Memory management
                del transformed_chunk
                gc.collect()
            
            # Load to core schema
            core_stats = self.load_to_core_schema(source_name)
            
            # Update ETL batch as successful
            self.update_etl_batch(
                status='SUCCESS',
                records_processed=total_input_rows,
                records_inserted=total_loaded_rows,
                records_updated=sum(stats.updated_rows for stats in core_stats.values())
            )
            
            # Generate summary
            summary = {
                'source_name': source_name,
                'batch_id': batch_id,
                'staging_stats': {
                    'total_input_rows': total_input_rows,
                    'total_loaded_rows': total_loaded_rows,
                    'batch_count': sum(stats.batch_count for stats in staging_stats_list),
                    'error_count': sum(stats.error_count for stats in staging_stats_list)
                },
                'core_stats': {
                    table: {
                        'input_rows': stats.input_rows,
                        'loaded_rows': stats.loaded_rows,
                        'processing_rate': stats.processing_rate_rows_sec
                    }
                    for table, stats in core_stats.items()
                },
                'overall_success': True,
                'load_timestamp': datetime.now()
            }
            
            self.logger.info(f"Complete load process finished for {source_name}: {total_loaded_rows:,} rows loaded")
            return summary
            
        except Exception as e:
            # Update ETL batch as failed
            self.update_etl_batch(
                status='FAILED',
                records_processed=total_input_rows,
                records_inserted=total_loaded_rows,
                error_message=str(e)
            )
            
            self.logger.error(f"Complete load process failed for {source_name}: {e}")
            raise
    
    def validate_load_integrity(self, source_name: str) -> Dict[str, Any]:
        """Validate data integrity after loading"""
        validation_results = {}
        
        try:
            # Get table mapping for source
            relevant_tables = []
            for table_name, config in self.column_mappings.items():
                if source_name.lower() in config['staging_table'].lower():
                    relevant_tables.append(table_name)
            
            for table_name in relevant_tables:
                table_validation = self.db_manager.validate_data_integrity(table_name, 'core')
                validation_results[table_name] = table_validation
            
            # Cross-reference validation
            if 'facilities' in relevant_tables:
                cross_ref_results = self._validate_cross_references()
                validation_results['cross_references'] = cross_ref_results
            
            return validation_results
            
        except Exception as e:
            self.logger.error(f"Load integrity validation failed: {e}")
            raise
    
    def _validate_cross_references(self) -> Dict[str, Any]:
        """Validate cross-dataset references"""
        try:
            with self.db_manager.get_connection() as conn:
                validation_results = {}
                
                # Check REGISTRY_ID consistency between facilities and air_emissions
                registry_check_query = """
                SELECT 
                    COUNT(ae.registry_id) as air_emissions_count,
                    COUNT(f.registry_id) as facilities_count,
                    COUNT(CASE WHEN f.registry_id IS NULL THEN 1 END) as missing_facilities
                FROM core.air_emissions ae
                LEFT JOIN core.facilities f ON ae.registry_id = f.registry_id
                WHERE ae.registry_id IS NOT NULL
                """
                
                registry_results = pd.read_sql(registry_check_query, conn)
                validation_results['registry_id_consistency'] = registry_results.to_dict('records')[0]
                
                # Check NPDES_ID consistency between permits and water_measurements
                npdes_check_query = """
                SELECT 
                    COUNT(wm.npdes_id) as water_measurements_count,
                    COUNT(p.npdes_id) as permits_count,
                    COUNT(CASE WHEN p.npdes_id IS NULL THEN 1 END) as missing_permits
                FROM core.water_measurements wm
                LEFT JOIN core.permits p ON wm.npdes_id = p.npdes_id
                WHERE wm.npdes_id IS NOT NULL
                """
                
                npdes_results = pd.read_sql(npdes_check_query, conn)
                validation_results['npdes_id_consistency'] = npdes_results.to_dict('records')[0]
                
                return validation_results
                
        except Exception as e:
            self.logger.error(f"Cross-reference validation failed: {e}")
            raise
    
    def get_load_summary(self) -> Dict[str, Any]:
        """Get comprehensive load summary"""
        try:
            # Get recent load performance
            performance_metrics = self.db_manager.get_load_performance_metrics()
            
            # Get table row counts
            table_counts = {}
            for schema in ['staging', 'core']:
                schema_tables = []
                
                if schema == 'staging':
                    schema_tables = ['echo_facilities', 'icis_facilities', 'icis_permits', 
                                   'npdes_measurements', 'air_emissions']
                else:
                    schema_tables = ['facilities', 'permits', 'water_measurements', 
                                   'air_emissions', 'compliance_history']
                
                for table in schema_tables:
                    try:
                        table_info = self.db_manager.get_table_info(table, schema)
                        table_counts[f"{schema}.{table}"] = table_info['row_count']
                    except:
                        table_counts[f"{schema}.{table}"] = 0
            
            return {
                'load_statistics': self.load_stats,
                'table_row_counts': table_counts,
                'performance_metrics': performance_metrics,
                'database_stats': self.db_manager.get_connection_stats(),
                'summary_timestamp': datetime.now()
            }
            
        except Exception as e:
            self.logger.error(f"Failed to generate load summary: {e}")
            raise
