#!/usr/bin/env python3
"""
Environmental Site Data Management & QA/QC Automation System
Load Validation Scripts

This module provides comprehensive validation of loaded data including:
- Data integrity verification
- Relationship consistency checks
- Performance validation
- Data quality assessments
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import sys

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.database import get_database_manager, DatabaseConfig

class LoadValidator:
    """Comprehensive load validation and integrity checking"""
    
    def __init__(self, db_config: Optional[DatabaseConfig] = None):
        self.db_manager = get_database_manager(db_config)
        self.validation_results = {}
    
    def validate_table_integrity(self, table_name: str, schema: str = 'core') -> Dict[str, Any]:
        """Validate data integrity for a specific table"""
        print(f"\n🔍 Validating {schema}.{table_name}")
        
        validation_results = {
            'table': f"{schema}.{table_name}",
            'timestamp': datetime.now(),
            'checks': {},
            'overall_status': 'PASS'
        }
        
        try:
            # Get table information
            table_info = self.db_manager.get_table_info(table_name, schema)
            validation_results['row_count'] = table_info['row_count']
            validation_results['column_count'] = table_info['column_count']
            
            print(f"  📊 Table stats: {table_info['row_count']:,} rows, {table_info['column_count']} columns")
            
            # Check 1: No completely null rows
            null_rows_query = f"""
            SELECT COUNT(*) as null_rows
            FROM {schema}.{table_name}
            WHERE {' AND '.join([f"{col} IS NULL" for col in table_info['columns'][:5]])}
            """
            
            null_rows_result = self.db_manager.execute_query(null_rows_query)
            null_rows = null_rows_result.iloc[0]['null_rows'] if not null_rows_result.empty else 0
            
            validation_results['checks']['null_rows'] = {
                'count': null_rows,
                'status': 'PASS' if null_rows == 0 else 'WARN'
            }
            
            print(f"  ✅ Null rows check: {null_rows} completely null rows")
            
            # Check 2: Primary key uniqueness (if applicable)
            if table_name in ['facilities', 'permits', 'water_measurements', 'air_emissions']:
                pk_column = self._get_primary_key_column(table_name)
                if pk_column:
                    pk_duplicate_query = f"""
                    SELECT COUNT(*) - COUNT(DISTINCT {pk_column}) as duplicates
                    FROM {schema}.{table_name}
                    WHERE {pk_column} IS NOT NULL
                    """
                    
                    pk_result = self.db_manager.execute_query(pk_duplicate_query)
                    duplicates = pk_result.iloc[0]['duplicates'] if not pk_result.empty else 0
                    
                    validation_results['checks']['primary_key_uniqueness'] = {
                        'column': pk_column,
                        'duplicates': duplicates,
                        'status': 'PASS' if duplicates == 0 else 'FAIL'
                    }
                    
                    if duplicates > 0:
                        validation_results['overall_status'] = 'FAIL'
                    
                    print(f"  ✅ Primary key uniqueness: {duplicates} duplicates in {pk_column}")
            
            # Check 3: Required field completeness
            required_fields = self._get_required_fields(table_name)
            for field in required_fields:
                if field in table_info['columns']:
                    missing_query = f"""
                    SELECT COUNT(*) as missing_count
                    FROM {schema}.{table_name}
                    WHERE {field} IS NULL
                    """
                    
                    missing_result = self.db_manager.execute_query(missing_query)
                    missing_count = missing_result.iloc[0]['missing_count'] if not missing_result.empty else 0
                    
                    validation_results['checks'][f'required_field_{field}'] = {
                        'missing_count': missing_count,
                        'status': 'PASS' if missing_count == 0 else 'WARN'
                    }
                    
                    print(f"  ✅ Required field {field}: {missing_count} missing values")
            
            # Check 4: Data type validation
            if table_name == 'facilities':
                # Validate coordinates
                coord_query = f"""
                SELECT 
                    COUNT(CASE WHEN latitude < -90 OR latitude > 90 THEN 1 END) as invalid_lat,
                    COUNT(CASE WHEN longitude < -180 OR longitude > 180 THEN 1 END) as invalid_lon
                FROM {schema}.{table_name}
                WHERE latitude IS NOT NULL OR longitude IS NOT NULL
                """
                
                coord_result = self.db_manager.execute_query(coord_query)
                if not coord_result.empty:
                    invalid_lat = coord_result.iloc[0]['invalid_lat']
                    invalid_lon = coord_result.iloc[0]['invalid_lon']
                    
                    validation_results['checks']['coordinate_validation'] = {
                        'invalid_latitude': invalid_lat,
                        'invalid_longitude': invalid_lon,
                        'status': 'PASS' if invalid_lat == 0 and invalid_lon == 0 else 'FAIL'
                    }
                    
                    print(f"  ✅ Coordinate validation: {invalid_lat} invalid lat, {invalid_lon} invalid lon")
            
            return validation_results
            
        except Exception as e:
            validation_results['checks']['error'] = str(e)
            validation_results['overall_status'] = 'ERROR'
            print(f"  ❌ Validation error: {e}")
            return validation_results
    
    def validate_relationships(self) -> Dict[str, Any]:
        """Validate foreign key relationships and cross-table consistency"""
        print(f"\n🔗 Validating table relationships")
        
        relationship_results = {
            'timestamp': datetime.now(),
            'checks': {},
            'overall_status': 'PASS'
        }
        
        try:
            # Check 1: Facilities → Air Emissions relationship
            facility_emissions_query = """
            SELECT 
                COUNT(ae.registry_id) as total_air_emissions,
                COUNT(f.registry_id) as matched_facilities,
                COUNT(ae.registry_id) - COUNT(f.registry_id) as unmatched_emissions
            FROM core.air_emissions ae
            LEFT JOIN core.facilities f ON ae.registry_id = f.registry_id
            """
            
            fe_result = self.db_manager.execute_query(facility_emissions_query)
            if not fe_result.empty:
                fe_data = fe_result.iloc[0]
                unmatched = fe_data['unmatched_emissions']
                
                relationship_results['checks']['facilities_air_emissions'] = {
                    'total_air_emissions': fe_data['total_air_emissions'],
                    'matched_facilities': fe_data['matched_facilities'],
                    'unmatched_emissions': unmatched,
                    'match_rate': (fe_data['matched_facilities'] / fe_data['total_air_emissions'] * 100) if fe_data['total_air_emissions'] > 0 else 0,
                    'status': 'PASS' if unmatched < fe_data['total_air_emissions'] * 0.1 else 'WARN'  # Allow 10% unmatched
                }
                
                print(f"  ✅ Facilities ↔ Air Emissions: {fe_data['matched_facilities']:,}/{fe_data['total_air_emissions']:,} matched ({unmatched:,} unmatched)")
            
            # Check 2: Permits → Water Measurements relationship
            permit_measurements_query = """
            SELECT 
                COUNT(wm.npdes_id) as total_water_measurements,
                COUNT(p.npdes_id) as matched_permits,
                COUNT(wm.npdes_id) - COUNT(p.npdes_id) as unmatched_measurements
            FROM core.water_measurements wm
            LEFT JOIN core.permits p ON wm.npdes_id = p.npdes_id
            """
            
            pm_result = self.db_manager.execute_query(permit_measurements_query)
            if not pm_result.empty:
                pm_data = pm_result.iloc[0]
                unmatched = pm_data['unmatched_measurements']
                
                relationship_results['checks']['permits_water_measurements'] = {
                    'total_water_measurements': pm_data['total_water_measurements'],
                    'matched_permits': pm_data['matched_permits'],
                    'unmatched_measurements': unmatched,
                    'match_rate': (pm_data['matched_permits'] / pm_data['total_water_measurements'] * 100) if pm_data['total_water_measurements'] > 0 else 0,
                    'status': 'PASS' if unmatched < pm_data['total_water_measurements'] * 0.1 else 'WARN'
                }
                
                print(f"  ✅ Permits ↔ Water Measurements: {pm_data['matched_permits']:,}/{pm_data['total_water_measurements']:,} matched ({unmatched:,} unmatched)")
            
            # Check 3: Facilities → Permits relationship
            facility_permits_query = """
            SELECT 
                COUNT(DISTINCT f.facility_id) as total_facilities,
                COUNT(DISTINCT p.facility_id) as facilities_with_permits
            FROM core.facilities f
            LEFT JOIN core.permits p ON f.facility_id = p.facility_id
            """
            
            fp_result = self.db_manager.execute_query(facility_permits_query)
            if not fp_result.empty:
                fp_data = fp_result.iloc[0]
                
                relationship_results['checks']['facilities_permits'] = {
                    'total_facilities': fp_data['total_facilities'],
                    'facilities_with_permits': fp_data['facilities_with_permits'],
                    'permit_coverage': (fp_data['facilities_with_permits'] / fp_data['total_facilities'] * 100) if fp_data['total_facilities'] > 0 else 0,
                    'status': 'PASS'
                }
                
                print(f"  ✅ Facilities → Permits: {fp_data['facilities_with_permits']:,}/{fp_data['total_facilities']:,} facilities have permits")
            
            return relationship_results
            
        except Exception as e:
            relationship_results['checks']['error'] = str(e)
            relationship_results['overall_status'] = 'ERROR'
            print(f"  ❌ Relationship validation error: {e}")
            return relationship_results
    
    def validate_data_quality(self) -> Dict[str, Any]:
        """Validate overall data quality metrics"""
        print(f"\n📊 Validating data quality metrics")
        
        quality_results = {
            'timestamp': datetime.now(),
            'metrics': {},
            'overall_status': 'PASS'
        }
        
        try:
            # Check 1: Data completeness by table
            tables = ['facilities', 'permits', 'water_measurements', 'air_emissions']
            
            for table in tables:
                try:
                    table_info = self.db_manager.get_table_info(table, 'core')
                    
                    # Calculate completeness
                    completeness_query = f"""
                    SELECT 
                        COUNT(*) as total_rows,
                        {', '.join([f"COUNT({col}) as {col}_count" for col in table_info['columns'][:10]])}
                    FROM core.{table}
                    """
                    
                    completeness_result = self.db_manager.execute_query(completeness_query)
                    if not completeness_result.empty:
                        data = completeness_result.iloc[0]
                        total_rows = data['total_rows']
                        
                        # Calculate average completeness
                        completeness_scores = []
                        for col in table_info['columns'][:10]:  # Check first 10 columns
                            col_count_key = f"{col}_count"
                            if col_count_key in data:
                                completeness = (data[col_count_key] / total_rows * 100) if total_rows > 0 else 0
                                completeness_scores.append(completeness)
                        
                        avg_completeness = np.mean(completeness_scores) if completeness_scores else 0
                        
                        quality_results['metrics'][f'{table}_completeness'] = {
                            'total_rows': total_rows,
                            'average_completeness': avg_completeness,
                            'status': 'PASS' if avg_completeness >= 80 else 'WARN'
                        }
                        
                        print(f"  ✅ {table} completeness: {avg_completeness:.1f}% ({total_rows:,} rows)")
                
                except Exception as table_error:
                    print(f"  ❌ {table} quality check failed: {table_error}")
            
            # Check 2: Recent data freshness
            freshness_query = """
            SELECT 
                process_name,
                MAX(end_time) as last_run,
                COUNT(*) as run_count
            FROM core.etl_run_log 
            WHERE end_time >= NOW() - INTERVAL '7 days'
            AND status = 'SUCCESS'
            GROUP BY process_name
            ORDER BY last_run DESC
            """
            
            freshness_result = self.db_manager.execute_query(freshness_query)
            if not freshness_result.empty:
                quality_results['metrics']['data_freshness'] = {
                    'recent_loads': freshness_result.to_dict('records'),
                    'status': 'PASS'
                }
                
                print(f"  ✅ Data freshness: {len(freshness_result)} recent successful loads")
            
            return quality_results
            
        except Exception as e:
            quality_results['metrics']['error'] = str(e)
            quality_results['overall_status'] = 'ERROR'
            print(f"  ❌ Data quality validation error: {e}")
            return quality_results
    
    def validate_performance_metrics(self) -> Dict[str, Any]:
        """Validate load performance and efficiency"""
        print(f"\n⚡ Validating performance metrics")
        
        performance_results = {
            'timestamp': datetime.now(),
            'metrics': {},
            'overall_status': 'PASS'
        }
        
        try:
            # Get recent performance data
            performance_query = """
            SELECT 
                process_name,
                AVG(records_processed) as avg_records,
                AVG(EXTRACT(EPOCH FROM (end_time - start_time))) as avg_duration_seconds,
                AVG(records_processed / NULLIF(EXTRACT(EPOCH FROM (end_time - start_time)), 0)) as avg_records_per_second,
                COUNT(*) as run_count
            FROM core.etl_run_log 
            WHERE end_time >= NOW() - INTERVAL '24 hours'
            AND status = 'SUCCESS'
            AND records_processed > 0
            GROUP BY process_name
            ORDER BY avg_records_per_second DESC
            """
            
            perf_result = self.db_manager.execute_query(performance_query)
            
            if not perf_result.empty:
                for _, row in perf_result.iterrows():
                    process_name = row['process_name']
                    avg_records = row['avg_records']
                    avg_duration = row['avg_duration_seconds']
                    avg_rps = row['avg_records_per_second']
                    
                    performance_results['metrics'][process_name] = {
                        'avg_records_processed': avg_records,
                        'avg_duration_seconds': avg_duration,
                        'avg_records_per_second': avg_rps,
                        'run_count': row['run_count'],
                        'status': 'PASS' if avg_rps > 1000 else 'WARN'  # Expect >1000 records/sec
                    }
                    
                    print(f"  ✅ {process_name}: {avg_rps:.0f} records/sec ({avg_records:.0f} avg records)")
            
            # Check database performance
            db_stats = self.db_manager.get_connection_stats()
            performance_results['metrics']['database_performance'] = {
                'connection_stats': db_stats,
                'health_status': db_stats.get('health_status', 'unknown'),
                'status': 'PASS' if db_stats.get('health_status') == 'healthy' else 'WARN'
            }
            
            print(f"  ✅ Database health: {db_stats.get('health_status', 'unknown')}")
            
            return performance_results
            
        except Exception as e:
            performance_results['metrics']['error'] = str(e)
            performance_results['overall_status'] = 'ERROR'
            print(f"  ❌ Performance validation error: {e}")
            return performance_results
    
    def _get_primary_key_column(self, table_name: str) -> Optional[str]:
        """Get primary key column for table"""
        pk_mapping = {
            'facilities': 'facility_id',
            'permits': 'permit_id',
            'water_measurements': 'measurement_id',
            'air_emissions': 'emission_id',
            'compliance_history': 'compliance_id'
        }
        return pk_mapping.get(table_name)
    
    def _get_required_fields(self, table_name: str) -> List[str]:
        """Get required fields for table"""
        required_fields = {
            'facilities': ['facility_name', 'state_code'],
            'permits': ['external_permit_number', 'permit_type_code'],
            'water_measurements': ['npdes_id', 'measurement_year'],
            'air_emissions': ['registry_id', 'reporting_year', 'pollutant_name'],
            'compliance_history': ['facility_id', 'program_type']
        }
        return required_fields.get(table_name, [])
    
    def run_complete_validation(self) -> Dict[str, Any]:
        """Run complete validation suite"""
        print("=" * 80)
        print(" EPA ENVIRONMENTAL DATA LOAD VALIDATION")
        print("=" * 80)
        
        complete_results = {
            'validation_timestamp': datetime.now(),
            'table_validations': {},
            'relationship_validation': {},
            'quality_validation': {},
            'performance_validation': {},
            'overall_status': 'PASS'
        }
        
        try:
            # Validate individual tables
            core_tables = ['facilities', 'permits', 'water_measurements', 'air_emissions']
            
            for table in core_tables:
                try:
                    table_result = self.validate_table_integrity(table, 'core')
                    complete_results['table_validations'][table] = table_result
                    
                    if table_result['overall_status'] == 'FAIL':
                        complete_results['overall_status'] = 'FAIL'
                        
                except Exception as e:
                    print(f"❌ Table validation failed for {table}: {e}")
                    complete_results['overall_status'] = 'FAIL'
            
            # Validate relationships
            try:
                relationship_result = self.validate_relationships()
                complete_results['relationship_validation'] = relationship_result
                
                if relationship_result['overall_status'] == 'FAIL':
                    complete_results['overall_status'] = 'FAIL'
                    
            except Exception as e:
                print(f"❌ Relationship validation failed: {e}")
                complete_results['overall_status'] = 'FAIL'
            
            # Validate data quality
            try:
                quality_result = self.validate_data_quality()
                complete_results['quality_validation'] = quality_result
                
            except Exception as e:
                print(f"❌ Quality validation failed: {e}")
            
            # Validate performance
            try:
                performance_result = self.validate_performance_metrics()
                complete_results['performance_validation'] = performance_result
                
            except Exception as e:
                print(f"❌ Performance validation failed: {e}")
            
            # Print summary
            print(f"\n" + "=" * 80)
            print(" VALIDATION SUMMARY")
            print("=" * 80)
            
            status_icon = "✅" if complete_results['overall_status'] == 'PASS' else "❌"
            print(f"{status_icon} Overall Status: {complete_results['overall_status']}")
            
            print(f"\n📊 Table Validation Results:")
            for table, result in complete_results['table_validations'].items():
                status_icon = "✅" if result['overall_status'] == 'PASS' else "❌"
                print(f"  {status_icon} {table}: {result['overall_status']} ({result.get('row_count', 0):,} rows)")
            
            if complete_results['relationship_validation']:
                rel_status = complete_results['relationship_validation']['overall_status']
                status_icon = "✅" if rel_status == 'PASS' else "❌"
                print(f"\n🔗 Relationship Validation: {status_icon} {rel_status}")
            
            if complete_results['quality_validation']:
                qual_status = complete_results['quality_validation']['overall_status']
                status_icon = "✅" if qual_status == 'PASS' else "❌"
                print(f"📊 Quality Validation: {status_icon} {qual_status}")
            
            if complete_results['performance_validation']:
                perf_status = complete_results['performance_validation']['overall_status']
                status_icon = "✅" if perf_status == 'PASS' else "❌"
                print(f"⚡ Performance Validation: {status_icon} {perf_status}")
            
            return complete_results
            
        except Exception as e:
            print(f"❌ Complete validation failed: {e}")
            complete_results['overall_status'] = 'ERROR'
            complete_results['error'] = str(e)
            return complete_results

def main():
    """Run load validation"""
    try:
        validator = LoadValidator()
        results = validator.run_complete_validation()
        
        # Return appropriate exit code
        if results['overall_status'] == 'PASS':
            print(f"\n🎉 All validations passed successfully!")
            return 0
        elif results['overall_status'] == 'WARN':
            print(f"\n⚠️  Validations completed with warnings")
            return 0
        else:
            print(f"\n❌ Validation failures detected")
            return 1
            
    except Exception as e:
        print(f"❌ Validation script failed: {e}")
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())
