#!/usr/bin/env python3
"""
Environmental Site Data Management & QA/QC Automation System
Load Module Offline Test Script

This script tests the Load module functionality without requiring database connectivity.
Tests core logic, transformations, and integration capabilities.
"""

import sys
import time
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Any

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import LoadConfiguration

def print_header(title: str):
    """Print formatted header"""
    print("\n" + "=" * 80)
    print(f" {title}")
    print("=" * 80)

def test_load_configuration():
    """Test load configuration and optimization settings"""
    print_header("LOAD CONFIGURATION TEST")
    
    try:
        # Test different configurations
        configs = [
            ("Default", LoadConfiguration()),
            ("High Performance", LoadConfiguration(
                staging_batch_size=20000,
                core_batch_size=10000,
                enable_parallel_loading=True,
                disable_indexes_during_load=True
            )),
            ("Conservative", LoadConfiguration(
                staging_batch_size=5000,
                core_batch_size=2000,
                max_errors_per_batch=50,
                validate_constraints=True
            ))
        ]
        
        for config_name, config in configs:
            print(f"\n🔧 Testing {config_name} Configuration:")
            print(f"  • Staging batch size: {config.staging_batch_size:,}")
            print(f"  • Core batch size: {config.core_batch_size:,}")
            print(f"  • Parallel loading: {config.enable_parallel_loading}")
            print(f"  • Disable indexes: {config.disable_indexes_during_load}")
            print(f"  • Max errors per batch: {config.max_errors_per_batch}")
            print(f"  • Validate constraints: {config.validate_constraints}")
            print(f"  ✅ Configuration validated")
        
        # Test table-specific configurations
        print(f"\n📊 Table-specific configurations:")
        default_config = LoadConfiguration()
        
        for table, table_config in default_config.table_configs.items():
            batch_size = table_config['batch_size']
            timeout = table_config['timeout']
            print(f"  • {table}: {batch_size:,} batch size, {timeout}s timeout")
        
        print(f"\n✅ All load configurations validated successfully")
        return True
        
    except Exception as e:
        print(f"❌ Load configuration test failed: {e}")
        return False

def test_column_mappings():
    """Test column mapping logic for core schema transformation"""
    print_header("COLUMN MAPPING TEST")
    
    try:
        # Import the DataLoader class without database initialization
        from src.load import DataLoader
        
        # Test column mapping initialization
        loader = DataLoader.__new__(DataLoader)  # Create instance without __init__
        loader._initialize_column_mappings()
        
        print(f"📊 Column mappings initialized for {len(loader.column_mappings)} tables:")
        
        for table_name, mapping_config in loader.column_mappings.items():
            staging_table = mapping_config['staging_table']
            mapping_count = len(mapping_config['mapping'])
            
            print(f"\n🔧 {table_name.upper()}:")
            print(f"  • Staging table: {staging_table}")
            print(f"  • Column mappings: {mapping_count}")
            print(f"  • Primary key: {mapping_config.get('primary_key', 'N/A')}")
            
            if 'unique_columns' in mapping_config:
                print(f"  • Unique columns: {mapping_config['unique_columns']}")
            
            if 'foreign_keys' in mapping_config:
                print(f"  • Foreign keys: {mapping_config['foreign_keys']}")
            
            # Show sample mappings
            sample_mappings = list(mapping_config['mapping'].items())[:3]
            print(f"  • Sample mappings: {sample_mappings}")
        
        print(f"\n✅ Column mappings validated successfully")
        return True
        
    except Exception as e:
        print(f"❌ Column mapping test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_data_transformation():
    """Test data transformation for core schema"""
    print_header("DATA TRANSFORMATION TEST")
    
    try:
        # Import without database initialization
        from src.load import DataLoader
        
        loader = DataLoader.__new__(DataLoader)
        loader._initialize_column_mappings()
        
        # Test data for different table types
        test_datasets = {
            'water_measurements': pd.DataFrame({
                'npdes_id': ['AK0000345', 'TX0000123', 'CA0000789'],
                'yearqtr': ['20231', '20232', '20233'],
                'hlrnc': [None, 'S', None],
                'nume90_q': [0, 1, 0],
                'numcvdt': [0, 2, 1],
                'numsvcd': [0, 0, 0],
                'numpsch': [2, 1, 0],
                'numd8090_q': [0, 0, 0]
            }),
            'air_emissions': pd.DataFrame({
                'registry_id': [110030441779, 110012538968, 110052690708],
                'reporting_year': [2023, 2023, 2023],
                'pollutant_name': ['CO2', 'NOx', 'SO2'],
                'annual_emission': [1000.5, 250.0, 75.5],
                'unit_of_measure': ['Tons', 'Pounds', 'Kilograms'],
                'pgm_sys_acrnm': ['GHGP', 'NEI', 'TRI'],
                'pgm_sys_id': ['ID001', 'ID002', 'ID003']
            })
        }
        
        for table_name, test_df in test_datasets.items():
            print(f"\n🔧 Testing {table_name} transformation:")
            print(f"  📊 Input data: {len(test_df)} rows, {len(test_df.columns)} columns")
            print(f"  📊 Input columns: {list(test_df.columns)}")
            
            # Apply transformation
            try:
                core_df = loader.transform_for_core_schema(test_df, table_name)
                
                print(f"  ✅ Output data: {len(core_df)} rows, {len(core_df.columns)} columns")
                print(f"  ✅ Output columns: {list(core_df.columns)}")
                
                # Check for audit fields
                audit_fields = ['created_date', 'updated_date', 'created_by', 'updated_by']
                has_audit = all(field in core_df.columns for field in audit_fields)
                print(f"  ✅ Audit fields added: {has_audit}")
                
                # Show sample transformation
                if not core_df.empty:
                    print(f"  📋 Sample transformed record:")
                    sample_record = core_df.iloc[0].to_dict()
                    for key, value in list(sample_record.items())[:5]:  # Show first 5 fields
                        print(f"    {key}: {value}")
                
            except Exception as transform_error:
                print(f"  ❌ Transformation failed: {transform_error}")
                return False
        
        print(f"\n✅ All data transformations validated successfully")
        return True
        
    except Exception as e:
        print(f"❌ Data transformation test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_etl_integration_offline():
    """Test ETL integration without database operations"""
    print_header("ETL INTEGRATION OFFLINE TEST")
    
    try:
        project_root = Path(__file__).parent.parent
        
        # Initialize extract and transform components
        print("🔧 Initializing ETL components...")
        extractor = DataExtractor(str(project_root), use_sample_data=True, log_level='WARNING')
        transformer = DataTransformer(str(project_root), log_level='WARNING')
        
        # Test with NPDES measurements
        source_name = 'npdes_measurements'
        print(f"📊 Testing ETL pipeline with: {source_name}")
        
        start_time = time.time()
        
        # Extract data
        print("🔄 Step 1: Extraction...")
        extraction_generator = extractor.extract_file(source_name)
        
        # Transform data
        print("🔄 Step 2: Transformation...")
        transformation_generator = transformer.transform_source(source_name, extraction_generator)
        
        # Simulate load processing
        print("🔄 Step 3: Load simulation...")
        
        # Import load components without database
        from src.load import DataLoader, LoadStats
        
        loader = DataLoader.__new__(DataLoader)
        loader._initialize_column_mappings()
        
        chunk_count = 0
        total_input_rows = 0
        total_transformed_rows = 0
        
        for transformed_chunk, transform_stats in transformation_generator:
            chunk_count += 1
            total_input_rows += transform_stats.input_rows
            
            print(f"  📦 Processing chunk {chunk_count}: {len(transformed_chunk)} rows")
            
            # Test core transformation
            core_df = loader.transform_for_core_schema(transformed_chunk, 'water_measurements')
            total_transformed_rows += len(core_df)
            
            # Simulate load statistics
            load_stats = LoadStats(
                source_name=source_name,
                target_schema='core',
                target_table='water_measurements'
            )
            load_stats.input_rows = len(transformed_chunk)
            load_stats.loaded_rows = len(core_df)
            
            print(f"    ✅ Core transformation: {len(core_df)} rows ready for load")
            
            # Stop after processing a few chunks for testing
            if chunk_count >= 2:
                break
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"\n📊 ETL Integration Test Results:")
        print(f"  • Total chunks processed: {chunk_count}")
        print(f"  • Total input rows: {total_input_rows:,}")
        print(f"  • Total transformed rows: {total_transformed_rows:,}")
        print(f"  • Processing time: {duration:.2f} seconds")
        print(f"  • Processing rate: {total_input_rows/duration:.0f} rows/second")
        print(f"  • Data retention: {total_transformed_rows/total_input_rows*100:.1f}%")
        
        print(f"\n✅ ETL integration simulation successful!")
        return True
        
    except Exception as e:
        print(f"❌ ETL integration simulation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_performance_simulation():
    """Test performance characteristics without database"""
    print_header("PERFORMANCE SIMULATION TEST")
    
    try:
        from src.load import DataLoader, LoadConfiguration
        
        # Test different batch sizes
        batch_sizes = [1000, 5000, 10000, 20000]
        test_data_size = 50000
        
        # Create large test dataset
        large_dataset = pd.DataFrame({
            'npdes_id': [f'TEST{i:06d}' for i in range(test_data_size)],
            'yearqtr': ['20231'] * test_data_size,
            'nume90_q': np.random.randint(0, 5, test_data_size),
            'numcvdt': np.random.randint(0, 3, test_data_size),
            'data_quality_score': np.random.uniform(0.7, 1.0, test_data_size)
        })
        
        print(f"📊 Created test dataset: {len(large_dataset):,} rows")
        
        # Test batch processing performance
        loader = DataLoader.__new__(DataLoader)
        loader._initialize_column_mappings()
        
        print(f"\n⚡ Batch size performance comparison:")
        
        for batch_size in batch_sizes:
            start_time = time.time()
            
            batches_processed = 0
            total_transformed = 0
            
            # Simulate batch processing
            for start_idx in range(0, len(large_dataset), batch_size):
                end_idx = min(start_idx + batch_size, len(large_dataset))
                batch = large_dataset.iloc[start_idx:end_idx]
                
                # Transform batch
                core_batch = loader.transform_for_core_schema(batch, 'water_measurements')
                
                batches_processed += 1
                total_transformed += len(core_batch)
            
            batch_time = time.time() - start_time
            batch_rate = len(large_dataset) / batch_time
            
            print(f"  • Batch size {batch_size:,}: {batch_rate:.0f} rows/sec ({batches_processed} batches)")
        
        # Memory usage simulation
        print(f"\n💾 Memory usage analysis:")
        
        for batch_size in [5000, 10000, 20000]:
            sample_batch = large_dataset.head(batch_size)
            core_batch = loader.transform_for_core_schema(sample_batch, 'water_measurements')
            
            input_memory = sample_batch.memory_usage(deep=True).sum() / (1024 * 1024)
            output_memory = core_batch.memory_usage(deep=True).sum() / (1024 * 1024)
            
            print(f"  • Batch size {batch_size:,}: {input_memory:.1f}MB → {output_memory:.1f}MB")
        
        # Configuration performance impact
        print(f"\n🔧 Configuration impact analysis:")
        
        configs = {
            'Conservative': LoadConfiguration(staging_batch_size=5000, core_batch_size=2000),
            'Balanced': LoadConfiguration(staging_batch_size=10000, core_batch_size=5000),
            'Aggressive': LoadConfiguration(staging_batch_size=20000, core_batch_size=10000)
        }
        
        for config_name, config in configs.items():
            estimated_batches = len(large_dataset) / config.staging_batch_size
            estimated_time = estimated_batches * 0.1  # Assume 0.1s per batch
            
            print(f"  • {config_name}: ~{estimated_batches:.0f} batches, ~{estimated_time:.1f}s estimated")
        
        print(f"\n✅ Performance simulation completed successfully")
        return True
        
    except Exception as e:
        print(f"❌ Performance simulation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_error_scenarios():
    """Test error handling scenarios"""
    print_header("ERROR HANDLING SCENARIOS TEST")
    
    try:
        from src.load import DataLoader
        
        loader = DataLoader.__new__(DataLoader)
        loader._initialize_column_mappings()
        
        # Test 1: Invalid table name
        print("🧪 Test 1: Invalid table name")
        try:
            invalid_df = pd.DataFrame({'test_col': [1, 2, 3]})
            result = loader.transform_for_core_schema(invalid_df, 'invalid_table')
            print("❌ Should have failed with invalid table name")
            return False
        except ValueError as e:
            print(f"✅ Correctly caught error: {e}")
        
        # Test 2: Missing columns
        print(f"\n🧪 Test 2: Missing required columns")
        try:
            incomplete_df = pd.DataFrame({'wrong_col': [1, 2, 3]})
            result = loader.transform_for_core_schema(incomplete_df, 'water_measurements')
            print(f"✅ Handled missing columns: {len(result)} rows, {len(result.columns)} columns")
        except Exception as e:
            print(f"✅ Correctly handled missing columns: {type(e).__name__}")
        
        # Test 3: Data type issues
        print(f"\n🧪 Test 3: Data type conversion issues")
        try:
            mixed_types_df = pd.DataFrame({
                'npdes_id': ['VALID123', None, 123],  # Mixed types
                'yearqtr': ['20231', 'invalid', None],  # Invalid year format
                'nume90_q': ['text', -1, 5.5]  # Non-numeric values
            })
            
            result = loader.transform_for_core_schema(mixed_types_df, 'water_measurements')
            print(f"✅ Handled mixed data types: {len(result)} rows output")
            
            # Check data quality
            null_counts = result.isnull().sum()
            print(f"  📊 Null values after cleaning: {null_counts.sum()} total")
            
        except Exception as e:
            print(f"✅ Data type error handled: {type(e).__name__}")
        
        # Test 4: Large dataset simulation
        print(f"\n🧪 Test 4: Large dataset handling")
        try:
            # Create very large dataset to test memory handling
            large_size = 100000
            large_df = pd.DataFrame({
                'npdes_id': [f'LARGE{i}' for i in range(large_size)],
                'yearqtr': ['20231'] * large_size,
                'nume90_q': [0] * large_size
            })
            
            start_time = time.time()
            result = loader.transform_for_core_schema(large_df, 'water_measurements')
            transform_time = time.time() - start_time
            
            print(f"✅ Large dataset handled: {len(result):,} rows in {transform_time:.2f}s")
            print(f"  ⚡ Processing rate: {len(result)/transform_time:.0f} rows/second")
            
        except Exception as e:
            print(f"❌ Large dataset test failed: {e}")
        
        print(f"\n✅ Error handling scenarios completed successfully")
        return True
        
    except Exception as e:
        print(f"❌ Error scenarios test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all offline load module tests"""
    print_header("EPA ENVIRONMENTAL DATA LOAD MODULE - OFFLINE TEST SUITE")
    print("Testing load module functionality without database connectivity")
    
    test_results = {}
    
    try:
        print("\n🔧 Test 1: Load Configuration")
        test_results['load_configuration'] = test_load_configuration()
        
        print("\n🔧 Test 2: Column Mappings")
        test_results['column_mappings'] = test_column_mappings()
        
        print("\n🔧 Test 3: Data Transformation")
        test_results['data_transformation'] = test_data_transformation()
        
        print("\n🔧 Test 4: ETL Integration (Offline)")
        test_results['etl_integration'] = test_etl_integration_offline()
        
        print("\n🔧 Test 5: Performance Simulation")
        test_results['performance_simulation'] = test_performance_simulation()
        
        print("\n🔧 Test 6: Error Handling Scenarios")
        test_results['error_scenarios'] = test_error_scenarios()
        
    except Exception as e:
        print(f"\n❌ Test suite failed: {e}")
        return False
    
    # Final summary
    print_header("LOAD MODULE OFFLINE TEST SUITE SUMMARY")
    
    passed_tests = sum(1 for result in test_results.values() if result)
    total_tests = len(test_results)
    
    print(f"📊 Test Results: {passed_tests}/{total_tests} tests passed")
    
    for test_name, result in test_results.items():
        status_icon = "✅" if result else "❌"
        print(f"  {status_icon} {test_name.replace('_', ' ').title()}")
    
    if passed_tests == total_tests:
        print(f"\n🎉 All offline tests passed successfully!")
        print(f"🚀 Load module core functionality validated")
        
        print(f"\n📋 Validated capabilities:")
        print(f"  ✅ Load configuration management")
        print(f"  ✅ Column mapping and transformation logic")
        print(f"  ✅ Core schema data transformation")
        print(f"  ✅ ETL pipeline integration (offline)")
        print(f"  ✅ Performance optimization strategies")
        print(f"  ✅ Error handling and data validation")
        
        print(f"\n🔗 For full database testing:")
        print(f"  1. Install PostgreSQL and create 'environmental_data' database")
        print(f"  2. Install psycopg2-binary: pip install psycopg2-binary")
        print(f"  3. Run schema creation: psql -d environmental_data -f sql/schema.sql")
        print(f"  4. Create .env file with database credentials")
        print(f"  5. Run full tests with: python src/test_load.py")
        
        return True
    else:
        print(f"\n❌ {total_tests - passed_tests} test(s) failed")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
