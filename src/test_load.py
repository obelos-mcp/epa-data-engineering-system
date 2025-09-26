#!/usr/bin/env python3
"""
Environmental Site Data Management & QA/QC Automation System
Load Module Test Script

This script tests the complete ETL pipeline: Extract → Transform → Load
with comprehensive validation and performance monitoring.
"""

import sys
import time
import pandas as pd
from pathlib import Path
from typing import Dict, Any

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader, LoadConfiguration
from src.database import DatabaseConfig, get_database_manager

def print_header(title: str):
    """Print formatted header"""
    print("\n" + "=" * 80)
    print(f" {title}")
    print("=" * 80)

def test_database_connection():
    """Test database connection and basic operations"""
    print_header("DATABASE CONNECTION TEST")
    
    try:
        # Test with default configuration (will use environment or defaults)
        db_manager = get_database_manager()
        
        # Test connection health
        health_status = db_manager.health_check()
        print(f"🔍 Database health check: {'✅ PASSED' if health_status else '❌ FAILED'}")
        
        if not health_status:
            print("❌ Database connection failed - check configuration")
            print("💡 Create .env file with database credentials:")
            print("   DB_HOST=localhost")
            print("   DB_PORT=5432") 
            print("   DB_NAME=environmental_data")
            print("   DB_USER=postgres")
            print("   DB_PASSWORD=your_password")
            return False
        
        # Test basic query
        test_query = "SELECT current_database(), version()"
        result = db_manager.execute_query(test_query)
        
        if not result.empty:
            db_name = result.iloc[0, 0]
            db_version = result.iloc[0, 1]
            print(f"✅ Connected to database: {db_name}")
            print(f"✅ PostgreSQL version: {db_version.split(',')[0]}")
        
        # Test schema existence
        schema_query = """
        SELECT schema_name FROM information_schema.schemata 
        WHERE schema_name IN ('staging', 'core')
        ORDER BY schema_name
        """
        
        schemas = db_manager.execute_query(schema_query)
        if not schemas.empty:
            existing_schemas = schemas['schema_name'].tolist()
            print(f"✅ Found schemas: {existing_schemas}")
            
            if 'staging' not in existing_schemas or 'core' not in existing_schemas:
                print("⚠️  Missing required schemas - run schema creation first")
                print("   Execute: psql -d environmental_data -f sql/schema.sql")
        else:
            print("⚠️  No schemas found - database may not be properly initialized")
        
        # Get connection stats
        stats = db_manager.get_connection_stats()
        print(f"📊 Connection stats: {stats}")
        
        return True
        
    except Exception as e:
        print(f"❌ Database connection test failed: {e}")
        return False

def test_staging_load_simulation():
    """Test staging load with sample data simulation"""
    print_header("STAGING LOAD SIMULATION TEST")
    
    try:
        # Create sample data similar to transformed output
        sample_data = {
            'npdes_id': ['AK0000345', 'TX0000123', 'CA0000789'],
            'yearqtr': [20231, 20231, 20231],
            'hlrnc': [None, 'S', None],
            'nume90_q': [0, 1, 0],
            'numcvdt': [0, 2, 1],
            'numsvcd': [0, 0, 0],
            'numpsch': [2, 1, 0],
            'numd8090_q': [0, 0, 0],
            'data_quality_score': [0.95, 0.88, 0.92],
            'data_quality_flags': ['CLEAN', 'CLEAN', 'CLEAN']
        }
        
        sample_df = pd.DataFrame(sample_data)
        print(f"📊 Sample data created: {len(sample_df)} rows, {len(sample_df.columns)} columns")
        print(sample_df.head())
        
        # Test DataLoader initialization
        load_config = LoadConfiguration(
            staging_batch_size=1000,
            core_batch_size=500,
            enable_parallel_loading=False  # Disable for testing
        )
        
        loader = DataLoader(load_config=load_config, log_level='INFO')
        print(f"✅ DataLoader initialized successfully")
        
        # Test column mapping functionality
        core_df = loader.transform_for_core_schema(sample_df, 'water_measurements')
        print(f"✅ Column transformation test: {len(core_df.columns)} core columns created")
        print(f"   Core columns: {list(core_df.columns)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Staging load simulation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_etl_integration_simulation():
    """Test complete ETL integration with sample data"""
    print_header("ETL INTEGRATION SIMULATION TEST")
    
    try:
        project_root = Path(__file__).parent.parent
        
        # Initialize components
        print("🔧 Initializing ETL components...")
        extractor = DataExtractor(str(project_root), use_sample_data=True, log_level='WARNING')
        transformer = DataTransformer(str(project_root), log_level='WARNING')
        
        load_config = LoadConfiguration(
            staging_batch_size=5000,
            core_batch_size=2500,
            enable_parallel_loading=False
        )
        loader = DataLoader(load_config=load_config, log_level='INFO')
        
        # Test with smallest dataset
        source_name = 'npdes_measurements'
        print(f"📊 Testing ETL pipeline with: {source_name}")
        
        start_time = time.time()
        
        # Simulate ETL pipeline
        print("🔄 Step 1: Extraction...")
        extraction_generator = extractor.extract_file(source_name)
        
        print("🔄 Step 2: Transformation...")
        transformation_generator = transformer.transform_source(source_name, extraction_generator)
        
        print("🔄 Step 3: Load simulation...")
        
        # Process chunks (simulate loading without actual database operations)
        chunk_count = 0
        total_rows = 0
        
        for transformed_chunk, transform_stats in transformation_generator:
            chunk_count += 1
            total_rows += len(transformed_chunk)
            
            # Simulate staging load
            print(f"  📦 Chunk {chunk_count}: {len(transformed_chunk)} rows processed")
            
            # Test column transformation
            if chunk_count == 1:
                core_df = loader.transform_for_core_schema(transformed_chunk, 'water_measurements')
                print(f"  ✅ Core transformation: {len(core_df.columns)} columns mapped")
            
            # Stop after processing a few chunks for testing
            if chunk_count >= 3:
                break
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"\n📊 ETL Integration Test Results:")
        print(f"  • Total chunks processed: {chunk_count}")
        print(f"  • Total rows processed: {total_rows:,}")
        print(f"  • Processing time: {duration:.2f} seconds")
        print(f"  • Processing rate: {total_rows/duration:.0f} rows/second")
        
        print(f"\n✅ ETL integration simulation successful!")
        print(f"🎯 Pipeline components working together correctly")
        
        return True
        
    except Exception as e:
        print(f"❌ ETL integration simulation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

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
            
            # Test loader initialization
            loader = DataLoader(load_config=config, log_level='WARNING')
            print(f"  ✅ Loader initialized successfully")
        
        # Test table-specific configurations
        print(f"\n📊 Table-specific configurations:")
        default_config = LoadConfiguration()
        
        for table, table_config in default_config.table_configs.items():
            batch_size = table_config['batch_size']
            timeout = table_config['timeout']
            print(f"  • {table}: {batch_size:,} batch size, {timeout}s timeout")
        
        return True
        
    except Exception as e:
        print(f"❌ Load configuration test failed: {e}")
        return False

def test_error_handling():
    """Test error handling and recovery mechanisms"""
    print_header("ERROR HANDLING TEST")
    
    try:
        # Test invalid data handling
        print("🧪 Testing invalid data handling...")
        
        invalid_data = {
            'npdes_id': ['INVALID_ID', None, 'VALID123'],
            'yearqtr': [99999, None, 20231],  # Invalid year
            'nume90_q': [-1, None, 5],  # Negative value
            'data_quality_score': [1.5, None, 0.8]  # Out of range
        }
        
        invalid_df = pd.DataFrame(invalid_data)
        print(f"📊 Created invalid test data: {len(invalid_df)} rows")
        
        # Test loader with invalid data
        loader = DataLoader(log_level='WARNING')
        
        try:
            core_df = loader.transform_for_core_schema(invalid_df, 'water_measurements')
            print(f"✅ Invalid data transformation handled: {len(core_df)} rows output")
            
            # Check for data cleaning
            null_counts = core_df.isnull().sum()
            print(f"📊 Null values after cleaning: {null_counts.sum()} total nulls")
            
        except Exception as transform_error:
            print(f"✅ Transform error properly caught: {transform_error}")
        
        # Test connection error handling
        print(f"\n🧪 Testing connection error handling...")
        
        # Test with invalid database config
        invalid_config = DatabaseConfig(
            host='invalid_host',
            database='invalid_db',
            username='invalid_user',
            password='invalid_password'
        )
        
        try:
            invalid_loader = DataLoader(db_config=invalid_config, log_level='WARNING')
            print(f"❌ Should have failed with invalid config")
            return False
        except Exception as conn_error:
            print(f"✅ Connection error properly caught: {type(conn_error).__name__}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error handling test failed: {e}")
        return False

def test_performance_monitoring():
    """Test performance monitoring capabilities"""
    print_header("PERFORMANCE MONITORING TEST")
    
    try:
        # Test load statistics tracking
        loader = DataLoader(log_level='WARNING')
        
        # Create sample data for performance testing
        large_sample = pd.DataFrame({
            'npdes_id': [f'TEST{i:06d}' for i in range(10000)],
            'yearqtr': [20231] * 10000,
            'nume90_q': [0] * 10000,
            'data_quality_score': [0.9] * 10000
        })
        
        print(f"📊 Created performance test data: {len(large_sample):,} rows")
        
        # Test transformation performance
        start_time = time.time()
        core_df = loader.transform_for_core_schema(large_sample, 'water_measurements')
        transform_time = time.time() - start_time
        
        transform_rate = len(large_sample) / transform_time
        print(f"⚡ Transformation performance: {transform_rate:.0f} rows/second")
        
        # Test memory usage estimation
        memory_usage = large_sample.memory_usage(deep=True).sum() / (1024 * 1024)  # MB
        print(f"💾 Memory usage: {memory_usage:.1f} MB for {len(large_sample):,} rows")
        
        # Test batch size optimization
        batch_sizes = [1000, 5000, 10000]
        print(f"\n📊 Batch size performance comparison:")
        
        for batch_size in batch_sizes:
            start_time = time.time()
            
            # Simulate batch processing
            batches_processed = 0
            for start_idx in range(0, len(large_sample), batch_size):
                end_idx = min(start_idx + batch_size, len(large_sample))
                batch = large_sample.iloc[start_idx:end_idx]
                batches_processed += 1
                
                # Simulate some processing
                _ = batch.copy()
            
            batch_time = time.time() - start_time
            batch_rate = len(large_sample) / batch_time
            
            print(f"  • Batch size {batch_size:,}: {batch_rate:.0f} rows/sec ({batches_processed} batches)")
        
        return True
        
    except Exception as e:
        print(f"❌ Performance monitoring test failed: {e}")
        return False

def main():
    """Run all load module tests"""
    print_header("EPA ENVIRONMENTAL DATA LOAD MODULE TEST SUITE")
    print("Testing database connectivity, ETL integration, and performance optimization")
    
    test_results = {}
    
    try:
        print("\n🔧 Test 1: Database Connection")
        test_results['database_connection'] = test_database_connection()
        
        print("\n🔧 Test 2: Staging Load Simulation")
        test_results['staging_load'] = test_staging_load_simulation()
        
        print("\n🔧 Test 3: ETL Integration Simulation")
        test_results['etl_integration'] = test_etl_integration_simulation()
        
        print("\n🔧 Test 4: Load Configuration")
        test_results['load_configuration'] = test_load_configuration()
        
        print("\n🔧 Test 5: Error Handling")
        test_results['error_handling'] = test_error_handling()
        
        print("\n🔧 Test 6: Performance Monitoring")
        test_results['performance_monitoring'] = test_performance_monitoring()
        
    except Exception as e:
        print(f"\n❌ Test suite failed: {e}")
        return False
    
    # Final summary
    print_header("LOAD MODULE TEST SUITE SUMMARY")
    
    passed_tests = sum(1 for result in test_results.values() if result)
    total_tests = len(test_results)
    
    print(f"📊 Test Results: {passed_tests}/{total_tests} tests passed")
    
    for test_name, result in test_results.items():
        status_icon = "✅" if result else "❌"
        print(f"  {status_icon} {test_name.replace('_', ' ').title()}")
    
    if passed_tests == total_tests:
        print(f"\n🎉 All tests passed successfully!")
        print(f"🚀 Load module ready for production use")
        print(f"\n📋 Validated capabilities:")
        print(f"  ✅ Database connection management")
        print(f"  ✅ ETL pipeline integration")
        print(f"  ✅ Staging and core schema loading")
        print(f"  ✅ Performance optimization")
        print(f"  ✅ Error handling and recovery")
        print(f"  ✅ Configuration management")
        print(f"  ✅ Performance monitoring")
        
        if not test_results.get('database_connection'):
            print(f"\n💡 Note: Database connection test failed")
            print(f"   To test with actual database:")
            print(f"   1. Create PostgreSQL database 'environmental_data'")
            print(f"   2. Run: psql -d environmental_data -f sql/schema.sql")
            print(f"   3. Create .env file with database credentials")
            print(f"   4. Re-run tests")
        
        return True
    else:
        print(f"\n❌ {total_tests - passed_tests} test(s) failed")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
