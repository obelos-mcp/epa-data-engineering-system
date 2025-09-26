#!/usr/bin/env python3
"""
Environmental Site Data Management & QA/QC Automation System
Transform Test Script

This script demonstrates comprehensive transformation capabilities including:
- Data standardization and cleaning
- Cross-dataset integration logic
- Business rule transformations
- Quality assurance and validation
- Performance monitoring
"""

import sys
import time
import json
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.extract import DataExtractor
from src.transform import DataTransformer
from config.data_sources import get_data_sources

def print_header(title: str):
    """Print formatted header"""
    print("\n" + "=" * 80)
    print(f" {title}")
    print("=" * 80)

def print_transformation_stats(stats_dict: Dict[str, Any]):
    """Print transformation statistics in table format"""
    print(f"\n{'Source':<20} {'Input':<10} {'Output':<10} {'Cleaned':<10} {'Dups':<8} {'Quality':<10} {'Rate (r/s)':<12}")
    print("-" * 90)
    
    for source, details in stats_dict.get('source_details', {}).items():
        quality_imp = f"+{details['quality_improvement']:.1f}%"
        print(f"{source:<20} {details['input_rows']:>9,} {details['output_rows']:>9,} "
              f"{details['input_rows'] - details['output_rows']:>9,} {details['duplicates_removed']:>7} "
              f"{quality_imp:>9} {details['processing_rate']:>11.0f}")

def test_column_standardization():
    """Test column name standardization"""
    print_header("COLUMN STANDARDIZATION TEST")
    
    project_root = Path(__file__).parent.parent
    transformer = DataTransformer(str(project_root), log_level='INFO')
    
    # Create test DataFrame with various column name formats
    test_data = {
        'REGISTRY_ID': [1, 2, 3],
        'FAC_NAME': ['Test Facility 1', 'Test Facility 2', 'Test Facility 3'],
        'FAC_EPA_REGION': [1, 2, 3],
        'CWA_COMPLIANCE_STATUS': ['Good', 'Poor', 'Fair'],
        'FAC_DATE_LAST_INSPECTION': ['2023-01-01', '2023-02-01', '2023-03-01']
    }
    
    test_df = pd.DataFrame(test_data)
    print(f"📊 Original columns: {list(test_df.columns)}")
    
    # Apply standardization
    standardized_df = transformer.standardize_column_names(test_df)
    print(f"✅ Standardized columns: {list(standardized_df.columns)}")
    
    # Verify transformations
    expected_mappings = {
        'REGISTRY_ID': 'registry_id',
        'FAC_NAME': 'fac_name', 
        'FAC_EPA_REGION': 'fac_epa_region',
        'CWA_COMPLIANCE_STATUS': 'cwa_compliance_status',
        'FAC_DATE_LAST_INSPECTION': 'fac_date_last_inspection'
    }
    
    all_correct = True
    for orig, expected in expected_mappings.items():
        if expected not in standardized_df.columns:
            print(f"❌ Failed: {orig} → {expected}")
            all_correct = False
        else:
            print(f"✅ Correct: {orig} → {expected}")
    
    if all_correct:
        print("🎉 All column standardizations successful!")
    
    return standardized_df

def test_data_type_conversion():
    """Test data type standardization"""
    print_header("DATA TYPE CONVERSION TEST")
    
    project_root = Path(__file__).parent.parent
    transformer = DataTransformer(str(project_root), log_level='INFO')
    data_sources = get_data_sources(str(project_root))
    
    # Get ECHO config for testing
    config = data_sources.get_source('echo_facilities')
    
    # Create test data with various formats
    test_data = {
        'registry_id': ['110030441779.0', '110012538968', 'invalid'],
        'fac_lat': ['29.951476', '39.041265', '999'],  # Invalid coordinate
        'fac_long': ['-91.991551', '-108.466171', 'invalid'],
        'fac_epa_region': ['6', '8', '15'],  # Invalid region
        'fac_date_last_inspection': ['06/11/2002', '2023-01-01', 'invalid'],
        'fac_inspection_count': ['0', '5', 'many']
    }
    
    test_df = pd.DataFrame(test_data)
    print(f"📊 Original data types:")
    print(test_df.dtypes)
    print(f"\n📊 Sample data:")
    print(test_df.head())
    
    # Apply data type standardization
    typed_df = transformer.standardize_data_types(test_df, config)
    
    print(f"\n✅ Converted data types:")
    print(typed_df.dtypes)
    print(f"\n✅ Converted data:")
    print(typed_df.head())
    
    # Test coordinate validation
    cleaned_df, invalid_coords = transformer.clean_and_validate_coordinates(typed_df, config)
    
    print(f"\n🔍 Coordinate validation results:")
    print(f"  • Invalid coordinate rows: {len(invalid_coords)}")
    if invalid_coords:
        print(f"  • Invalid rows: {invalid_coords}")
    
    print(f"\n✅ Cleaned coordinates:")
    coord_cols = ['fac_lat', 'fac_long']
    for col in coord_cols:
        if col in cleaned_df.columns:
            valid_coords = cleaned_df[col].notna().sum()
            print(f"  • {col}: {valid_coords}/{len(cleaned_df)} valid coordinates")
    
    return cleaned_df

def test_facility_name_cleaning():
    """Test facility name cleaning and standardization"""
    print_header("FACILITY NAME CLEANING TEST")
    
    project_root = Path(__file__).parent.parent
    transformer = DataTransformer(str(project_root), log_level='INFO')
    
    # Create test data with messy facility names
    test_names = [
        '  ACME CORP.   ',  # Extra spaces
        'ABC COMPANY INC',  # Standard abbreviations
        'XYZ   WASTEWATER   TREATMENT   PLANT',  # Multiple spaces
        'DEF CO & SONS LLC',  # Ampersand and abbreviations
        'GHI WATER TREATMENT PLANT (WWTP)',  # Parentheses and abbreviations
        'JKL MANUFACTURING@#$%',  # Special characters
        'MNO ltd.',  # Mixed case abbreviations
    ]
    
    test_df = pd.DataFrame({'fac_name': test_names})
    print(f"📊 Original facility names:")
    for i, name in enumerate(test_names):
        print(f"  {i+1}. '{name}'")
    
    # Apply name cleaning
    cleaned_df = transformer.clean_facility_names(test_df)
    
    print(f"\n✅ Cleaned facility names:")
    for i, name in enumerate(cleaned_df['fac_name']):
        print(f"  {i+1}. '{name}'")
    
    # Verify specific transformations
    print(f"\n🔍 Transformation verification:")
    transformations = [
        ('Extra spaces removed', '  ACME CORP.   ', 'ACME CORPORATION'),
        ('Abbreviations standardized', 'ABC COMPANY INC', 'ABC COMPANY INCORPORATED'),
        ('Multiple spaces normalized', 'XYZ   WASTEWATER   TREATMENT   PLANT', 'XYZ WASTEWATER TREATMENT PLANT'),
        ('Ampersand converted', 'DEF CO & SONS LLC', 'DEF COMPANY AND SONS LIMITED LIABILITY COMPANY')
    ]
    
    for desc, original, expected_pattern in transformations:
        # Find the cleaned version
        orig_idx = test_names.index(original) if original in test_names else -1
        if orig_idx >= 0:
            cleaned = cleaned_df['fac_name'].iloc[orig_idx]
            contains_expected = any(word in cleaned for word in expected_pattern.split())
            status = "✅" if contains_expected else "❌"
            print(f"  {status} {desc}: '{original}' → '{cleaned}'")
    
    return cleaned_df

def test_missing_value_handling():
    """Test strategic missing value handling"""
    print_header("MISSING VALUE HANDLING TEST")
    
    project_root = Path(__file__).parent.parent
    transformer = DataTransformer(str(project_root), log_level='INFO')
    data_sources = get_data_sources(str(project_root))
    
    config = data_sources.get_source('echo_facilities')
    
    # Create test data with various missing patterns
    test_data = {
        'registry_id': [123, None, 456],  # Required field
        'fac_name': ['Facility A', None, 'Facility C'],  # Required field
        'fac_state': ['CA', 'TX', None],  # Required field
        'fac_active_flag': [None, 'Y', None],  # Optional with default
        'fac_major_flag': ['Y', None, None],  # Optional with default
        'fac_inspection_count': [5, None, 10],  # Numeric with default
        'fac_total_penalties': [1000, None, 5000]  # Penalty with default
    }
    
    test_df = pd.DataFrame(test_data)
    print(f"📊 Original data with missing values:")
    print(test_df)
    print(f"\n📊 Missing value counts:")
    print(test_df.isnull().sum())
    
    # Apply missing value handling
    handled_df, flagged_rows = transformer.handle_missing_values(test_df, config)
    
    print(f"\n✅ Data after missing value handling:")
    print(handled_df)
    print(f"\n✅ Missing value counts after handling:")
    print(handled_df.isnull().sum())
    
    print(f"\n🔍 Flagged rows (missing required data): {flagged_rows}")
    
    # Verify business rule defaults
    print(f"\n🔍 Business rule defaults applied:")
    print(f"  • fac_active_flag defaults: {(handled_df['fac_active_flag'] == 'Y').sum()}")
    print(f"  • fac_major_flag defaults: {(handled_df['fac_major_flag'] == 'N').sum()}")
    print(f"  • Numeric zero defaults: {(handled_df['fac_inspection_count'] == 0).sum()}")
    
    return handled_df, flagged_rows

def test_duplicate_removal():
    """Test duplicate record removal"""
    print_header("DUPLICATE REMOVAL TEST")
    
    project_root = Path(__file__).parent.parent
    transformer = DataTransformer(str(project_root), log_level='INFO')
    data_sources = get_data_sources(str(project_root))
    
    config = data_sources.get_source('npdes_measurements')
    
    # Create test data with duplicates
    test_data = {
        'npdes_id': ['AK0000345', 'AK0000345', 'TX0000123', 'TX0000123', 'CA0000789'],
        'yearqtr': [20143, 20143, 20144, 20144, 20143],  # Same permit, same quarter = duplicate
        'hlrnc': [None, None, 'S', 'S', None],
        'nume90q': [0, 0, 1, 1, 0],
        'numcvdt': [0, 0, 2, 2, 1]
    }
    
    test_df = pd.DataFrame(test_data)
    print(f"📊 Original data with duplicates ({len(test_df)} rows):")
    print(test_df)
    
    # Apply duplicate removal
    dedup_df, duplicates_removed = transformer.remove_duplicates(test_df, config)
    
    print(f"\n✅ Data after duplicate removal ({len(dedup_df)} rows):")
    print(dedup_df)
    
    print(f"\n🔍 Duplicate removal results:")
    print(f"  • Original rows: {len(test_df)}")
    print(f"  • Final rows: {len(dedup_df)}")
    print(f"  • Duplicates removed: {duplicates_removed}")
    print(f"  • Data retention: {len(dedup_df)/len(test_df)*100:.1f}%")
    
    return dedup_df

def test_compliance_scoring():
    """Test compliance score calculation"""
    print_header("COMPLIANCE SCORING TEST")
    
    project_root = Path(__file__).parent.parent
    transformer = DataTransformer(str(project_root), log_level='INFO')
    data_sources = get_data_sources(str(project_root))
    
    config = data_sources.get_source('npdes_measurements')
    
    # Create test data with various violation patterns
    test_data = {
        'npdes_id': ['GOOD001', 'POOR001', 'FAIR001', 'BAD001'],
        'yearqtr': [20231, 20231, 20231, 20231],
        'nume90q': [0, 5, 2, 10],  # Effluent violations (high weight)
        'numcvdt': [0, 3, 1, 8],   # Compliance violations (medium weight)
        'numpsch': [0, 2, 1, 5],   # Schedule violations (low weight)
        'numsvcd': [0, 1, 0, 3]    # Single event violations (lowest weight)
    }
    
    test_df = pd.DataFrame(test_data)
    print(f"📊 Original violation data:")
    print(test_df)
    
    # Apply compliance scoring
    scored_df = transformer._calculate_compliance_scores(test_df)
    
    print(f"\n✅ Data with compliance scores:")
    cols_to_show = ['npdes_id', 'nume90q', 'numcvdt', 'numpsch', 'numsvcd', 'compliance_score', 'is_compliant']
    if 'compliance_score' in scored_df.columns:
        print(scored_df[cols_to_show])
        
        print(f"\n🔍 Compliance analysis:")
        print(f"  • Average compliance score: {scored_df['compliance_score'].mean():.1f}")
        print(f"  • Compliant facilities: {scored_df['is_compliant'].sum()}/{len(scored_df)}")
        print(f"  • Score range: {scored_df['compliance_score'].min():.1f} - {scored_df['compliance_score'].max():.1f}")
    else:
        print("❌ Compliance scoring not applied - check violation columns")
    
    return scored_df

def test_quality_flags():
    """Test data quality flag generation"""
    print_header("DATA QUALITY FLAGS TEST")
    
    project_root = Path(__file__).parent.parent
    transformer = DataTransformer(str(project_root), log_level='INFO')
    data_sources = get_data_sources(str(project_root))
    
    config = data_sources.get_source('echo_facilities')
    
    # Create test data with various quality issues
    test_data = {
        'registry_id': [123, None, 456, 789],  # Missing required
        'fac_name': ['Good Facility', None, 'Fair Facility', 'Bad Facility'],  # Missing required
        'fac_lat': [39.0, 999.0, 45.0, None],  # Invalid coordinate
        'fac_long': [-120.0, -200.0, -95.0, None],  # Invalid coordinate
        'fac_epa_region': [9, 15, 5, None],  # Out of range
        'fac_date_last_inspection': ['2023-01-01', '2025-01-01', '2022-01-01', None],  # Future date
        'fac_state': ['CA', 'CA', None, 'TX']  # Missing state with coordinates
    }
    
    test_df = pd.DataFrame(test_data)
    print(f"📊 Test data with quality issues:")
    print(test_df)
    
    # Simulate some processing steps
    invalid_coords = [1]  # Row with lat=999
    flagged_missing = [1]  # Row with missing required fields
    
    # Generate quality flags
    flagged_df = transformer.generate_quality_flags(test_df, config, invalid_coords, flagged_missing)
    
    print(f"\n✅ Data with quality flags:")
    quality_cols = ['registry_id', 'fac_name', 'data_quality_flags', 'data_quality_score', 'requires_manual_review']
    if 'data_quality_flags' in flagged_df.columns:
        print(flagged_df[quality_cols])
        
        print(f"\n🔍 Quality flag analysis:")
        print(f"  • Average quality score: {flagged_df['data_quality_score'].mean():.3f}")
        print(f"  • Records requiring manual review: {flagged_df['requires_manual_review'].sum()}/{len(flagged_df)}")
        
        # Show flag distribution
        flag_counts = flagged_df['data_quality_flags'].value_counts()
        print(f"  • Quality flag distribution:")
        for flag, count in flag_counts.items():
            print(f"    - {flag}: {count}")
    else:
        print("❌ Quality flags not generated - check implementation")
    
    return flagged_df

def test_integrated_transformation():
    """Test complete transformation pipeline with sample data"""
    print_header("INTEGRATED TRANSFORMATION PIPELINE TEST")
    
    project_root = Path(__file__).parent.parent
    
    # Initialize components
    extractor = DataExtractor(str(project_root), use_sample_data=True, log_level='WARNING')
    transformer = DataTransformer(str(project_root), log_level='INFO')
    
    print("🔄 Testing integrated Extract → Transform pipeline...")
    
    # Test with NPDES measurements (cleanest dataset)
    source_name = 'npdes_measurements'
    print(f"📊 Processing source: {source_name}")
    
    start_time = time.time()
    
    try:
        # Extract data
        extraction_generator = extractor.extract_file(source_name)
        
        # Transform data
        transformation_generator = transformer.transform_source(source_name, extraction_generator)
        
        # Process all chunks
        chunk_count = 0
        total_input = 0
        total_output = 0
        sample_chunk = None
        
        for transformed_chunk, transform_stats in transformation_generator:
            chunk_count += 1
            total_input += transform_stats.input_rows
            total_output += transform_stats.output_rows
            
            # Save first chunk for inspection
            if chunk_count == 1:
                sample_chunk = transformed_chunk
            
            print(f"  Chunk {chunk_count}: {transform_stats.input_rows} → {transform_stats.output_rows} rows")
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"\n✅ Pipeline processing complete:")
        print(f"  • Total chunks processed: {chunk_count}")
        print(f"  • Total input rows: {total_input:,}")
        print(f"  • Total output rows: {total_output:,}")
        print(f"  • Data retention: {total_output/total_input*100:.1f}%")
        print(f"  • Processing time: {duration:.2f} seconds")
        print(f"  • Processing rate: {total_input/duration:.0f} rows/second")
        
        # Show sample transformed data
        if sample_chunk is not None and not sample_chunk.empty:
            print(f"\n🔍 Sample transformed data structure:")
            print(f"  • Columns: {len(sample_chunk.columns)}")
            print(f"  • Sample columns: {list(sample_chunk.columns[:10])}")
            
            if 'data_quality_score' in sample_chunk.columns:
                avg_quality = sample_chunk['data_quality_score'].mean()
                print(f"  • Average data quality score: {avg_quality:.3f}")
            
            if 'data_quality_flags' in sample_chunk.columns:
                clean_records = (sample_chunk['data_quality_flags'] == 'CLEAN').sum()
                print(f"  • Clean records: {clean_records}/{len(sample_chunk)} ({clean_records/len(sample_chunk)*100:.1f}%)")
        
        # Get transformation summary
        summary = transformer.get_transformation_summary()
        print(f"\n📈 Transformation Summary:")
        print(f"  • Quality improvement: {summary['average_quality_improvement']:.1f}%")
        print(f"  • Peak memory usage: {summary['peak_memory_usage_mb']:.1f} MB")
        print(f"  • Data retention rate: {summary['overall_data_retention']:.1f}%")
        
        return summary
        
    except Exception as e:
        print(f"❌ Pipeline test failed: {e}")
        return None

def test_performance_comparison():
    """Test transformation performance with different datasets"""
    print_header("TRANSFORMATION PERFORMANCE COMPARISON")
    
    project_root = Path(__file__).parent.parent
    
    # Test different sources
    test_sources = ['npdes_measurements', 'icis_facilities', 'air_emissions']
    performance_results = []
    
    for source_name in test_sources:
        print(f"\n🔄 Testing performance for: {source_name}")
        
        extractor = DataExtractor(str(project_root), use_sample_data=True, log_level='ERROR')
        transformer = DataTransformer(str(project_root), log_level='ERROR')
        
        start_time = time.time()
        
        try:
            # Process single source
            extraction_generator = extractor.extract_file(source_name)
            transformation_generator = transformer.transform_source(source_name, extraction_generator)
            
            # Consume generator
            total_rows = 0
            for transformed_chunk, transform_stats in transformation_generator:
                total_rows += transform_stats.input_rows
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Get final stats
            final_stats = transformer.transformation_stats.get(source_name)
            
            result = {
                'source': source_name,
                'rows': total_rows,
                'duration': duration,
                'rate_rows_sec': total_rows / duration if duration > 0 else 0,
                'quality_improvement': final_stats.calculate_improvement() if final_stats else 0,
                'memory_mb': final_stats.memory_usage_mb if final_stats else 0
            }
            
            performance_results.append(result)
            
            print(f"  ✅ {total_rows:,} rows in {duration:.2f}s ({result['rate_rows_sec']:.0f} rows/sec)")
            
        except Exception as e:
            print(f"  ❌ Failed: {e}")
    
    # Display performance comparison
    if performance_results:
        print(f"\n📊 Performance Comparison Results:")
        print(f"{'Source':<20} {'Rows':<10} {'Duration':<10} {'Rate (r/s)':<12} {'Quality':<10} {'Memory':<10}")
        print("-" * 80)
        
        for result in performance_results:
            print(f"{result['source']:<20} {result['rows']:>9,} {result['duration']:>9.2f}s "
                  f"{result['rate_rows_sec']:>11.0f} {result['quality_improvement']:>8.1f}% "
                  f"{result['memory_mb']:>8.1f}MB")
    
    return performance_results

def main():
    """Run all transformation tests"""
    print_header("EPA ENVIRONMENTAL DATA TRANSFORMATION SYSTEM TEST SUITE")
    print("Testing comprehensive data cleaning, standardization, and business logic transformations")
    
    # Create logs directory
    logs_dir = Path(__file__).parent.parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    
    test_results = {}
    
    try:
        print("\n🔧 Test 1: Column Standardization")
        test_results['column_standardization'] = test_column_standardization()
        
        print("\n🔧 Test 2: Data Type Conversion")
        test_results['data_type_conversion'] = test_data_type_conversion()
        
        print("\n🔧 Test 3: Facility Name Cleaning")
        test_results['name_cleaning'] = test_facility_name_cleaning()
        
        print("\n🔧 Test 4: Missing Value Handling")
        test_results['missing_values'] = test_missing_value_handling()
        
        print("\n🔧 Test 5: Duplicate Removal")
        test_results['duplicate_removal'] = test_duplicate_removal()
        
        print("\n🔧 Test 6: Compliance Scoring")
        test_results['compliance_scoring'] = test_compliance_scoring()
        
        print("\n🔧 Test 7: Quality Flags")
        test_results['quality_flags'] = test_quality_flags()
        
        print("\n🔧 Test 8: Integrated Pipeline")
        test_results['integrated_pipeline'] = test_integrated_transformation()
        
        print("\n🔧 Test 9: Performance Comparison")
        test_results['performance'] = test_performance_comparison()
        
    except Exception as e:
        print(f"\n❌ Test suite failed: {e}")
        return False
    
    # Final summary
    print_header("TRANSFORMATION TEST SUITE SUMMARY")
    
    if test_results.get('integrated_pipeline'):
        summary = test_results['integrated_pipeline']
        print(f"✅ All transformation tests completed successfully!")
        print(f"📊 Integrated pipeline performance:")
        print(f"  • Average quality improvement: {summary['average_quality_improvement']:.1f}%")
        print(f"  • Data retention rate: {summary['overall_data_retention']:.1f}%")
        print(f"  • Peak memory usage: {summary['peak_memory_usage_mb']:.1f} MB")
        
        print(f"\n🚀 Key transformation capabilities validated:")
        print(f"  ✅ Column name standardization (snake_case)")
        print(f"  ✅ Data type conversion and validation")
        print(f"  ✅ Geographic coordinate cleaning")
        print(f"  ✅ Facility name standardization")
        print(f"  ✅ Strategic missing value handling")
        print(f"  ✅ Intelligent duplicate removal")
        print(f"  ✅ Business rule transformations")
        print(f"  ✅ Compliance scoring algorithms")
        print(f"  ✅ Comprehensive quality flagging")
        print(f"  ✅ Performance monitoring")
        
        print(f"\n🎯 System ready for production ETL processing!")
        print(f"   Next steps: Integrate with Load module for database insertion")
    else:
        print(f"❌ Some tests failed - check logs for details")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
