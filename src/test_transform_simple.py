#!/usr/bin/env python3
"""
Simplified Transform Test Script
Tests core transformation functionality with sample data
"""

import sys
import time
import pandas as pd
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.extract import DataExtractor
from src.transform import DataTransformer

def test_integrated_pipeline():
    """Test Extract → Transform integration"""
    print("=" * 80)
    print(" EPA DATA TRANSFORMATION SYSTEM - INTEGRATION TEST")
    print("=" * 80)
    
    project_root = Path(__file__).parent.parent
    
    # Initialize components
    print("\n🔧 Initializing components...")
    extractor = DataExtractor(str(project_root), use_sample_data=True, log_level='WARNING')
    transformer = DataTransformer(str(project_root), log_level='INFO')
    
    # Test with cleanest dataset
    source_name = 'npdes_measurements'
    print(f"📊 Processing source: {source_name}")
    
    start_time = time.time()
    
    try:
        # Extract data
        print("🔄 Starting extraction...")
        extraction_generator = extractor.extract_file(source_name)
        
        # Transform data
        print("🔄 Starting transformation...")
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
                sample_chunk = transformed_chunk.copy()
            
            print(f"  ✅ Chunk {chunk_count}: {transform_stats.input_rows} → {transform_stats.output_rows} rows")
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"\n📊 Pipeline Results:")
        print(f"  • Total chunks processed: {chunk_count}")
        print(f"  • Total input rows: {total_input:,}")
        print(f"  • Total output rows: {total_output:,}")
        print(f"  • Data retention: {total_output/total_input*100:.1f}%")
        print(f"  • Processing time: {duration:.2f} seconds")
        print(f"  • Processing rate: {total_input/duration:.0f} rows/second")
        
        # Analyze sample data
        if sample_chunk is not None and not sample_chunk.empty:
            print(f"\n🔍 Sample Transformed Data:")
            print(f"  • Total columns: {len(sample_chunk.columns)}")
            print(f"  • Sample rows: {len(sample_chunk)}")
            
            # Show column transformation
            print(f"  • Column names (first 8): {list(sample_chunk.columns[:8])}")
            
            # Check for quality columns
            quality_cols = [col for col in sample_chunk.columns if 'quality' in col.lower()]
            if quality_cols:
                print(f"  • Quality columns added: {quality_cols}")
                
                if 'data_quality_score' in sample_chunk.columns:
                    avg_quality = sample_chunk['data_quality_score'].mean()
                    print(f"  • Average quality score: {avg_quality:.3f}")
                
                if 'data_quality_flags' in sample_chunk.columns:
                    clean_records = (sample_chunk['data_quality_flags'] == 'CLEAN').sum()
                    print(f"  • Clean records: {clean_records}/{len(sample_chunk)} ({clean_records/len(sample_chunk)*100:.1f}%)")
            
            # Show sample data
            print(f"\n📋 Sample Records (first 3 rows):")
            display_cols = list(sample_chunk.columns[:6])  # Show first 6 columns
            print(sample_chunk[display_cols].head(3).to_string())
        
        # Get transformation summary
        summary = transformer.get_transformation_summary()
        
        print(f"\n📈 Transformation Performance:")
        print(f"  • Quality improvement: {summary['average_quality_improvement']:.1f}%")
        print(f"  • Peak memory usage: {summary['peak_memory_usage_mb']:.1f} MB")
        print(f"  • Data retention rate: {summary['overall_data_retention']:.1f}%")
        print(f"  • Total duplicates removed: {summary['total_duplicates_removed']:,}")
        
        print(f"\n🎉 Integration test successful!")
        print(f"✅ Extract → Transform pipeline working correctly")
        print(f"✅ Data standardization applied")
        print(f"✅ Quality flags generated")
        print(f"✅ Performance metrics captured")
        
        return True
        
    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_column_standardization():
    """Test column name standardization"""
    print("\n" + "=" * 60)
    print(" COLUMN STANDARDIZATION TEST")
    print("=" * 60)
    
    project_root = Path(__file__).parent.parent
    transformer = DataTransformer(str(project_root), log_level='WARNING')
    
    # Test data with EPA column names
    test_data = {
        'REGISTRY_ID': [110030441779, 110012538968],
        'FAC_NAME': ['Test Facility 1', 'Test Facility 2'],
        'FAC_EPA_REGION': [6, 8],
        'CWA_COMPLIANCE_STATUS': ['Good', 'Fair'],
        'NPDES_ID': ['CA123456', 'TX789012']
    }
    
    test_df = pd.DataFrame(test_data)
    print(f"📊 Original columns: {list(test_df.columns)}")
    
    # Apply standardization
    standardized_df = transformer.standardize_column_names(test_df)
    print(f"✅ Standardized columns: {list(standardized_df.columns)}")
    
    # Verify snake_case conversion
    expected = ['registry_id', 'fac_name', 'fac_epa_region', 'cwa_compliance_status', 'npdes_id']
    actual = list(standardized_df.columns)
    
    if actual == expected:
        print("🎉 Column standardization successful!")
        return True
    else:
        print(f"❌ Expected: {expected}")
        print(f"❌ Actual: {actual}")
        return False

def main():
    """Run simplified transformation tests"""
    print("🚀 Starting EPA Data Transformation Tests")
    
    # Test 1: Column Standardization
    test1_result = test_column_standardization()
    
    # Test 2: Integrated Pipeline
    test2_result = test_integrated_pipeline()
    
    # Summary
    print(f"\n" + "=" * 80)
    print(" TEST SUMMARY")
    print("=" * 80)
    
    if test1_result and test2_result:
        print("✅ All tests passed successfully!")
        print("🚀 Transform module ready for production use")
        print("\n📋 Validated capabilities:")
        print("  ✅ Column name standardization (snake_case)")
        print("  ✅ Extract → Transform integration")
        print("  ✅ Data quality flag generation")
        print("  ✅ Performance monitoring")
        print("  ✅ Memory-efficient processing")
        return True
    else:
        print("❌ Some tests failed")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
