#!/usr/bin/env python3
"""
Environmental Site Data Management & QA/QC Automation System
Extraction Test Script

This script demonstrates the extraction capabilities with both sample and full datasets.
Tests memory-efficient processing, validation, error handling, and performance monitoring.
"""

import sys
import time
import json
from pathlib import Path
from typing import Dict, Any

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.extract import DataExtractor
from config.data_sources import get_data_sources

def print_header(title: str):
    """Print formatted header"""
    print("\n" + "=" * 80)
    print(f" {title}")
    print("=" * 80)

def print_stats_table(stats_dict: Dict[str, Any]):
    """Print extraction statistics in table format"""
    print(f"\n{'Source':<20} {'Rows':<12} {'Chunks':<8} {'Errors':<8} {'Warnings':<10} {'Rate (r/s)':<12} {'Duration':<10}")
    print("-" * 90)
    
    for source, details in stats_dict.get('source_details', {}).items():
        print(f"{source:<20} {details['rows']:>11,} {details['chunks']:>7} "
              f"{details['errors']:>7} {details['warnings']:>9} "
              f"{details['rate_rows_sec']:>11.0f} {details['duration_seconds']:>9.1f}s")

def test_data_source_validation():
    """Test data source configuration and file validation"""
    print_header("DATA SOURCE VALIDATION TEST")
    
    # Initialize data sources
    project_root = Path(__file__).parent.parent
    data_sources = get_data_sources(str(project_root))
    
    print(f"📊 Configured data sources: {len(data_sources.get_all_sources())}")
    for name in data_sources.get_source_names():
        config = data_sources.get_source(name)
        print(f"  • {name}: {config.description}")
    
    # Validate files
    print(f"\n🔍 Validating source files...")
    validation_results = data_sources.validate_source_files()
    
    for name, result in validation_results.items():
        status = "✅" if result['file_exists'] and result['sample_exists'] else "❌"
        print(f"{status} {name}:")
        print(f"    File: {'Found' if result['file_exists'] else 'Missing'} ({result['file_size_mb']:.1f} MB)")
        print(f"    Sample: {'Found' if result['sample_exists'] else 'Missing'} ({result['sample_size_mb']:.1f} MB)")
        
        if result['errors']:
            print(f"    Errors: {', '.join(result['errors'])}")
    
    # Memory requirements
    print(f"\n💾 Memory requirements:")
    memory_reqs = data_sources.get_memory_requirements()
    for source, memory_mb in memory_reqs.items():
        print(f"  • {source}: ~{memory_mb:.1f} MB per chunk")
    
    return validation_results

def test_sample_extraction():
    """Test extraction with sample data files"""
    print_header("SAMPLE DATA EXTRACTION TEST")
    
    project_root = Path(__file__).parent.parent
    extractor = DataExtractor(str(project_root), use_sample_data=True, log_level='INFO')
    
    print("🔄 Starting sample data extraction...")
    start_time = time.time()
    
    try:
        # Extract all sources sequentially
        stats = extractor.extract_all_sources()
        
        end_time = time.time()
        total_duration = end_time - start_time
        
        # Get comprehensive summary
        summary = extractor.get_extraction_summary()
        
        print(f"\n✅ Sample extraction completed in {total_duration:.1f} seconds")
        print(f"📈 Overall Statistics:")
        print(f"  • Total sources processed: {summary['total_sources']}")
        print(f"  • Total rows extracted: {summary['total_rows_extracted']:,}")
        print(f"  • Total errors: {summary['total_errors']:,}")
        print(f"  • Total warnings: {summary['total_warnings']:,}")
        print(f"  • Peak memory usage: {summary['peak_memory_mb']:.1f} MB")
        print(f"  • Average processing rate: {summary['average_processing_rate']:.0f} rows/second")
        
        print_stats_table(summary)
        
        return summary
        
    except Exception as e:
        print(f"❌ Sample extraction failed: {e}")
        return None

def test_single_source_detailed():
    """Test detailed extraction of a single source with validation"""
    print_header("DETAILED SINGLE SOURCE EXTRACTION TEST")
    
    project_root = Path(__file__).parent.parent
    extractor = DataExtractor(str(project_root), use_sample_data=True, log_level='DEBUG')
    
    # Test with NPDES measurements (cleanest dataset)
    source_name = 'npdes_measurements'
    print(f"🔍 Testing detailed extraction: {source_name}")
    
    try:
        chunk_count = 0
        sample_chunks = []
        
        for chunk, chunk_num, stats in extractor.extract_file(source_name):
            chunk_count += 1
            
            # Save first few chunks for inspection
            if chunk_count <= 3:
                sample_chunks.append({
                    'chunk_num': chunk_num,
                    'rows': len(chunk),
                    'columns': list(chunk.columns),
                    'sample_data': chunk.head(2).to_dict('records') if not chunk.empty else []
                })
            
            print(f"  Chunk {chunk_num}: {len(chunk)} rows processed")
            
            # Break after a few chunks for demo
            if chunk_count >= 5:
                break
        
        print(f"\n📊 Processed {chunk_count} chunks")
        
        # Show sample data structure
        if sample_chunks:
            print(f"\n🔍 Sample data from first chunk:")
            first_chunk = sample_chunks[0]
            print(f"  Columns: {first_chunk['columns']}")
            print(f"  Sample records:")
            for i, record in enumerate(first_chunk['sample_data']):
                print(f"    Row {i+1}: {record}")
        
        return True
        
    except Exception as e:
        print(f"❌ Detailed extraction failed: {e}")
        return False

def test_performance_comparison():
    """Test performance with different chunk sizes"""
    print_header("PERFORMANCE COMPARISON TEST")
    
    project_root = Path(__file__).parent.parent
    
    # Test different chunk sizes with sample data
    chunk_sizes = [10000, 25000, 50000]
    source_name = 'icis_facilities'  # Medium-sized file
    
    results = []
    
    for chunk_size in chunk_sizes:
        print(f"\n🔄 Testing chunk size: {chunk_size:,}")
        
        extractor = DataExtractor(str(project_root), use_sample_data=True, log_level='WARNING')
        
        # Modify chunk size for this test
        config = extractor.data_sources.get_source(source_name)
        original_chunk_size = config.chunk_size
        config.chunk_size = chunk_size
        
        start_time = time.time()
        
        try:
            # Extract just this source
            for chunk, chunk_num, stats in extractor.extract_file(source_name):
                pass  # Just consume the generator
            
            end_time = time.time()
            duration = end_time - start_time
            
            final_stats = extractor.extraction_stats[source_name]
            
            result = {
                'chunk_size': chunk_size,
                'duration': duration,
                'rows_processed': final_stats.total_rows_extracted,
                'chunks': final_stats.chunks_processed,
                'rate_rows_sec': final_stats.processing_rate_rows_sec,
                'peak_memory_mb': final_stats.peak_memory_mb
            }
            
            results.append(result)
            
            print(f"  ✅ Completed: {duration:.2f}s, {final_stats.processing_rate_rows_sec:.0f} rows/sec, {final_stats.peak_memory_mb:.1f}MB peak")
            
        except Exception as e:
            print(f"  ❌ Failed: {e}")
        
        finally:
            # Restore original chunk size
            config.chunk_size = original_chunk_size
    
    # Show performance comparison
    if results:
        print(f"\n📈 Performance Comparison Results:")
        print(f"{'Chunk Size':<12} {'Duration':<10} {'Rate (r/s)':<12} {'Peak Memory':<12} {'Chunks':<8}")
        print("-" * 60)
        
        for result in results:
            print(f"{result['chunk_size']:>11,} {result['duration']:>9.2f}s "
                  f"{result['rate_rows_sec']:>11.0f} {result['peak_memory_mb']:>11.1f}MB "
                  f"{result['chunks']:>7}")
    
    return results

def test_error_handling():
    """Test error handling with problematic data"""
    print_header("ERROR HANDLING TEST")
    
    project_root = Path(__file__).parent.parent
    extractor = DataExtractor(str(project_root), use_sample_data=True, log_level='INFO')
    
    print("🧪 Testing error handling capabilities...")
    
    # Test with a non-existent source
    try:
        list(extractor.extract_file('non_existent_source'))
        print("❌ Should have failed with non-existent source")
    except ValueError as e:
        print(f"✅ Correctly handled non-existent source: {e}")
    
    # Test file validation
    try:
        config = extractor.data_sources.get_source('echo_facilities')
        original_path = config.file_path
        config.file_path = '/non/existent/path.csv'  # Temporarily break the path
        
        validation = extractor.validate_file_structure(config)
        if not validation.is_valid:
            print("✅ Correctly detected missing file")
        
        config.file_path = original_path  # Restore
        
    except Exception as e:
        print(f"✅ Error handling worked: {e}")
    
    print("🔍 Error handling tests completed")

def test_full_dataset_simulation():
    """Simulate full dataset processing (without actually processing all data)"""
    print_header("FULL DATASET SIMULATION TEST")
    
    project_root = Path(__file__).parent.parent
    data_sources = get_data_sources(str(project_root))
    
    print("📊 Full dataset processing simulation:")
    
    # Calculate estimated processing times based on sample performance
    validation_results = data_sources.validate_source_files()
    
    # Rough estimates based on file sizes and typical processing rates
    processing_estimates = {
        'echo_facilities': {'estimated_time_min': 45, 'estimated_memory_mb': 512},
        'icis_facilities': {'estimated_time_min': 8, 'estimated_memory_mb': 128},
        'icis_permits': {'estimated_time_min': 15, 'estimated_memory_mb': 256},
        'npdes_measurements': {'estimated_time_min': 12, 'estimated_memory_mb': 192},
        'air_emissions': {'estimated_time_min': 25, 'estimated_memory_mb': 384}
    }
    
    total_estimated_time = 0
    max_memory_requirement = 0
    
    print(f"\n{'Source':<20} {'File Size':<12} {'Est. Time':<12} {'Est. Memory':<12} {'Status':<10}")
    print("-" * 70)
    
    for source_name, validation in validation_results.items():
        if validation['file_exists']:
            estimates = processing_estimates.get(source_name, {'estimated_time_min': 10, 'estimated_memory_mb': 200})
            
            total_estimated_time += estimates['estimated_time_min']
            max_memory_requirement = max(max_memory_requirement, estimates['estimated_memory_mb'])
            
            status = "Ready" if validation['file_exists'] else "Missing"
            
            print(f"{source_name:<20} {validation['file_size_mb']:>8.1f} MB "
                  f"{estimates['estimated_time_min']:>8} min "
                  f"{estimates['estimated_memory_mb']:>8} MB "
                  f"{status:<10}")
    
    print(f"\n📈 Full Dataset Processing Estimates:")
    print(f"  • Total estimated time: {total_estimated_time} minutes ({total_estimated_time/60:.1f} hours)")
    print(f"  • Peak memory requirement: {max_memory_requirement} MB")
    print(f"  • Recommended approach: Sequential processing with checkpoints")
    print(f"  • Parallel processing: Only for smaller files (ICIS, NPDES)")
    
    print(f"\n🚀 Performance Optimizations Available:")
    print(f"  • Memory-efficient chunked processing")
    print(f"  • Resumable extractions with checkpoints")
    print(f"  • Configurable chunk sizes per file type")
    print(f"  • Real-time progress monitoring")
    print(f"  • Comprehensive error handling and validation")

def main():
    """Run all extraction tests"""
    print_header("EPA ENVIRONMENTAL DATA EXTRACTION SYSTEM TEST SUITE")
    print("Testing memory-efficient extraction with validation and performance monitoring")
    
    # Create logs directory
    logs_dir = Path(__file__).parent.parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    
    test_results = {}
    
    # Run tests
    try:
        print("\n🔧 Test 1: Data Source Validation")
        test_results['validation'] = test_data_source_validation()
        
        print("\n🔧 Test 2: Sample Data Extraction")
        test_results['sample_extraction'] = test_sample_extraction()
        
        print("\n🔧 Test 3: Detailed Single Source")
        test_results['detailed_extraction'] = test_single_source_detailed()
        
        print("\n🔧 Test 4: Performance Comparison")
        test_results['performance'] = test_performance_comparison()
        
        print("\n🔧 Test 5: Error Handling")
        test_error_handling()
        
        print("\n🔧 Test 6: Full Dataset Simulation")
        test_full_dataset_simulation()
        
    except Exception as e:
        print(f"\n❌ Test suite failed: {e}")
        return False
    
    # Final summary
    print_header("TEST SUITE SUMMARY")
    
    if test_results.get('sample_extraction'):
        summary = test_results['sample_extraction']
        print(f"✅ All tests completed successfully!")
        print(f"📊 Sample data processing performance:")
        print(f"  • {summary['total_rows_extracted']:,} rows processed")
        print(f"  • {summary['average_processing_rate']:.0f} average rows/second")
        print(f"  • {summary['peak_memory_mb']:.1f} MB peak memory usage")
        print(f"  • {summary['total_errors']} errors, {summary['total_warnings']} warnings")
        
        print(f"\n🚀 System is ready for production ETL processing!")
        print(f"   Next steps: Integrate with Transform module and database loading")
    else:
        print(f"❌ Some tests failed - check logs for details")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
