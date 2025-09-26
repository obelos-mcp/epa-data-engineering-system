#!/usr/bin/env python3
"""
Environmental Site Data Management & QA/QC Automation System
Data Exploration Script

This script explores EPA environmental datasets to understand structure,
data quality, and prepare sample files for development.
"""

import pandas as pd
import numpy as np
import os
import sys
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

class DataExplorer:
    """Class to explore and analyze EPA environmental datasets"""
    
    def __init__(self, project_root):
        self.project_root = Path(project_root)
        self.data_dir = self.project_root
        self.samples_dir = self.project_root / "data" / "samples"
        
        # Key CSV files to analyze
        self.csv_files = {
            'ECHO_EXPORTER': 'ECHO_EXPORTER.csv',
            'ICIS_FACILITIES': 'ICIS_FACILITIES.csv', 
            'ICIS_PERMITS': 'ICIS_PERMITS.csv',
            'NPDES_QNCR_HISTORY': 'NPDES_QNCR_HISTORY.csv',
            'POLL_RPT_COMBINED_EMISSIONS': 'POLL_RPT_COMBINED_EMISSIONS.csv'
        }
        
        self.exploration_results = {}
        
    def get_file_size(self, filepath):
        """Get file size in MB"""
        try:
            size_bytes = os.path.getsize(filepath)
            size_mb = size_bytes / (1024 * 1024)
            return f"{size_mb:.2f} MB"
        except:
            return "File not found"
    
    def explore_csv(self, filename, sample_size=1000):
        """Explore a single CSV file"""
        filepath = self.data_dir / filename
        
        if not filepath.exists():
            return {
                'filename': filename,
                'status': 'File not found',
                'file_size': '0 MB'
            }
        
        try:
            print(f"\n📊 Analyzing {filename}...")
            
            # Read sample data
            df_sample = pd.read_csv(filepath, nrows=sample_size, low_memory=False)
            
            # Get file size
            file_size = self.get_file_size(filepath)
            
            # Basic info
            total_columns = len(df_sample.columns)
            sample_rows = len(df_sample)
            
            # Data types analysis
            dtype_counts = df_sample.dtypes.value_counts().to_dict()
            
            # Missing values analysis
            missing_counts = df_sample.isnull().sum()
            high_missing_cols = missing_counts[missing_counts > sample_rows * 0.5].index.tolist()
            
            # Numeric columns statistics
            numeric_cols = df_sample.select_dtypes(include=[np.number]).columns.tolist()
            
            # Text columns with potential data quality issues
            text_cols = df_sample.select_dtypes(include=['object']).columns.tolist()
            
            # Sample of column names (first 10)
            sample_columns = df_sample.columns[:10].tolist()
            
            result = {
                'filename': filename,
                'status': 'Success',
                'file_size': file_size,
                'sample_rows': sample_rows,
                'total_columns': total_columns,
                'sample_columns': sample_columns,
                'data_types': dtype_counts,
                'numeric_columns_count': len(numeric_cols),
                'text_columns_count': len(text_cols),
                'high_missing_columns': high_missing_cols,
                'missing_data_percentage': (missing_counts.sum() / (sample_rows * total_columns)) * 100
            }
            
            return result
            
        except Exception as e:
            return {
                'filename': filename,
                'status': f'Error: {str(e)}',
                'file_size': self.get_file_size(filepath)
            }
    
    def create_sample_file(self, filename, sample_size=10000):
        """Create sample file with specified number of rows"""
        filepath = self.data_dir / filename
        sample_filepath = self.samples_dir / f"sample_{filename}"
        
        if not filepath.exists():
            print(f"❌ Source file {filename} not found")
            return False
            
        try:
            print(f"📝 Creating sample file for {filename}...")
            
            # Read sample data
            df_sample = pd.read_csv(filepath, nrows=sample_size, low_memory=False)
            
            # Save sample
            df_sample.to_csv(sample_filepath, index=False)
            
            sample_size_mb = self.get_file_size(sample_filepath)
            print(f"✅ Created {sample_filepath.name} ({sample_size_mb})")
            
            return True
            
        except Exception as e:
            print(f"❌ Error creating sample for {filename}: {str(e)}")
            return False
    
    def run_exploration(self):
        """Run complete data exploration"""
        print("🔍 Starting EPA Environmental Data Exploration")
        print("=" * 60)
        
        # Explore each CSV file
        for key, filename in self.csv_files.items():
            result = self.explore_csv(filename)
            self.exploration_results[key] = result
            
            # Print summary
            if result['status'] == 'Success':
                print(f"✅ {filename}")
                print(f"   Size: {result['file_size']}")
                print(f"   Columns: {result['total_columns']}")
                print(f"   Sample columns: {', '.join(result['sample_columns'])}")
                print(f"   Missing data: {result['missing_data_percentage']:.1f}%")
            else:
                print(f"❌ {filename}: {result['status']}")
        
        # Create sample files
        print(f"\n📁 Creating sample files...")
        for key, filename in self.csv_files.items():
            if self.exploration_results[key]['status'] == 'Success':
                self.create_sample_file(filename)
        
        return self.exploration_results
    
    def generate_summary_report(self):
        """Generate comprehensive data summary report"""
        print("\n📋 DATA SUMMARY REPORT")
        print("=" * 60)
        
        total_files = len(self.csv_files)
        successful_files = sum(1 for r in self.exploration_results.values() if r['status'] == 'Success')
        
        print(f"📊 OVERVIEW")
        print(f"   Total files analyzed: {total_files}")
        print(f"   Successfully processed: {successful_files}")
        print(f"   Failed: {total_files - successful_files}")
        
        print(f"\n📁 FILE DETAILS")
        for key, result in self.exploration_results.items():
            if result['status'] == 'Success':
                print(f"\n   {result['filename']}:")
                print(f"     • File size: {result['file_size']}")
                print(f"     • Columns: {result['total_columns']}")
                print(f"     • Numeric columns: {result['numeric_columns_count']}")
                print(f"     • Text columns: {result['text_columns_count']}")
                print(f"     • Missing data: {result['missing_data_percentage']:.1f}%")
                
                if result['high_missing_columns']:
                    print(f"     • High missing columns: {len(result['high_missing_columns'])}")
            else:
                print(f"\n   {result['filename']}: {result['status']}")
        
        print(f"\n🔍 DATA QUALITY INSIGHTS")
        
        # Aggregate insights
        total_columns = sum(r['total_columns'] for r in self.exploration_results.values() if r['status'] == 'Success')
        avg_missing = np.mean([r['missing_data_percentage'] for r in self.exploration_results.values() if r['status'] == 'Success'])
        
        print(f"   • Total columns across all files: {total_columns}")
        print(f"   • Average missing data percentage: {avg_missing:.1f}%")
        
        # Identify potential key relationships
        print(f"\n🔗 POTENTIAL KEY RELATIONSHIPS")
        print(f"   • ECHO_EXPORTER likely contains facility master data")
        print(f"   • ICIS_FACILITIES and ICIS_PERMITS are related (water discharge)")
        print(f"   • NPDES_QNCR_HISTORY contains water quality measurements")
        print(f"   • POLL_RPT_COMBINED_EMISSIONS contains air quality data")
        
        print(f"\n✅ Sample files created in: {self.samples_dir}")

def main():
    """Main execution function"""
    # Get project root directory
    project_root = Path(__file__).parent.parent
    
    # Initialize explorer
    explorer = DataExplorer(project_root)
    
    # Run exploration
    results = explorer.run_exploration()
    
    # Generate summary report
    explorer.generate_summary_report()
    
    print(f"\n🎯 NEXT STEPS:")
    print(f"   1. Review sample files in data/samples/")
    print(f"   2. Design database schema based on column analysis")
    print(f"   3. Implement data validation and cleaning rules")
    print(f"   4. Create ETL pipeline for data ingestion")

if __name__ == "__main__":
    main()
