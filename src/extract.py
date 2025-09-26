"""
Environmental Site Data Management & QA/QC Automation System
Extract Module - Memory-Efficient CSV Processing

This module handles extraction of large EPA environmental datasets with:
- Memory-efficient chunked processing
- Comprehensive data validation
- Progress tracking and performance monitoring
- Error handling and recovery
- Integration with ETL audit trail
"""

import pandas as pd
import numpy as np
import logging
import time
import gc
import psutil
import re
from pathlib import Path
from typing import Dict, List, Optional, Iterator, Tuple, Any, Union
from dataclasses import dataclass, field
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)

# Import our configuration
import sys
sys.path.append(str(Path(__file__).parent.parent))
from config.data_sources import get_data_sources, DataSourceConfig, PERFORMANCE_CONFIG

@dataclass
class ExtractionStats:
    """Statistics for extraction process"""
    source_name: str
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    total_rows_processed: int = 0
    total_rows_extracted: int = 0
    total_chunks: int = 0
    chunks_processed: int = 0
    error_count: int = 0
    warning_count: int = 0
    bytes_processed: int = 0
    peak_memory_mb: float = 0.0
    processing_rate_rows_sec: float = 0.0
    processing_rate_mb_sec: float = 0.0
    validation_errors: Dict[str, int] = field(default_factory=dict)
    
    def calculate_rates(self):
        """Calculate processing rates"""
        if self.end_time and self.start_time:
            duration_seconds = (self.end_time - self.start_time).total_seconds()
            if duration_seconds > 0:
                self.processing_rate_rows_sec = self.total_rows_processed / duration_seconds
                self.processing_rate_mb_sec = (self.bytes_processed / (1024 * 1024)) / duration_seconds

@dataclass
class ValidationResult:
    """Result of data validation"""
    is_valid: bool
    error_count: int = 0
    warning_count: int = 0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    corrupt_rows: List[int] = field(default_factory=list)
    missing_required_columns: List[str] = field(default_factory=list)

class DataExtractor:
    """Main class for extracting EPA environmental data"""
    
    def __init__(self, project_root: str, use_sample_data: bool = True, log_level: str = 'INFO'):
        self.project_root = Path(project_root)
        self.use_sample_data = use_sample_data
        self.data_sources = get_data_sources(project_root)
        
        # Setup logging
        self._setup_logging(log_level)
        
        # Performance monitoring
        self.process = psutil.Process()
        self.extraction_stats: Dict[str, ExtractionStats] = {}
        
        # Checkpoint management
        self.checkpoint_dir = self.project_root / "checkpoints"
        self.checkpoint_dir.mkdir(exist_ok=True)
        
        self.logger.info(f"DataExtractor initialized - Sample mode: {use_sample_data}")
    
    def _setup_logging(self, log_level: str):
        """Setup comprehensive logging"""
        log_dir = self.project_root / "logs"
        log_dir.mkdir(exist_ok=True)
        
        # Create logger
        self.logger = logging.getLogger('data_extractor')
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # File handler
        log_file = log_dir / f"extraction_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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
    
    def validate_file_structure(self, config: DataSourceConfig) -> ValidationResult:
        """Validate file structure before processing"""
        file_path = config.sample_file_path if self.use_sample_data else config.file_path
        
        self.logger.info(f"Validating file structure: {file_path}")
        
        result = ValidationResult(is_valid=True)
        
        # Check file exists
        if not Path(file_path).exists():
            result.is_valid = False
            result.errors.append(f"File not found: {file_path}")
            return result
        
        try:
            # Read just the header and first few rows
            sample_df = pd.read_csv(file_path, nrows=5, encoding=config.encoding)
            
            # Check required columns
            missing_columns = set(config.required_columns) - set(sample_df.columns)
            if missing_columns:
                result.is_valid = False
                result.missing_required_columns = list(missing_columns)
                result.errors.append(f"Missing required columns: {missing_columns}")
            
            # Check expected columns
            missing_expected = set(config.expected_columns) - set(sample_df.columns)
            if missing_expected:
                result.warnings.append(f"Missing expected columns: {missing_expected}")
                result.warning_count += len(missing_expected)
            
            # Check for extra columns
            extra_columns = set(sample_df.columns) - set(config.expected_columns)
            if extra_columns:
                result.warnings.append(f"Extra columns found: {extra_columns}")
                result.warning_count += 1
            
            # Basic data type validation
            for col in config.numeric_columns:
                if col in sample_df.columns:
                    try:
                        pd.to_numeric(sample_df[col], errors='coerce')
                    except Exception as e:
                        result.warnings.append(f"Potential numeric conversion issue in {col}: {e}")
                        result.warning_count += 1
            
            # Date column validation
            for col in config.date_columns:
                if col in sample_df.columns:
                    try:
                        pd.to_datetime(sample_df[col], errors='coerce')
                    except Exception as e:
                        result.warnings.append(f"Potential date conversion issue in {col}: {e}")
                        result.warning_count += 1
            
            self.logger.info(f"File validation complete - Errors: {result.error_count}, Warnings: {result.warning_count}")
            
        except Exception as e:
            result.is_valid = False
            result.errors.append(f"File validation error: {str(e)}")
            self.logger.error(f"File validation failed: {e}")
        
        return result
    
    def validate_chunk_data(self, chunk: pd.DataFrame, config: DataSourceConfig, chunk_num: int) -> ValidationResult:
        """Validate data within a chunk"""
        result = ValidationResult(is_valid=True)
        
        # Check for empty chunk
        if chunk.empty:
            result.warnings.append(f"Chunk {chunk_num} is empty")
            result.warning_count += 1
            return result
        
        # Coordinate validation
        for coord_col in config.coordinate_columns:
            if coord_col in chunk.columns:
                coord_data = pd.to_numeric(chunk[coord_col], errors='coerce')
                
                if 'lat' in coord_col.lower():
                    invalid_coords = (coord_data < -90) | (coord_data > 90)
                elif 'lon' in coord_col.lower() or 'long' in coord_col.lower():
                    invalid_coords = (coord_data < -180) | (coord_data > 180)
                else:
                    continue
                
                invalid_count = invalid_coords.sum()
                if invalid_count > 0:
                    result.warnings.append(f"Invalid coordinates in {coord_col}: {invalid_count} records")
                    result.warning_count += invalid_count
        
        # Business rule validation
        if 'validation_rules' in config.__dict__ and config.validation_rules:
            self._validate_business_rules(chunk, config, result)
        
        # Check for obviously corrupt rows (all null or malformed)
        null_counts = chunk.isnull().sum(axis=1)
        corrupt_threshold = len(chunk.columns) * 0.8  # 80% null values
        corrupt_rows = chunk.index[null_counts >= corrupt_threshold].tolist()
        
        if corrupt_rows:
            result.corrupt_rows = corrupt_rows
            result.warning_count += len(corrupt_rows)
            result.warnings.append(f"Found {len(corrupt_rows)} potentially corrupt rows")
        
        return result
    
    def _validate_business_rules(self, chunk: pd.DataFrame, config: DataSourceConfig, result: ValidationResult):
        """Apply business rule validations"""
        rules = config.validation_rules
        
        # EPA region validation
        if 'epa_region_range' in rules and 'FAC_EPA_REGION' in chunk.columns:
            region_col = pd.to_numeric(chunk['FAC_EPA_REGION'], errors='coerce')
            invalid_regions = ((region_col < rules['epa_region_range']['min']) | 
                             (region_col > rules['epa_region_range']['max'])).sum()
            if invalid_regions > 0:
                result.warnings.append(f"Invalid EPA regions: {invalid_regions} records")
                result.warning_count += invalid_regions
        
        # State code validation
        if 'state_codes' in rules and 'FAC_STATE' in chunk.columns:
            invalid_states = (~chunk['FAC_STATE'].isin(rules['state_codes'] + ['', None])).sum()
            if invalid_states > 0:
                result.warnings.append(f"Invalid state codes: {invalid_states} records")
                result.warning_count += invalid_states
        
        # Year range validation
        if 'year_range' in rules:
            year_columns = ['REPORTING_YEAR', 'YEARQTR']
            for col in year_columns:
                if col in chunk.columns:
                    if col == 'YEARQTR':
                        # Extract year from YEARQTR format (YYYYQ)
                        years = pd.to_numeric(chunk[col].astype(str).str[:4], errors='coerce')
                    else:
                        years = pd.to_numeric(chunk[col], errors='coerce')
                    
                    invalid_years = ((years < rules['year_range']['min']) | 
                                   (years > rules['year_range']['max'])).sum()
                    if invalid_years > 0:
                        result.warnings.append(f"Invalid years in {col}: {invalid_years} records")
                        result.warning_count += invalid_years
    
    def extract_file(self, source_name: str, resume_from_chunk: int = 0) -> Iterator[Tuple[pd.DataFrame, int, ExtractionStats]]:
        """Extract data from a single file with chunked processing"""
        config = self.data_sources.get_source(source_name)
        if not config:
            raise ValueError(f"Unknown source: {source_name}")
        
        # Initialize stats
        stats = ExtractionStats(source_name=source_name)
        self.extraction_stats[source_name] = stats
        
        # Validate file structure
        validation = self.validate_file_structure(config)
        if not validation.is_valid:
            self.logger.error(f"File validation failed for {source_name}: {validation.errors}")
            raise ValueError(f"File validation failed: {validation.errors}")
        
        if validation.warnings:
            self.logger.warning(f"File validation warnings for {source_name}: {validation.warnings}")
        
        file_path = config.sample_file_path if self.use_sample_data else config.file_path
        self.logger.info(f"Starting extraction: {source_name} from {file_path}")
        
        try:
            # Get total rows for progress tracking
            total_rows = self._count_file_rows(file_path)
            stats.total_rows_processed = total_rows
            
            # Calculate total chunks
            stats.total_chunks = (total_rows // config.chunk_size) + (1 if total_rows % config.chunk_size else 0)
            
            self.logger.info(f"File has {total_rows:,} rows, will process in {stats.total_chunks} chunks of {config.chunk_size:,}")
            
            # Setup chunk reader
            chunk_reader = pd.read_csv(
                file_path,
                chunksize=config.chunk_size,
                encoding=config.encoding,
                delimiter=config.delimiter,
                skiprows=range(1, resume_from_chunk * config.chunk_size + 1) if resume_from_chunk > 0 else None,
                low_memory=False
            )
            
            chunk_num = resume_from_chunk
            last_progress_time = time.time()
            
            for chunk in chunk_reader:
                chunk_start_time = time.time()
                
                # Memory monitoring
                current_memory = self.process.memory_info().rss / (1024 * 1024)  # MB
                stats.peak_memory_mb = max(stats.peak_memory_mb, current_memory)
                
                # Skip empty chunks
                if chunk.empty:
                    chunk_num += 1
                    continue
                
                # Clean chunk data
                chunk = self._clean_chunk(chunk, config)
                
                # Validate chunk
                chunk_validation = self.validate_chunk_data(chunk, config, chunk_num)
                if chunk_validation.error_count > 0:
                    stats.error_count += chunk_validation.error_count
                    self.logger.error(f"Chunk {chunk_num} validation errors: {chunk_validation.errors}")
                
                if chunk_validation.warning_count > 0:
                    stats.warning_count += chunk_validation.warning_count
                    if chunk_validation.warning_count > 100:  # Only log if many warnings
                        self.logger.warning(f"Chunk {chunk_num} has {chunk_validation.warning_count} validation warnings")
                
                # Update stats
                stats.chunks_processed += 1
                stats.total_rows_extracted += len(chunk)
                stats.bytes_processed += chunk.memory_usage(deep=True).sum()
                
                # Progress reporting
                current_time = time.time()
                if current_time - last_progress_time >= 2.0:  # Every 2 seconds
                    progress_pct = (stats.chunks_processed / stats.total_chunks) * 100
                    rows_per_sec = stats.total_rows_extracted / (current_time - stats.start_time.timestamp())
                    
                    self.logger.info(
                        f"{source_name}: {progress_pct:.1f}% complete "
                        f"({stats.chunks_processed}/{stats.total_chunks} chunks, "
                        f"{stats.total_rows_extracted:,} rows, "
                        f"{rows_per_sec:.0f} rows/sec, "
                        f"{current_memory:.1f}MB memory)"
                    )
                    last_progress_time = current_time
                
                # Yield processed chunk
                yield chunk, chunk_num, stats
                
                # Checkpoint saving
                if chunk_num % PERFORMANCE_CONFIG.get('gc_frequency', 20) == 0:
                    self._save_checkpoint(source_name, chunk_num, stats)
                    gc.collect()  # Force garbage collection
                
                chunk_num += 1
                
                # Memory pressure check
                if current_memory > PERFORMANCE_CONFIG.get('max_memory_mb', 2048):
                    self.logger.warning(f"High memory usage: {current_memory:.1f}MB, forcing garbage collection")
                    gc.collect()
            
            # Finalize stats
            stats.end_time = datetime.now()
            stats.calculate_rates()
            
            self.logger.info(
                f"Extraction complete for {source_name}: "
                f"{stats.total_rows_extracted:,} rows processed in "
                f"{(stats.end_time - stats.start_time).total_seconds():.1f}s "
                f"({stats.processing_rate_rows_sec:.0f} rows/sec, "
                f"{stats.processing_rate_mb_sec:.1f} MB/sec)"
            )
            
        except Exception as e:
            self.logger.error(f"Extraction failed for {source_name}: {e}")
            stats.error_count += 1
            raise
    
    def _clean_chunk(self, chunk: pd.DataFrame, config: DataSourceConfig) -> pd.DataFrame:
        """Clean and preprocess chunk data"""
        # Remove completely empty rows
        chunk = chunk.dropna(how='all')
        
        # Strip whitespace from string columns
        string_columns = chunk.select_dtypes(include=['object']).columns
        for col in string_columns:
            chunk[col] = chunk[col].astype(str).str.strip()
            # Replace 'nan' strings with actual NaN
            chunk[col] = chunk[col].replace(['nan', 'NaN', 'NULL', ''], np.nan)
        
        # Convert numeric columns
        for col in config.numeric_columns:
            if col in chunk.columns:
                chunk[col] = pd.to_numeric(chunk[col], errors='coerce')
        
        # Convert date columns
        for col in config.date_columns:
            if col in chunk.columns:
                chunk[col] = pd.to_datetime(chunk[col], errors='coerce')
        
        return chunk
    
    def _count_file_rows(self, file_path: str) -> int:
        """Efficiently count rows in a file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                row_count = sum(1 for _ in f) - 1  # Subtract header row
            return max(0, row_count)
        except Exception:
            # Fallback to pandas if direct counting fails
            try:
                return len(pd.read_csv(file_path, usecols=[0]))
            except Exception:
                self.logger.warning(f"Could not count rows in {file_path}, using chunk size estimate")
                return 100000  # Conservative estimate
    
    def _save_checkpoint(self, source_name: str, chunk_num: int, stats: ExtractionStats):
        """Save extraction checkpoint"""
        checkpoint_file = self.checkpoint_dir / f"{source_name}_checkpoint.json"
        
        checkpoint_data = {
            'source_name': source_name,
            'last_chunk': chunk_num,
            'timestamp': datetime.now().isoformat(),
            'rows_processed': stats.total_rows_extracted,
            'chunks_processed': stats.chunks_processed,
            'error_count': stats.error_count
        }
        
        try:
            import json
            with open(checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)
        except Exception as e:
            self.logger.warning(f"Could not save checkpoint: {e}")
    
    def load_checkpoint(self, source_name: str) -> Optional[Dict]:
        """Load extraction checkpoint"""
        checkpoint_file = self.checkpoint_dir / f"{source_name}_checkpoint.json"
        
        if not checkpoint_file.exists():
            return None
        
        try:
            import json
            with open(checkpoint_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            self.logger.warning(f"Could not load checkpoint: {e}")
            return None
    
    def extract_all_sources(self, sources: Optional[List[str]] = None, parallel: bool = False) -> Dict[str, ExtractionStats]:
        """Extract data from multiple sources"""
        if sources is None:
            sources = self.data_sources.get_processing_order()
        
        self.logger.info(f"Starting extraction for sources: {sources}")
        
        if parallel and len(sources) > 1:
            return self._extract_parallel(sources)
        else:
            return self._extract_sequential(sources)
    
    def _extract_sequential(self, sources: List[str]) -> Dict[str, ExtractionStats]:
        """Extract sources sequentially"""
        all_stats = {}
        
        for source_name in sources:
            self.logger.info(f"Processing source: {source_name}")
            
            try:
                # Check for checkpoint
                checkpoint = self.load_checkpoint(source_name)
                resume_chunk = checkpoint['last_chunk'] if checkpoint else 0
                
                if resume_chunk > 0:
                    self.logger.info(f"Resuming from chunk {resume_chunk}")
                
                # Extract data (consume generator to complete extraction)
                for chunk, chunk_num, stats in self.extract_file(source_name, resume_chunk):
                    # In a real ETL pipeline, this is where you'd pass the chunk
                    # to the Transform module or load it into staging tables
                    pass
                
                all_stats[source_name] = self.extraction_stats[source_name]
                
            except Exception as e:
                self.logger.error(f"Failed to extract {source_name}: {e}")
                # Continue with next source
                continue
        
        return all_stats
    
    def _extract_parallel(self, sources: List[str]) -> Dict[str, ExtractionStats]:
        """Extract sources in parallel (for smaller files)"""
        all_stats = {}
        
        # Only parallelize smaller files to avoid memory issues
        small_sources = [s for s in sources if s != 'echo_facilities']  # ECHO is too large
        large_sources = [s for s in sources if s == 'echo_facilities']
        
        # Process small sources in parallel
        if small_sources:
            with ThreadPoolExecutor(max_workers=min(len(small_sources), 3)) as executor:
                future_to_source = {
                    executor.submit(self._extract_single_source, source): source 
                    for source in small_sources
                }
                
                for future in as_completed(future_to_source):
                    source = future_to_source[future]
                    try:
                        stats = future.result()
                        all_stats[source] = stats
                    except Exception as e:
                        self.logger.error(f"Parallel extraction failed for {source}: {e}")
        
        # Process large sources sequentially
        for source in large_sources:
            try:
                stats = self._extract_single_source(source)
                all_stats[source] = stats
            except Exception as e:
                self.logger.error(f"Large file extraction failed for {source}: {e}")
        
        return all_stats
    
    def _extract_single_source(self, source_name: str) -> ExtractionStats:
        """Extract a single source (helper for parallel processing)"""
        checkpoint = self.load_checkpoint(source_name)
        resume_chunk = checkpoint['last_chunk'] if checkpoint else 0
        
        # Consume the generator
        for chunk, chunk_num, stats in self.extract_file(source_name, resume_chunk):
            pass
        
        return self.extraction_stats[source_name]
    
    def get_extraction_summary(self) -> Dict[str, Any]:
        """Get comprehensive extraction summary"""
        total_rows = sum(stats.total_rows_extracted for stats in self.extraction_stats.values())
        total_errors = sum(stats.error_count for stats in self.extraction_stats.values())
        total_warnings = sum(stats.warning_count for stats in self.extraction_stats.values())
        
        return {
            'total_sources': len(self.extraction_stats),
            'total_rows_extracted': total_rows,
            'total_errors': total_errors,
            'total_warnings': total_warnings,
            'peak_memory_mb': max(stats.peak_memory_mb for stats in self.extraction_stats.values()),
            'average_processing_rate': sum(stats.processing_rate_rows_sec for stats in self.extraction_stats.values()) / len(self.extraction_stats),
            'source_details': {
                name: {
                    'rows': stats.total_rows_extracted,
                    'chunks': stats.chunks_processed,
                    'errors': stats.error_count,
                    'warnings': stats.warning_count,
                    'rate_rows_sec': stats.processing_rate_rows_sec,
                    'duration_seconds': (stats.end_time - stats.start_time).total_seconds() if stats.end_time else 0
                }
                for name, stats in self.extraction_stats.items()
            }
        }
