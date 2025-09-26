"""
Environmental Site Data Management & QA/QC Automation System
Performance Monitor

This module provides comprehensive performance monitoring and metrics tracking
for the ETL pipeline, supporting the "37% faster processing" claim with
detailed benchmarking and optimization tracking.
"""

import time
import psutil
import threading
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import json
import numpy as np
import pandas as pd
from collections import defaultdict, deque

@dataclass
class PerformanceMetrics:
    """Comprehensive performance metrics for a process or stage"""
    process_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0
    
    # Processing metrics
    records_processed: int = 0
    bytes_processed: int = 0
    records_per_second: float = 0.0
    mb_per_second: float = 0.0
    
    # Resource metrics
    peak_memory_mb: float = 0.0
    average_memory_mb: float = 0.0
    peak_cpu_percent: float = 0.0
    average_cpu_percent: float = 0.0
    disk_io_read_mb: float = 0.0
    disk_io_write_mb: float = 0.0
    
    # Custom metrics
    custom_metrics: Dict[str, float] = field(default_factory=dict)
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def finalize(self):
        """Finalize metrics calculation"""
        if self.end_time and self.start_time:
            self.duration_seconds = (self.end_time - self.start_time).total_seconds()
            
            if self.duration_seconds > 0:
                self.records_per_second = self.records_processed / self.duration_seconds
                self.mb_per_second = (self.bytes_processed / (1024 * 1024)) / self.duration_seconds
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary for serialization"""
        return {
            'process_name': self.process_name,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': self.duration_seconds,
            'records_processed': self.records_processed,
            'bytes_processed': self.bytes_processed,
            'records_per_second': self.records_per_second,
            'mb_per_second': self.mb_per_second,
            'peak_memory_mb': self.peak_memory_mb,
            'average_memory_mb': self.average_memory_mb,
            'peak_cpu_percent': self.peak_cpu_percent,
            'average_cpu_percent': self.average_cpu_percent,
            'disk_io_read_mb': self.disk_io_read_mb,
            'disk_io_write_mb': self.disk_io_write_mb,
            'custom_metrics': self.custom_metrics,
            'metadata': self.metadata
        }

@dataclass
class BenchmarkComparison:
    """Comparison between two performance benchmarks"""
    baseline_metrics: PerformanceMetrics
    optimized_metrics: PerformanceMetrics
    improvement_percentage: float = 0.0
    throughput_improvement: float = 0.0
    memory_improvement: float = 0.0
    cpu_improvement: float = 0.0
    
    def __post_init__(self):
        """Calculate improvement metrics"""
        # Overall processing time improvement
        if self.baseline_metrics.duration_seconds > 0:
            self.improvement_percentage = (
                (self.baseline_metrics.duration_seconds - self.optimized_metrics.duration_seconds) /
                self.baseline_metrics.duration_seconds * 100
            )
        
        # Throughput improvement
        if self.baseline_metrics.records_per_second > 0:
            self.throughput_improvement = (
                (self.optimized_metrics.records_per_second - self.baseline_metrics.records_per_second) /
                self.baseline_metrics.records_per_second * 100
            )
        
        # Memory improvement (negative means less memory used)
        if self.baseline_metrics.average_memory_mb > 0:
            self.memory_improvement = (
                (self.baseline_metrics.average_memory_mb - self.optimized_metrics.average_memory_mb) /
                self.baseline_metrics.average_memory_mb * 100
            )
        
        # CPU improvement (negative means less CPU used)
        if self.baseline_metrics.average_cpu_percent > 0:
            self.cpu_improvement = (
                (self.baseline_metrics.average_cpu_percent - self.optimized_metrics.average_cpu_percent) /
                self.baseline_metrics.average_cpu_percent * 100
            )

class ResourceMonitor:
    """Real-time resource monitoring"""
    
    def __init__(self, sampling_interval: float = 1.0):
        self.sampling_interval = sampling_interval
        self.monitoring = False
        self.monitor_thread: Optional[threading.Thread] = None
        
        # Resource tracking
        self.cpu_samples = deque(maxlen=1000)
        self.memory_samples = deque(maxlen=1000)
        self.disk_io_samples = deque(maxlen=1000)
        
        # Process tracking
        self.process = psutil.Process()
        self.initial_disk_io = self.process.io_counters()
    
    def start_monitoring(self):
        """Start resource monitoring in background thread"""
        if self.monitoring:
            return
        
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_resources, daemon=True)
        self.monitor_thread.start()
    
    def stop_monitoring(self):
        """Stop resource monitoring"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=2.0)
    
    def _monitor_resources(self):
        """Background resource monitoring loop"""
        while self.monitoring:
            try:
                # CPU usage
                cpu_percent = self.process.cpu_percent()
                self.cpu_samples.append(cpu_percent)
                
                # Memory usage
                memory_info = self.process.memory_info()
                memory_mb = memory_info.rss / (1024 * 1024)
                self.memory_samples.append(memory_mb)
                
                # Disk I/O
                try:
                    disk_io = self.process.io_counters()
                    self.disk_io_samples.append({
                        'read_bytes': disk_io.read_bytes,
                        'write_bytes': disk_io.write_bytes
                    })
                except (psutil.AccessDenied, AttributeError):
                    # Some systems don't support per-process I/O counters
                    pass
                
                time.sleep(self.sampling_interval)
                
            except Exception as e:
                # Process might have ended or access denied
                break
    
    def get_current_stats(self) -> Dict[str, float]:
        """Get current resource statistics"""
        stats = {
            'cpu_percent': 0.0,
            'memory_mb': 0.0,
            'peak_memory_mb': 0.0,
            'average_memory_mb': 0.0,
            'peak_cpu_percent': 0.0,
            'average_cpu_percent': 0.0,
            'disk_read_mb': 0.0,
            'disk_write_mb': 0.0
        }
        
        if self.cpu_samples:
            stats['cpu_percent'] = self.cpu_samples[-1]
            stats['peak_cpu_percent'] = max(self.cpu_samples)
            stats['average_cpu_percent'] = np.mean(self.cpu_samples)
        
        if self.memory_samples:
            stats['memory_mb'] = self.memory_samples[-1]
            stats['peak_memory_mb'] = max(self.memory_samples)
            stats['average_memory_mb'] = np.mean(self.memory_samples)
        
        if self.disk_io_samples and self.initial_disk_io:
            try:
                current_io = self.process.io_counters()
                stats['disk_read_mb'] = (current_io.read_bytes - self.initial_disk_io.read_bytes) / (1024 * 1024)
                stats['disk_write_mb'] = (current_io.write_bytes - self.initial_disk_io.write_bytes) / (1024 * 1024)
            except (psutil.AccessDenied, AttributeError):
                pass
        
        return stats

class PerformanceMonitor:
    """Main performance monitoring system"""
    
    def __init__(self, log_level: str = 'INFO'):
        self.logger = self._setup_logging(log_level)
        
        # Active monitoring sessions
        self.active_monitors: Dict[str, ResourceMonitor] = {}
        self.metrics_history: List[PerformanceMetrics] = []
        
        # Performance baselines for comparison
        self.baselines: Dict[str, PerformanceMetrics] = {}
        
        # Benchmarking results
        self.benchmark_results: Dict[str, BenchmarkComparison] = {}
        
        self.logger.info("PerformanceMonitor initialized")
    
    def _setup_logging(self, log_level: str):
        """Setup logging"""
        log_dir = Path(__file__).parent.parent / "logs"
        log_dir.mkdir(exist_ok=True)
        
        logger = logging.getLogger('performance_monitor')
        logger.setLevel(getattr(logging, log_level.upper()))
        
        if not logger.handlers:
            # File handler
            log_file = log_dir / f"performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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
            
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)
        
        return logger
    
    def start_monitoring(self, process_name: str, metadata: Dict[str, Any] = None) -> str:
        """Start monitoring a process"""
        monitor_id = f"{process_name}_{int(time.time())}"
        
        # Create resource monitor
        resource_monitor = ResourceMonitor()
        resource_monitor.start_monitoring()
        
        self.active_monitors[monitor_id] = resource_monitor
        
        # Create performance metrics
        metrics = PerformanceMetrics(
            process_name=process_name,
            start_time=datetime.now(),
            metadata=metadata or {}
        )
        
        self.logger.info(f"Started monitoring process: {process_name} (ID: {monitor_id})")
        
        return monitor_id
    
    def stop_monitoring(self, monitor_id: str, records_processed: int = 0, 
                       bytes_processed: int = 0, custom_metrics: Dict[str, float] = None) -> PerformanceMetrics:
        """Stop monitoring and finalize metrics"""
        if monitor_id not in self.active_monitors:
            raise ValueError(f"Monitor ID {monitor_id} not found")
        
        resource_monitor = self.active_monitors[monitor_id]
        resource_monitor.stop_monitoring()
        
        # Get final resource stats
        resource_stats = resource_monitor.get_current_stats()
        
        # Create final metrics
        metrics = PerformanceMetrics(
            process_name=resource_monitor.process.name() if hasattr(resource_monitor, 'process') else monitor_id.split('_')[0],
            start_time=datetime.now() - timedelta(seconds=len(resource_monitor.cpu_samples)),
            end_time=datetime.now(),
            records_processed=records_processed,
            bytes_processed=bytes_processed,
            peak_memory_mb=resource_stats['peak_memory_mb'],
            average_memory_mb=resource_stats['average_memory_mb'],
            peak_cpu_percent=resource_stats['peak_cpu_percent'],
            average_cpu_percent=resource_stats['average_cpu_percent'],
            disk_io_read_mb=resource_stats['disk_read_mb'],
            disk_io_write_mb=resource_stats['disk_write_mb'],
            custom_metrics=custom_metrics or {}
        )
        
        metrics.finalize()
        
        # Store metrics
        self.metrics_history.append(metrics)
        
        # Clean up
        del self.active_monitors[monitor_id]
        
        self.logger.info(
            f"Stopped monitoring {monitor_id}: "
            f"Duration={metrics.duration_seconds:.2f}s, "
            f"Records/sec={metrics.records_per_second:.0f}, "
            f"Memory={metrics.average_memory_mb:.1f}MB"
        )
        
        return metrics
    
    def benchmark_process(self, process_name: str, process_function: Callable, 
                         *args, baseline_name: str = None, **kwargs) -> PerformanceMetrics:
        """Benchmark a process function"""
        self.logger.info(f"Starting benchmark for process: {process_name}")
        
        # Start monitoring
        monitor_id = self.start_monitoring(process_name, {'benchmark': True})
        
        start_time = time.time()
        result = None
        error = None
        
        try:
            # Execute the process
            result = process_function(*args, **kwargs)
        except Exception as e:
            error = e
            self.logger.error(f"Benchmark process {process_name} failed: {e}")
        
        end_time = time.time()
        
        # Determine records and bytes processed from result if possible
        records_processed = 0
        bytes_processed = 0
        
        if isinstance(result, dict):
            records_processed = result.get('records_processed', 0)
            bytes_processed = result.get('bytes_processed', 0)
        elif hasattr(result, 'shape') and hasattr(result.shape, '__len__'):
            # Pandas DataFrame or similar
            records_processed = result.shape[0] if len(result.shape) > 0 else 0
        
        # Stop monitoring
        metrics = self.stop_monitoring(
            monitor_id, 
            records_processed=records_processed,
            bytes_processed=bytes_processed,
            custom_metrics={'execution_success': 1.0 if error is None else 0.0}
        )
        
        # Store as baseline if specified
        if baseline_name:
            self.baselines[baseline_name] = metrics
            self.logger.info(f"Stored baseline metrics: {baseline_name}")
        
        if error:
            raise error
        
        return metrics
    
    def compare_with_baseline(self, current_metrics: PerformanceMetrics, 
                             baseline_name: str) -> BenchmarkComparison:
        """Compare current metrics with a baseline"""
        if baseline_name not in self.baselines:
            raise ValueError(f"Baseline {baseline_name} not found")
        
        baseline_metrics = self.baselines[baseline_name]
        comparison = BenchmarkComparison(
            baseline_metrics=baseline_metrics,
            optimized_metrics=current_metrics
        )
        
        # Store comparison
        comparison_key = f"{baseline_name}_vs_{current_metrics.process_name}"
        self.benchmark_results[comparison_key] = comparison
        
        self.logger.info(
            f"Benchmark comparison {comparison_key}: "
            f"Improvement={comparison.improvement_percentage:.1f}%, "
            f"Throughput={comparison.throughput_improvement:.1f}%"
        )
        
        return comparison
    
    def get_performance_report(self, process_name: str = None) -> Dict[str, Any]:
        """Generate comprehensive performance report"""
        if process_name:
            # Filter metrics for specific process
            filtered_metrics = [m for m in self.metrics_history if m.process_name == process_name]
        else:
            filtered_metrics = self.metrics_history
        
        if not filtered_metrics:
            return {'error': f'No metrics found for process: {process_name}'}
        
        # Calculate aggregate statistics
        durations = [m.duration_seconds for m in filtered_metrics]
        throughputs = [m.records_per_second for m in filtered_metrics if m.records_per_second > 0]
        memory_usage = [m.average_memory_mb for m in filtered_metrics]
        cpu_usage = [m.average_cpu_percent for m in filtered_metrics]
        
        report = {
            'process_name': process_name or 'All Processes',
            'report_generated': datetime.now().isoformat(),
            'total_executions': len(filtered_metrics),
            'performance_summary': {
                'average_duration_seconds': np.mean(durations) if durations else 0,
                'min_duration_seconds': min(durations) if durations else 0,
                'max_duration_seconds': max(durations) if durations else 0,
                'average_throughput_records_per_second': np.mean(throughputs) if throughputs else 0,
                'peak_throughput_records_per_second': max(throughputs) if throughputs else 0,
                'average_memory_mb': np.mean(memory_usage) if memory_usage else 0,
                'peak_memory_mb': max(memory_usage) if memory_usage else 0,
                'average_cpu_percent': np.mean(cpu_usage) if cpu_usage else 0
            },
            'recent_executions': [
                m.to_dict() for m in filtered_metrics[-10:]  # Last 10 executions
            ],
            'benchmark_comparisons': {
                name: {
                    'improvement_percentage': comp.improvement_percentage,
                    'throughput_improvement': comp.throughput_improvement,
                    'memory_improvement': comp.memory_improvement,
                    'cpu_improvement': comp.cpu_improvement
                }
                for name, comp in self.benchmark_results.items()
                if process_name is None or process_name in name
            }
        }
        
        return report
    
    def calculate_processing_improvement(self, baseline_duration: float, 
                                       optimized_duration: float) -> Dict[str, float]:
        """Calculate processing improvement metrics"""
        if baseline_duration <= 0:
            return {'error': 'Invalid baseline duration'}
        
        improvement_percentage = ((baseline_duration - optimized_duration) / baseline_duration) * 100
        speed_multiplier = baseline_duration / optimized_duration if optimized_duration > 0 else 0
        
        return {
            'baseline_duration_seconds': baseline_duration,
            'optimized_duration_seconds': optimized_duration,
            'improvement_percentage': improvement_percentage,
            'speed_multiplier': speed_multiplier,
            'time_saved_seconds': baseline_duration - optimized_duration,
            'target_achievement': {
                'target_improvement': 37.0,  # 37% faster processing target
                'achieved_improvement': improvement_percentage,
                'target_met': improvement_percentage >= 37.0
            }
        }
    
    def export_metrics(self, filename: str = None, format_type: str = 'json') -> str:
        """Export metrics to file"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"performance_metrics_{timestamp}.{format_type}"
        
        reports_dir = Path(__file__).parent.parent / "reports"
        reports_dir.mkdir(exist_ok=True)
        
        file_path = reports_dir / filename
        
        export_data = {
            'export_timestamp': datetime.now().isoformat(),
            'total_metrics': len(self.metrics_history),
            'baselines': {name: metrics.to_dict() for name, metrics in self.baselines.items()},
            'metrics_history': [metrics.to_dict() for metrics in self.metrics_history],
            'benchmark_results': {
                name: {
                    'baseline': comp.baseline_metrics.to_dict(),
                    'optimized': comp.optimized_metrics.to_dict(),
                    'improvement_percentage': comp.improvement_percentage,
                    'throughput_improvement': comp.throughput_improvement,
                    'memory_improvement': comp.memory_improvement,
                    'cpu_improvement': comp.cpu_improvement
                }
                for name, comp in self.benchmark_results.items()
            }
        }
        
        if format_type.lower() == 'json':
            with open(file_path, 'w') as f:
                json.dump(export_data, f, indent=2, default=str)
        elif format_type.lower() == 'csv':
            # Convert to DataFrame for CSV export
            df_data = []
            for metrics in self.metrics_history:
                df_data.append(metrics.to_dict())
            
            df = pd.DataFrame(df_data)
            df.to_csv(file_path, index=False)
        
        self.logger.info(f"Metrics exported to: {file_path}")
        return str(file_path)
    
    def clear_metrics(self):
        """Clear all stored metrics"""
        self.metrics_history.clear()
        self.baselines.clear()
        self.benchmark_results.clear()
        self.logger.info("All metrics cleared")

# Context manager for easy performance monitoring
class performance_monitor:
    """Context manager for automatic performance monitoring"""
    
    def __init__(self, monitor: PerformanceMonitor, process_name: str, 
                 records_processed: int = 0, bytes_processed: int = 0,
                 metadata: Dict[str, Any] = None):
        self.monitor = monitor
        self.process_name = process_name
        self.records_processed = records_processed
        self.bytes_processed = bytes_processed
        self.metadata = metadata
        self.monitor_id = None
    
    def __enter__(self):
        self.monitor_id = self.monitor.start_monitoring(self.process_name, self.metadata)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.monitor_id:
            metrics = self.monitor.stop_monitoring(
                self.monitor_id,
                self.records_processed,
                self.bytes_processed
            )
            return metrics
