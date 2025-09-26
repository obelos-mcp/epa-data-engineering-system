"""
Environmental Site Data Management & QA/QC Automation System
Task Management System

This module provides advanced task management capabilities including
caching, checkpointing, parallel execution, and failure isolation
for the workflow orchestration system.
"""

import time
import pickle
import hashlib
import threading
from pathlib import Path
from typing import Dict, List, Any, Optional, Callable, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import json
import logging
import concurrent.futures
from queue import Queue, Empty
import psutil

@dataclass
class TaskCheckpoint:
    """Task checkpoint for resumable execution"""
    task_id: str
    checkpoint_time: datetime
    task_state: Dict[str, Any]
    progress_percentage: float
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert checkpoint to dictionary"""
        return {
            'task_id': self.task_id,
            'checkpoint_time': self.checkpoint_time.isoformat(),
            'task_state': self.task_state,
            'progress_percentage': self.progress_percentage,
            'metadata': self.metadata
        }

@dataclass
class CacheEntry:
    """Cache entry for task results"""
    task_id: str
    task_signature: str  # Hash of task function + arguments
    result: Any
    created_time: datetime
    access_count: int = 0
    last_accessed: datetime = field(default_factory=datetime.now)
    expiry_time: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def is_expired(self) -> bool:
        """Check if cache entry is expired"""
        if self.expiry_time is None:
            return False
        return datetime.now() > self.expiry_time
    
    def access(self):
        """Mark cache entry as accessed"""
        self.access_count += 1
        self.last_accessed = datetime.now()

class TaskCache:
    """Advanced caching system for task results"""
    
    def __init__(self, cache_dir: str = None, max_cache_size_mb: int = 1024, 
                 default_ttl_hours: int = 24):
        self.cache_dir = Path(cache_dir or Path.cwd() / ".task_cache")
        self.cache_dir.mkdir(exist_ok=True)
        
        self.max_cache_size_mb = max_cache_size_mb
        self.default_ttl_hours = default_ttl_hours
        
        # In-memory cache index
        self.cache_index: Dict[str, CacheEntry] = {}
        self.cache_lock = threading.RLock()
        
        # Setup logging
        self.logger = logging.getLogger('task_cache')
        
        # Load existing cache index
        self._load_cache_index()
        
        self.logger.info(f"TaskCache initialized: {len(self.cache_index)} entries")
    
    def _load_cache_index(self):
        """Load cache index from disk"""
        index_file = self.cache_dir / "cache_index.json"
        
        if index_file.exists():
            try:
                with open(index_file, 'r') as f:
                    index_data = json.load(f)
                
                for entry_data in index_data:
                    entry = CacheEntry(
                        task_id=entry_data['task_id'],
                        task_signature=entry_data['task_signature'],
                        result=None,  # Will be loaded on demand
                        created_time=datetime.fromisoformat(entry_data['created_time']),
                        access_count=entry_data.get('access_count', 0),
                        last_accessed=datetime.fromisoformat(entry_data.get('last_accessed', entry_data['created_time'])),
                        expiry_time=datetime.fromisoformat(entry_data['expiry_time']) if entry_data.get('expiry_time') else None,
                        metadata=entry_data.get('metadata', {})
                    )
                    
                    # Check if cache file exists and entry is not expired
                    cache_file = self.cache_dir / f"{entry_data['task_signature']}.pkl"
                    if cache_file.exists() and not entry.is_expired():
                        self.cache_index[entry.task_signature] = entry
                
            except Exception as e:
                self.logger.warning(f"Failed to load cache index: {e}")
    
    def _save_cache_index(self):
        """Save cache index to disk"""
        index_file = self.cache_dir / "cache_index.json"
        
        try:
            index_data = []
            for entry in self.cache_index.values():
                index_data.append({
                    'task_id': entry.task_id,
                    'task_signature': entry.task_signature,
                    'created_time': entry.created_time.isoformat(),
                    'access_count': entry.access_count,
                    'last_accessed': entry.last_accessed.isoformat(),
                    'expiry_time': entry.expiry_time.isoformat() if entry.expiry_time else None,
                    'metadata': entry.metadata
                })
            
            with open(index_file, 'w') as f:
                json.dump(index_data, f, indent=2)
                
        except Exception as e:
            self.logger.error(f"Failed to save cache index: {e}")
    
    def _generate_task_signature(self, task_id: str, task_function: Callable, 
                                *args, **kwargs) -> str:
        """Generate unique signature for task"""
        # Create hash from task function name, args, and kwargs
        signature_data = {
            'task_id': task_id,
            'function_name': task_function.__name__,
            'module': task_function.__module__,
            'args': str(args),
            'kwargs': str(sorted(kwargs.items()))
        }
        
        signature_str = json.dumps(signature_data, sort_keys=True)
        return hashlib.sha256(signature_str.encode()).hexdigest()
    
    def get(self, task_id: str, task_function: Callable, *args, **kwargs) -> Optional[Any]:
        """Get cached result for task"""
        task_signature = self._generate_task_signature(task_id, task_function, *args, **kwargs)
        
        with self.cache_lock:
            if task_signature not in self.cache_index:
                return None
            
            entry = self.cache_index[task_signature]
            
            # Check if expired
            if entry.is_expired():
                self._remove_cache_entry(task_signature)
                return None
            
            # Load result from disk if not in memory
            if entry.result is None:
                cache_file = self.cache_dir / f"{task_signature}.pkl"
                if cache_file.exists():
                    try:
                        with open(cache_file, 'rb') as f:
                            entry.result = pickle.load(f)
                    except Exception as e:
                        self.logger.warning(f"Failed to load cached result: {e}")
                        self._remove_cache_entry(task_signature)
                        return None
                else:
                    self._remove_cache_entry(task_signature)
                    return None
            
            # Update access statistics
            entry.access()
            
            self.logger.debug(f"Cache hit for task {task_id}")
            return entry.result
    
    def put(self, task_id: str, task_function: Callable, result: Any, 
            ttl_hours: int = None, *args, **kwargs):
        """Store task result in cache"""
        task_signature = self._generate_task_signature(task_id, task_function, *args, **kwargs)
        ttl_hours = ttl_hours or self.default_ttl_hours
        
        with self.cache_lock:
            # Create cache entry
            entry = CacheEntry(
                task_id=task_id,
                task_signature=task_signature,
                result=result,
                created_time=datetime.now(),
                expiry_time=datetime.now() + timedelta(hours=ttl_hours) if ttl_hours > 0 else None
            )
            
            # Store to disk
            cache_file = self.cache_dir / f"{task_signature}.pkl"
            try:
                with open(cache_file, 'wb') as f:
                    pickle.dump(result, f)
                
                # Add to index
                self.cache_index[task_signature] = entry
                
                # Clean up old entries if needed
                self._cleanup_cache()
                
                # Save index
                self._save_cache_index()
                
                self.logger.debug(f"Cached result for task {task_id}")
                
            except Exception as e:
                self.logger.error(f"Failed to cache result for task {task_id}: {e}")
    
    def _remove_cache_entry(self, task_signature: str):
        """Remove cache entry"""
        if task_signature in self.cache_index:
            del self.cache_index[task_signature]
        
        cache_file = self.cache_dir / f"{task_signature}.pkl"
        if cache_file.exists():
            try:
                cache_file.unlink()
            except Exception as e:
                self.logger.warning(f"Failed to remove cache file: {e}")
    
    def _cleanup_cache(self):
        """Clean up expired and least-used cache entries"""
        current_time = datetime.now()
        
        # Remove expired entries
        expired_signatures = [
            sig for sig, entry in self.cache_index.items()
            if entry.is_expired()
        ]
        
        for sig in expired_signatures:
            self._remove_cache_entry(sig)
        
        # Check cache size
        cache_size_mb = self._get_cache_size_mb()
        
        if cache_size_mb > self.max_cache_size_mb:
            # Remove least recently used entries
            sorted_entries = sorted(
                self.cache_index.items(),
                key=lambda x: (x[1].access_count, x[1].last_accessed)
            )
            
            entries_to_remove = len(sorted_entries) // 4  # Remove 25%
            
            for sig, _ in sorted_entries[:entries_to_remove]:
                self._remove_cache_entry(sig)
            
            self.logger.info(f"Cache cleanup: removed {entries_to_remove} entries")
    
    def _get_cache_size_mb(self) -> float:
        """Get total cache size in MB"""
        total_size = 0
        
        for file_path in self.cache_dir.glob("*.pkl"):
            try:
                total_size += file_path.stat().st_size
            except Exception:
                pass
        
        return total_size / (1024 * 1024)
    
    def clear(self):
        """Clear all cache entries"""
        with self.cache_lock:
            for sig in list(self.cache_index.keys()):
                self._remove_cache_entry(sig)
            
            self._save_cache_index()
            self.logger.info("Cache cleared")

class CheckpointManager:
    """Manages task checkpoints for resumable execution"""
    
    def __init__(self, checkpoint_dir: str = None):
        self.checkpoint_dir = Path(checkpoint_dir or Path.cwd() / ".task_checkpoints")
        self.checkpoint_dir.mkdir(exist_ok=True)
        
        self.checkpoints: Dict[str, TaskCheckpoint] = {}
        self.checkpoint_lock = threading.RLock()
        
        # Setup logging
        self.logger = logging.getLogger('checkpoint_manager')
        
        # Load existing checkpoints
        self._load_checkpoints()
        
        self.logger.info(f"CheckpointManager initialized: {len(self.checkpoints)} checkpoints")
    
    def _load_checkpoints(self):
        """Load checkpoints from disk"""
        for checkpoint_file in self.checkpoint_dir.glob("*.json"):
            try:
                with open(checkpoint_file, 'r') as f:
                    checkpoint_data = json.load(f)
                
                checkpoint = TaskCheckpoint(
                    task_id=checkpoint_data['task_id'],
                    checkpoint_time=datetime.fromisoformat(checkpoint_data['checkpoint_time']),
                    task_state=checkpoint_data['task_state'],
                    progress_percentage=checkpoint_data['progress_percentage'],
                    metadata=checkpoint_data.get('metadata', {})
                )
                
                self.checkpoints[checkpoint.task_id] = checkpoint
                
            except Exception as e:
                self.logger.warning(f"Failed to load checkpoint {checkpoint_file}: {e}")
    
    def save_checkpoint(self, task_id: str, task_state: Dict[str, Any], 
                       progress_percentage: float, metadata: Dict[str, Any] = None):
        """Save task checkpoint"""
        with self.checkpoint_lock:
            checkpoint = TaskCheckpoint(
                task_id=task_id,
                checkpoint_time=datetime.now(),
                task_state=task_state,
                progress_percentage=progress_percentage,
                metadata=metadata or {}
            )
            
            self.checkpoints[task_id] = checkpoint
            
            # Save to disk
            checkpoint_file = self.checkpoint_dir / f"{task_id}.json"
            try:
                with open(checkpoint_file, 'w') as f:
                    json.dump(checkpoint.to_dict(), f, indent=2, default=str)
                
                self.logger.debug(f"Checkpoint saved for task {task_id} ({progress_percentage:.1f}%)")
                
            except Exception as e:
                self.logger.error(f"Failed to save checkpoint for task {task_id}: {e}")
    
    def get_checkpoint(self, task_id: str) -> Optional[TaskCheckpoint]:
        """Get checkpoint for task"""
        return self.checkpoints.get(task_id)
    
    def remove_checkpoint(self, task_id: str):
        """Remove checkpoint for completed task"""
        with self.checkpoint_lock:
            if task_id in self.checkpoints:
                del self.checkpoints[task_id]
            
            checkpoint_file = self.checkpoint_dir / f"{task_id}.json"
            if checkpoint_file.exists():
                try:
                    checkpoint_file.unlink()
                    self.logger.debug(f"Checkpoint removed for task {task_id}")
                except Exception as e:
                    self.logger.warning(f"Failed to remove checkpoint file: {e}")
    
    def clear_checkpoints(self):
        """Clear all checkpoints"""
        with self.checkpoint_lock:
            for task_id in list(self.checkpoints.keys()):
                self.remove_checkpoint(task_id)
            
            self.logger.info("All checkpoints cleared")

class ParallelTaskExecutor:
    """Advanced parallel task execution with resource management"""
    
    def __init__(self, max_workers: int = None, resource_limits: Dict[str, Any] = None):
        self.max_workers = max_workers or min(32, (psutil.cpu_count() or 1) + 4)
        self.resource_limits = resource_limits or {
            'max_memory_mb': 8192,
            'max_cpu_percent': 80.0
        }
        
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers)
        self.active_tasks: Dict[str, concurrent.futures.Future] = {}
        self.task_queue = Queue()
        self.resource_monitor_thread = None
        self.monitoring = False
        
        # Setup logging
        self.logger = logging.getLogger('parallel_executor')
        
        # Start resource monitoring
        self._start_resource_monitoring()
        
        self.logger.info(f"ParallelTaskExecutor initialized: {self.max_workers} workers")
    
    def _start_resource_monitoring(self):
        """Start resource monitoring thread"""
        self.monitoring = True
        self.resource_monitor_thread = threading.Thread(
            target=self._monitor_resources, 
            daemon=True
        )
        self.resource_monitor_thread.start()
    
    def _monitor_resources(self):
        """Monitor system resources and throttle execution if needed"""
        while self.monitoring:
            try:
                # Check memory usage
                memory_info = psutil.virtual_memory()
                memory_used_mb = (memory_info.total - memory_info.available) / (1024 * 1024)
                
                # Check CPU usage
                cpu_percent = psutil.cpu_percent(interval=1.0)
                
                # Throttle if resources are constrained
                if (memory_used_mb > self.resource_limits['max_memory_mb'] or 
                    cpu_percent > self.resource_limits['max_cpu_percent']):
                    
                    self.logger.warning(
                        f"Resource constraints detected: "
                        f"Memory={memory_used_mb:.0f}MB, CPU={cpu_percent:.1f}%"
                    )
                    
                    # Reduce parallelism temporarily
                    time.sleep(2.0)
                else:
                    time.sleep(0.5)
                    
            except Exception as e:
                self.logger.error(f"Resource monitoring error: {e}")
                time.sleep(1.0)
    
    def submit_task(self, task_id: str, task_function: Callable, 
                   *args, **kwargs) -> concurrent.futures.Future:
        """Submit task for parallel execution"""
        future = self.executor.submit(task_function, *args, **kwargs)
        self.active_tasks[task_id] = future
        
        self.logger.debug(f"Task submitted for parallel execution: {task_id}")
        
        return future
    
    def wait_for_completion(self, task_ids: List[str] = None, 
                          timeout: float = None) -> Dict[str, Any]:
        """Wait for tasks to complete"""
        if task_ids is None:
            task_ids = list(self.active_tasks.keys())
        
        results = {}
        
        # Get futures for specified tasks
        futures_to_tasks = {
            self.active_tasks[task_id]: task_id 
            for task_id in task_ids 
            if task_id in self.active_tasks
        }
        
        # Wait for completion
        try:
            for future in concurrent.futures.as_completed(futures_to_tasks, timeout=timeout):
                task_id = futures_to_tasks[future]
                
                try:
                    result = future.result()
                    results[task_id] = {'success': True, 'result': result}
                    
                    self.logger.debug(f"Task completed successfully: {task_id}")
                    
                except Exception as e:
                    results[task_id] = {'success': False, 'error': str(e)}
                    
                    self.logger.error(f"Task failed: {task_id} - {e}")
                
                # Remove from active tasks
                if task_id in self.active_tasks:
                    del self.active_tasks[task_id]
        
        except concurrent.futures.TimeoutError:
            self.logger.warning(f"Timeout waiting for tasks: {task_ids}")
            
            # Mark timed-out tasks
            for task_id in task_ids:
                if task_id in self.active_tasks and task_id not in results:
                    results[task_id] = {'success': False, 'error': 'Timeout'}
        
        return results
    
    def cancel_task(self, task_id: str) -> bool:
        """Cancel a running task"""
        if task_id in self.active_tasks:
            future = self.active_tasks[task_id]
            cancelled = future.cancel()
            
            if cancelled:
                del self.active_tasks[task_id]
                self.logger.info(f"Task cancelled: {task_id}")
            
            return cancelled
        
        return False
    
    def get_active_tasks(self) -> List[str]:
        """Get list of active task IDs"""
        return list(self.active_tasks.keys())
    
    def shutdown(self, wait: bool = True):
        """Shutdown the executor"""
        self.monitoring = False
        
        if self.resource_monitor_thread:
            self.resource_monitor_thread.join(timeout=2.0)
        
        self.executor.shutdown(wait=wait)
        self.logger.info("ParallelTaskExecutor shut down")

class TaskManager:
    """Advanced task management system with caching, checkpointing, and parallel execution"""
    
    def __init__(self, cache_dir: str = None, checkpoint_dir: str = None,
                 max_parallel_tasks: int = None, enable_caching: bool = True,
                 enable_checkpointing: bool = True):
        
        self.enable_caching = enable_caching
        self.enable_checkpointing = enable_checkpointing
        
        # Initialize components
        self.cache = TaskCache(cache_dir) if enable_caching else None
        self.checkpoint_manager = CheckpointManager(checkpoint_dir) if enable_checkpointing else None
        self.parallel_executor = ParallelTaskExecutor(max_parallel_tasks)
        
        # Setup logging
        self.logger = logging.getLogger('task_manager')
        
        self.logger.info(
            f"TaskManager initialized: "
            f"Caching={'enabled' if enable_caching else 'disabled'}, "
            f"Checkpointing={'enabled' if enable_checkpointing else 'disabled'}, "
            f"Parallel={max_parallel_tasks or 'auto'}"
        )
    
    def execute_task(self, task_id: str, task_function: Callable, 
                    use_cache: bool = True, use_checkpoint: bool = True,
                    cache_ttl_hours: int = 24, *args, **kwargs) -> Any:
        """Execute task with caching and checkpointing"""
        
        # Check cache first
        if self.cache and use_cache:
            cached_result = self.cache.get(task_id, task_function, *args, **kwargs)
            if cached_result is not None:
                self.logger.info(f"Task result retrieved from cache: {task_id}")
                return cached_result
        
        # Check for existing checkpoint
        checkpoint = None
        if self.checkpoint_manager and use_checkpoint:
            checkpoint = self.checkpoint_manager.get_checkpoint(task_id)
            if checkpoint:
                self.logger.info(f"Resuming task from checkpoint: {task_id} ({checkpoint.progress_percentage:.1f}%)")
        
        # Execute task
        try:
            self.logger.info(f"Executing task: {task_id}")
            
            # Add checkpoint callback if checkpointing is enabled
            if self.checkpoint_manager and use_checkpoint:
                kwargs['checkpoint_callback'] = lambda state, progress: self.checkpoint_manager.save_checkpoint(
                    task_id, state, progress
                )
            
            # Add resume state if available
            if checkpoint:
                kwargs['resume_state'] = checkpoint.task_state
            
            # Execute the task
            result = task_function(*args, **kwargs)
            
            # Cache the result
            if self.cache and use_cache:
                self.cache.put(task_id, task_function, result, cache_ttl_hours, *args, **kwargs)
            
            # Remove checkpoint on successful completion
            if self.checkpoint_manager and use_checkpoint:
                self.checkpoint_manager.remove_checkpoint(task_id)
            
            self.logger.info(f"Task completed successfully: {task_id}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Task failed: {task_id} - {e}")
            raise
    
    def execute_parallel_tasks(self, tasks: List[Dict[str, Any]], 
                              timeout: float = None) -> Dict[str, Any]:
        """Execute multiple tasks in parallel"""
        
        self.logger.info(f"Executing {len(tasks)} tasks in parallel")
        
        # Submit all tasks
        for task_config in tasks:
            task_id = task_config['task_id']
            task_function = task_config['task_function']
            args = task_config.get('args', ())
            kwargs = task_config.get('kwargs', {})
            
            # Wrap task execution with caching/checkpointing
            wrapped_function = lambda tf=task_function, tid=task_id, a=args, kw=kwargs: self.execute_task(
                tid, tf, *a, **kw
            )
            
            self.parallel_executor.submit_task(task_id, wrapped_function)
        
        # Wait for completion
        task_ids = [task['task_id'] for task in tasks]
        results = self.parallel_executor.wait_for_completion(task_ids, timeout)
        
        self.logger.info(f"Parallel execution completed: {len(results)} results")
        
        return results
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        if not self.cache:
            return {'caching_disabled': True}
        
        return {
            'total_entries': len(self.cache.cache_index),
            'cache_size_mb': self.cache._get_cache_size_mb(),
            'max_cache_size_mb': self.cache.max_cache_size_mb,
            'cache_hit_rate': 'Not tracked',  # Would need to implement hit rate tracking
            'oldest_entry': min(
                (entry.created_time for entry in self.cache.cache_index.values()),
                default=None
            ),
            'most_accessed_entry': max(
                (entry.access_count for entry in self.cache.cache_index.values()),
                default=0
            )
        }
    
    def get_checkpoint_stats(self) -> Dict[str, Any]:
        """Get checkpoint statistics"""
        if not self.checkpoint_manager:
            return {'checkpointing_disabled': True}
        
        return {
            'total_checkpoints': len(self.checkpoint_manager.checkpoints),
            'checkpoints_by_progress': {
                '0-25%': sum(1 for cp in self.checkpoint_manager.checkpoints.values() if cp.progress_percentage < 25),
                '25-50%': sum(1 for cp in self.checkpoint_manager.checkpoints.values() if 25 <= cp.progress_percentage < 50),
                '50-75%': sum(1 for cp in self.checkpoint_manager.checkpoints.values() if 50 <= cp.progress_percentage < 75),
                '75-100%': sum(1 for cp in self.checkpoint_manager.checkpoints.values() if cp.progress_percentage >= 75)
            },
            'oldest_checkpoint': min(
                (cp.checkpoint_time for cp in self.checkpoint_manager.checkpoints.values()),
                default=None
            )
        }
    
    def cleanup(self):
        """Cleanup resources"""
        if self.cache:
            self.cache.clear()
        
        if self.checkpoint_manager:
            self.checkpoint_manager.clear_checkpoints()
        
        self.parallel_executor.shutdown()
        
        self.logger.info("TaskManager cleanup completed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()
