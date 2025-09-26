"""
Environmental Site Data Management & QA/QC Automation System
Workflow Orchestrator

This module provides Airflow-like workflow orchestration capabilities with
DAG definition, task scheduling, dependency resolution, and parallel processing
for the EPA environmental data processing pipeline.
"""

import time
import logging
import threading
from pathlib import Path
from typing import Dict, List, Any, Optional, Callable, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import json
import concurrent.futures
from queue import Queue
import traceback

class TaskStatus(Enum):
    """Task execution status"""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    SKIPPED = "SKIPPED"
    UPSTREAM_FAILED = "UPSTREAM_FAILED"

class WorkflowStatus(Enum):
    """Workflow execution status"""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"

@dataclass
class TaskResult:
    """Result of task execution"""
    task_id: str
    status: TaskStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0
    output: Any = None
    error_message: str = ""
    retry_count: int = 0
    memory_usage_mb: float = 0.0
    
    def finalize(self):
        """Finalize task result calculations"""
        if self.end_time and self.start_time:
            self.duration_seconds = (self.end_time - self.start_time).total_seconds()

@dataclass
class TaskConfig:
    """Configuration for a task"""
    task_id: str
    task_function: Callable
    dependencies: List[str] = field(default_factory=list)
    max_retries: int = 3
    retry_delay_seconds: int = 60
    timeout_seconds: int = 3600  # 1 hour default
    retry_exponential_backoff: bool = True
    task_args: tuple = field(default_factory=tuple)
    task_kwargs: Dict[str, Any] = field(default_factory=dict)
    description: str = ""
    
class Task:
    """Individual task in the workflow"""
    
    def __init__(self, config: TaskConfig):
        self.config = config
        self.status = TaskStatus.PENDING
        self.result: Optional[TaskResult] = None
        self.dependencies_met = False
        self.logger = logging.getLogger(f'task.{config.task_id}')
    
    def can_execute(self, completed_tasks: Set[str]) -> bool:
        """Check if task can be executed based on dependencies"""
        if self.status != TaskStatus.PENDING:
            return False
        
        # Check if all dependencies are completed successfully
        for dep_id in self.config.dependencies:
            if dep_id not in completed_tasks:
                return False
        
        return True
    
    def execute(self, workflow_context: Dict[str, Any] = None) -> TaskResult:
        """Execute the task with retry logic"""
        workflow_context = workflow_context or {}
        
        self.result = TaskResult(
            task_id=self.config.task_id,
            status=TaskStatus.RUNNING,
            start_time=datetime.now()
        )
        
        self.status = TaskStatus.RUNNING
        self.logger.info(f"Starting task execution: {self.config.task_id}")
        
        for attempt in range(self.config.max_retries + 1):
            try:
                self.result.retry_count = attempt
                
                # Execute the task function
                start_time = time.time()
                
                # Pass workflow context and task results to the function
                task_kwargs = self.config.task_kwargs.copy()
                task_kwargs['workflow_context'] = workflow_context
                
                output = self.config.task_function(*self.config.task_args, **task_kwargs)
                
                end_time = time.time()
                
                # Task succeeded
                self.result.status = TaskStatus.SUCCESS
                self.result.end_time = datetime.now()
                self.result.output = output
                self.result.finalize()
                
                self.status = TaskStatus.SUCCESS
                
                self.logger.info(
                    f"Task completed successfully: {self.config.task_id} "
                    f"(duration: {self.result.duration_seconds:.2f}s, attempt: {attempt + 1})"
                )
                
                return self.result
                
            except Exception as e:
                error_msg = f"Task failed on attempt {attempt + 1}: {str(e)}"
                self.logger.error(f"{error_msg}\n{traceback.format_exc()}")
                
                self.result.error_message = error_msg
                
                if attempt < self.config.max_retries:
                    # Calculate retry delay
                    if self.config.retry_exponential_backoff:
                        delay = self.config.retry_delay_seconds * (2 ** attempt)
                    else:
                        delay = self.config.retry_delay_seconds
                    
                    self.status = TaskStatus.RETRYING
                    self.logger.info(f"Retrying task {self.config.task_id} in {delay} seconds...")
                    
                    time.sleep(delay)
                else:
                    # Max retries exceeded
                    self.result.status = TaskStatus.FAILED
                    self.result.end_time = datetime.now()
                    self.result.finalize()
                    
                    self.status = TaskStatus.FAILED
                    
                    self.logger.error(f"Task failed after {self.config.max_retries + 1} attempts: {self.config.task_id}")
                    
                    return self.result
        
        return self.result

class WorkflowDAG:
    """Directed Acyclic Graph for workflow definition"""
    
    def __init__(self, dag_id: str, description: str = ""):
        self.dag_id = dag_id
        self.description = description
        self.tasks: Dict[str, Task] = {}
        self.task_dependencies: Dict[str, Set[str]] = {}
        self.logger = logging.getLogger(f'dag.{dag_id}')
        
        # Workflow execution state
        self.status = WorkflowStatus.PENDING
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.execution_context: Dict[str, Any] = {}
    
    def add_task(self, task_config: TaskConfig) -> 'WorkflowDAG':
        """Add a task to the DAG"""
        if task_config.task_id in self.tasks:
            raise ValueError(f"Task {task_config.task_id} already exists in DAG")
        
        task = Task(task_config)
        self.tasks[task_config.task_id] = task
        self.task_dependencies[task_config.task_id] = set(task_config.dependencies)
        
        self.logger.info(f"Added task to DAG: {task_config.task_id} (dependencies: {task_config.dependencies})")
        
        return self  # For method chaining
    
    def validate_dag(self) -> bool:
        """Validate DAG for cycles and missing dependencies"""
        # Check for missing dependencies
        for task_id, deps in self.task_dependencies.items():
            for dep_id in deps:
                if dep_id not in self.tasks:
                    raise ValueError(f"Task {task_id} depends on non-existent task {dep_id}")
        
        # Check for cycles using DFS
        visited = set()
        rec_stack = set()
        
        def has_cycle(task_id: str) -> bool:
            visited.add(task_id)
            rec_stack.add(task_id)
            
            for dep_id in self.task_dependencies.get(task_id, []):
                if dep_id not in visited:
                    if has_cycle(dep_id):
                        return True
                elif dep_id in rec_stack:
                    return True
            
            rec_stack.remove(task_id)
            return False
        
        for task_id in self.tasks:
            if task_id not in visited:
                if has_cycle(task_id):
                    raise ValueError(f"Cycle detected in DAG involving task {task_id}")
        
        return True
    
    def get_execution_order(self) -> List[List[str]]:
        """Get task execution order in topological levels for parallel execution"""
        # Calculate in-degrees
        in_degree = {task_id: 0 for task_id in self.tasks}
        for task_id, deps in self.task_dependencies.items():
            in_degree[task_id] = len(deps)
        
        # Topological sort with levels
        execution_levels = []
        remaining_tasks = set(self.tasks.keys())
        
        while remaining_tasks:
            # Find tasks with no remaining dependencies
            ready_tasks = [
                task_id for task_id in remaining_tasks 
                if in_degree[task_id] == 0
            ]
            
            if not ready_tasks:
                raise ValueError("Circular dependency detected in DAG")
            
            execution_levels.append(ready_tasks)
            
            # Remove ready tasks and update in-degrees
            for task_id in ready_tasks:
                remaining_tasks.remove(task_id)
                
                # Update in-degrees for dependent tasks
                for other_task_id in remaining_tasks:
                    if task_id in self.task_dependencies[other_task_id]:
                        in_degree[other_task_id] -= 1
        
        return execution_levels
    
    def get_task_summary(self) -> Dict[str, Any]:
        """Get summary of DAG tasks and their status"""
        return {
            'dag_id': self.dag_id,
            'description': self.description,
            'total_tasks': len(self.tasks),
            'task_status_counts': {
                status.value: sum(1 for task in self.tasks.values() if task.status == status)
                for status in TaskStatus
            },
            'workflow_status': self.status.value,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'tasks': [
                {
                    'task_id': task.config.task_id,
                    'status': task.status.value,
                    'dependencies': task.config.dependencies,
                    'duration_seconds': task.result.duration_seconds if task.result else 0,
                    'retry_count': task.result.retry_count if task.result else 0,
                    'description': task.config.description
                }
                for task in self.tasks.values()
            ]
        }

class WorkflowOrchestrator:
    """Main workflow orchestrator with parallel execution capabilities"""
    
    def __init__(self, max_parallel_tasks: int = 4, log_level: str = 'INFO'):
        self.max_parallel_tasks = max_parallel_tasks
        self.active_workflows: Dict[str, WorkflowDAG] = {}
        
        # Setup logging
        self._setup_logging(log_level)
        
        # Execution state
        self.execution_stats: Dict[str, Any] = {}
        
        self.logger.info(f"WorkflowOrchestrator initialized (max_parallel_tasks: {max_parallel_tasks})")
    
    def _setup_logging(self, log_level: str):
        """Setup comprehensive logging"""
        log_dir = Path(__file__).parent.parent / "logs"
        log_dir.mkdir(exist_ok=True)
        
        # Create logger
        self.logger = logging.getLogger('workflow_orchestrator')
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # File handler
        log_file = log_dir / f"orchestrator_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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
    
    def register_workflow(self, dag: WorkflowDAG) -> bool:
        """Register a workflow DAG"""
        try:
            # Validate DAG
            dag.validate_dag()
            
            # Register workflow
            self.active_workflows[dag.dag_id] = dag
            
            self.logger.info(f"Registered workflow: {dag.dag_id} ({len(dag.tasks)} tasks)")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to register workflow {dag.dag_id}: {e}")
            return False
    
    def execute_workflow(self, dag_id: str, execution_context: Dict[str, Any] = None) -> Dict[str, Any]:
        """Execute a workflow with parallel task execution"""
        if dag_id not in self.active_workflows:
            raise ValueError(f"Workflow {dag_id} not found")
        
        dag = self.active_workflows[dag_id]
        execution_context = execution_context or {}
        
        self.logger.info(f"Starting workflow execution: {dag_id}")
        
        # Initialize workflow execution state
        dag.status = WorkflowStatus.RUNNING
        dag.start_time = datetime.now()
        dag.execution_context = execution_context
        
        execution_stats = {
            'dag_id': dag_id,
            'start_time': dag.start_time,
            'total_tasks': len(dag.tasks),
            'completed_tasks': 0,
            'failed_tasks': 0,
            'task_results': {},
            'execution_levels': [],
            'total_duration_seconds': 0.0
        }
        
        try:
            # Get execution order (topological levels)
            execution_levels = dag.get_execution_order()
            execution_stats['execution_levels'] = [
                {'level': i, 'tasks': level} for i, level in enumerate(execution_levels)
            ]
            
            self.logger.info(f"Execution plan: {len(execution_levels)} levels, tasks per level: {[len(level) for level in execution_levels]}")
            
            completed_tasks = set()
            
            # Execute tasks level by level
            for level_idx, task_level in enumerate(execution_levels):
                self.logger.info(f"Executing level {level_idx + 1}/{len(execution_levels)}: {task_level}")
                
                level_start_time = time.time()
                
                # Execute tasks in parallel within the level
                level_results = self._execute_task_level(dag, task_level, completed_tasks, execution_context)
                
                level_duration = time.time() - level_start_time
                
                # Process level results
                level_success_count = 0
                level_failure_count = 0
                
                for task_id, result in level_results.items():
                    execution_stats['task_results'][task_id] = {
                        'status': result.status.value,
                        'duration_seconds': result.duration_seconds,
                        'retry_count': result.retry_count,
                        'error_message': result.error_message,
                        'memory_usage_mb': result.memory_usage_mb
                    }
                    
                    if result.status == TaskStatus.SUCCESS:
                        completed_tasks.add(task_id)
                        level_success_count += 1
                        execution_stats['completed_tasks'] += 1
                    else:
                        level_failure_count += 1
                        execution_stats['failed_tasks'] += 1
                        
                        # Mark downstream tasks as upstream failed
                        self._mark_downstream_failed(dag, task_id)
                
                self.logger.info(
                    f"Level {level_idx + 1} completed: {level_success_count} success, "
                    f"{level_failure_count} failed (duration: {level_duration:.2f}s)"
                )
                
                # If any critical tasks failed, consider stopping
                if level_failure_count > 0:
                    self.logger.warning(f"Level {level_idx + 1} had {level_failure_count} failed tasks")
            
            # Finalize workflow execution
            dag.end_time = datetime.now()
            execution_stats['end_time'] = dag.end_time
            execution_stats['total_duration_seconds'] = (dag.end_time - dag.start_time).total_seconds()
            
            # Determine final workflow status
            if execution_stats['failed_tasks'] == 0:
                dag.status = WorkflowStatus.SUCCESS
            elif execution_stats['completed_tasks'] > 0:
                dag.status = WorkflowStatus.PARTIAL_SUCCESS
            else:
                dag.status = WorkflowStatus.FAILED
            
            execution_stats['final_status'] = dag.status.value
            
            self.logger.info(
                f"Workflow {dag_id} completed: {dag.status.value} "
                f"({execution_stats['completed_tasks']}/{execution_stats['total_tasks']} tasks successful, "
                f"duration: {execution_stats['total_duration_seconds']:.2f}s)"
            )
            
            # Store execution stats
            self.execution_stats[dag_id] = execution_stats
            
            return execution_stats
            
        except Exception as e:
            dag.status = WorkflowStatus.FAILED
            dag.end_time = datetime.now()
            
            execution_stats['final_status'] = WorkflowStatus.FAILED.value
            execution_stats['error_message'] = str(e)
            execution_stats['end_time'] = dag.end_time
            
            self.logger.error(f"Workflow {dag_id} failed: {e}\n{traceback.format_exc()}")
            
            return execution_stats
    
    def _execute_task_level(self, dag: WorkflowDAG, task_ids: List[str], 
                           completed_tasks: Set[str], execution_context: Dict[str, Any]) -> Dict[str, TaskResult]:
        """Execute a level of tasks in parallel"""
        level_results = {}
        
        # Filter tasks that can actually execute
        executable_tasks = []
        for task_id in task_ids:
            task = dag.tasks[task_id]
            if task.can_execute(completed_tasks):
                executable_tasks.append(task)
            else:
                # Mark as upstream failed
                task.status = TaskStatus.UPSTREAM_FAILED
                result = TaskResult(
                    task_id=task_id,
                    status=TaskStatus.UPSTREAM_FAILED,
                    start_time=datetime.now(),
                    end_time=datetime.now(),
                    error_message="Upstream task failed"
                )
                result.finalize()
                level_results[task_id] = result
        
        if not executable_tasks:
            return level_results
        
        # Execute tasks in parallel
        max_workers = min(len(executable_tasks), self.max_parallel_tasks)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tasks
            future_to_task = {
                executor.submit(task.execute, execution_context): task 
                for task in executable_tasks
            }
            
            # Collect results
            for future in concurrent.futures.as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    result = future.result()
                    level_results[task.config.task_id] = result
                except Exception as e:
                    # This shouldn't happen as exceptions are handled in task.execute()
                    self.logger.error(f"Unexpected error in task {task.config.task_id}: {e}")
                    result = TaskResult(
                        task_id=task.config.task_id,
                        status=TaskStatus.FAILED,
                        start_time=datetime.now(),
                        end_time=datetime.now(),
                        error_message=f"Unexpected error: {str(e)}"
                    )
                    result.finalize()
                    level_results[task.config.task_id] = result
        
        return level_results
    
    def _mark_downstream_failed(self, dag: WorkflowDAG, failed_task_id: str):
        """Mark all downstream tasks as upstream failed"""
        # Find all tasks that depend on the failed task
        downstream_tasks = []
        
        def find_downstream(task_id: str):
            for other_task_id, deps in dag.task_dependencies.items():
                if task_id in deps and other_task_id not in downstream_tasks:
                    downstream_tasks.append(other_task_id)
                    find_downstream(other_task_id)  # Recursively find downstream
        
        find_downstream(failed_task_id)
        
        # Mark downstream tasks as upstream failed
        for task_id in downstream_tasks:
            task = dag.tasks[task_id]
            if task.status == TaskStatus.PENDING:
                task.status = TaskStatus.UPSTREAM_FAILED
                self.logger.info(f"Marked task {task_id} as upstream failed due to {failed_task_id}")
    
    def get_workflow_status(self, dag_id: str) -> Optional[Dict[str, Any]]:
        """Get current status of a workflow"""
        if dag_id not in self.active_workflows:
            return None
        
        dag = self.active_workflows[dag_id]
        return dag.get_task_summary()
    
    def get_execution_metrics(self, dag_id: str = None) -> Dict[str, Any]:
        """Get execution metrics for workflows"""
        if dag_id:
            return self.execution_stats.get(dag_id, {})
        else:
            return {
                'total_workflows_executed': len(self.execution_stats),
                'workflows': self.execution_stats,
                'summary': {
                    'total_tasks_executed': sum(
                        stats.get('total_tasks', 0) for stats in self.execution_stats.values()
                    ),
                    'total_successful_tasks': sum(
                        stats.get('completed_tasks', 0) for stats in self.execution_stats.values()
                    ),
                    'total_failed_tasks': sum(
                        stats.get('failed_tasks', 0) for stats in self.execution_stats.values()
                    ),
                    'average_workflow_duration': sum(
                        stats.get('total_duration_seconds', 0) for stats in self.execution_stats.values()
                    ) / len(self.execution_stats) if self.execution_stats else 0
                }
            }
    
    def clear_execution_history(self):
        """Clear execution history"""
        self.execution_stats.clear()
        self.logger.info("Execution history cleared")

# Utility functions for common task patterns

def create_etl_dag(dag_id: str = "epa_etl_pipeline") -> WorkflowDAG:
    """Create a standard ETL DAG template"""
    dag = WorkflowDAG(
        dag_id=dag_id,
        description="EPA Environmental Data ETL Pipeline with QA/QC Integration"
    )
    
    # This is a template - actual task functions would be added by the pipeline runner
    return dag

def create_task_config(task_id: str, task_function: Callable, 
                      dependencies: List[str] = None, **kwargs) -> TaskConfig:
    """Helper function to create task configuration"""
    return TaskConfig(
        task_id=task_id,
        task_function=task_function,
        dependencies=dependencies or [],
        **kwargs
    )
