"""
Environmental Site Data Management & QA/QC Automation System
Workflow Monitoring & Alerting

This module provides real-time workflow monitoring, alerting, and
dashboard data generation for the orchestration system.
"""

import time
import threading
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import json
import logging
import psutil
from collections import deque, defaultdict

class AlertSeverity(Enum):
    """Alert severity levels"""
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"
    EMERGENCY = "EMERGENCY"

class AlertType(Enum):
    """Types of alerts"""
    TASK_FAILURE = "TASK_FAILURE"
    WORKFLOW_FAILURE = "WORKFLOW_FAILURE"
    PERFORMANCE_DEGRADATION = "PERFORMANCE_DEGRADATION"
    RESOURCE_EXHAUSTION = "RESOURCE_EXHAUSTION"
    SLA_BREACH = "SLA_BREACH"
    SYSTEM_ERROR = "SYSTEM_ERROR"
    DATA_QUALITY_ISSUE = "DATA_QUALITY_ISSUE"

@dataclass
class Alert:
    """Alert message"""
    alert_id: str
    alert_type: AlertType
    severity: AlertSeverity
    title: str
    message: str
    timestamp: datetime
    source: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    acknowledged: bool = False
    resolved: bool = False
    escalated: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert alert to dictionary"""
        return {
            'alert_id': self.alert_id,
            'alert_type': self.alert_type.value,
            'severity': self.severity.value,
            'title': self.title,
            'message': self.message,
            'timestamp': self.timestamp.isoformat(),
            'source': self.source,
            'metadata': self.metadata,
            'acknowledged': self.acknowledged,
            'resolved': self.resolved,
            'escalated': self.escalated
        }

@dataclass
class SLADefinition:
    """Service Level Agreement definition"""
    name: str
    description: str
    metric_type: str  # 'duration', 'throughput', 'error_rate', etc.
    threshold_value: float
    threshold_operator: str  # '<', '>', '<=', '>='
    measurement_window_minutes: int = 60
    breach_tolerance_count: int = 1  # How many breaches before alert
    
class PerformanceDashboard:
    """Real-time performance dashboard data"""
    
    def __init__(self, history_size: int = 1000):
        self.history_size = history_size
        
        # Time-series data
        self.workflow_metrics = deque(maxlen=history_size)
        self.task_metrics = deque(maxlen=history_size)
        self.resource_metrics = deque(maxlen=history_size)
        
        # Current state
        self.active_workflows: Dict[str, Dict[str, Any]] = {}
        self.active_tasks: Dict[str, Dict[str, Any]] = {}
        
        # Aggregated statistics
        self.stats = {
            'total_workflows_executed': 0,
            'total_tasks_executed': 0,
            'total_workflow_failures': 0,
            'total_task_failures': 0,
            'average_workflow_duration': 0.0,
            'average_task_duration': 0.0,
            'current_throughput': 0.0,
            'system_health_score': 100.0
        }
    
    def update_workflow_metrics(self, workflow_id: str, metrics: Dict[str, Any]):
        """Update workflow metrics"""
        metrics['timestamp'] = datetime.now()
        metrics['workflow_id'] = workflow_id
        
        self.workflow_metrics.append(metrics)
        
        # Update active workflows
        if metrics.get('status') in ['RUNNING', 'PENDING']:
            self.active_workflows[workflow_id] = metrics
        elif workflow_id in self.active_workflows:
            del self.active_workflows[workflow_id]
        
        # Update statistics
        self._update_workflow_stats(metrics)
    
    def update_task_metrics(self, task_id: str, metrics: Dict[str, Any]):
        """Update task metrics"""
        metrics['timestamp'] = datetime.now()
        metrics['task_id'] = task_id
        
        self.task_metrics.append(metrics)
        
        # Update active tasks
        if metrics.get('status') in ['RUNNING', 'PENDING']:
            self.active_tasks[task_id] = metrics
        elif task_id in self.active_tasks:
            del self.active_tasks[task_id]
        
        # Update statistics
        self._update_task_stats(metrics)
    
    def update_resource_metrics(self, metrics: Dict[str, Any]):
        """Update system resource metrics"""
        metrics['timestamp'] = datetime.now()
        self.resource_metrics.append(metrics)
    
    def _update_workflow_stats(self, metrics: Dict[str, Any]):
        """Update workflow statistics"""
        if metrics.get('status') == 'COMPLETED':
            self.stats['total_workflows_executed'] += 1
            
            # Update average duration
            duration = metrics.get('duration_seconds', 0)
            current_avg = self.stats['average_workflow_duration']
            total_workflows = self.stats['total_workflows_executed']
            
            self.stats['average_workflow_duration'] = (
                (current_avg * (total_workflows - 1) + duration) / total_workflows
            )
        
        elif metrics.get('status') == 'FAILED':
            self.stats['total_workflow_failures'] += 1
    
    def _update_task_stats(self, metrics: Dict[str, Any]):
        """Update task statistics"""
        if metrics.get('status') == 'SUCCESS':
            self.stats['total_tasks_executed'] += 1
            
            # Update average duration
            duration = metrics.get('duration_seconds', 0)
            current_avg = self.stats['average_task_duration']
            total_tasks = self.stats['total_tasks_executed']
            
            self.stats['average_task_duration'] = (
                (current_avg * (total_tasks - 1) + duration) / total_tasks
            )
        
        elif metrics.get('status') == 'FAILED':
            self.stats['total_task_failures'] += 1
    
    def get_dashboard_data(self) -> Dict[str, Any]:
        """Get current dashboard data"""
        return {
            'timestamp': datetime.now().isoformat(),
            'statistics': self.stats,
            'active_workflows': len(self.active_workflows),
            'active_tasks': len(self.active_tasks),
            'workflow_details': list(self.active_workflows.values()),
            'task_details': list(self.active_tasks.values()),
            'recent_metrics': {
                'workflows': list(self.workflow_metrics)[-10:],  # Last 10
                'tasks': list(self.task_metrics)[-10:],
                'resources': list(self.resource_metrics)[-10:]
            },
            'performance_trends': self._calculate_performance_trends()
        }
    
    def _calculate_performance_trends(self) -> Dict[str, Any]:
        """Calculate performance trends"""
        if len(self.workflow_metrics) < 2:
            return {'insufficient_data': True}
        
        # Get recent metrics for trend calculation
        recent_workflows = list(self.workflow_metrics)[-20:]  # Last 20 workflows
        
        if not recent_workflows:
            return {'no_recent_data': True}
        
        # Calculate trends
        durations = [w.get('duration_seconds', 0) for w in recent_workflows if w.get('duration_seconds')]
        throughputs = [w.get('throughput', 0) for w in recent_workflows if w.get('throughput')]
        
        trends = {}
        
        if durations:
            recent_avg_duration = sum(durations[-5:]) / min(5, len(durations))
            older_avg_duration = sum(durations[:-5]) / max(1, len(durations) - 5)
            
            if older_avg_duration > 0:
                duration_trend = ((recent_avg_duration - older_avg_duration) / older_avg_duration) * 100
                trends['duration_trend_percent'] = duration_trend
        
        if throughputs:
            recent_avg_throughput = sum(throughputs[-5:]) / min(5, len(throughputs))
            older_avg_throughput = sum(throughputs[:-5]) / max(1, len(throughputs) - 5)
            
            if older_avg_throughput > 0:
                throughput_trend = ((recent_avg_throughput - older_avg_throughput) / older_avg_throughput) * 100
                trends['throughput_trend_percent'] = throughput_trend
        
        return trends

class AlertManager:
    """Manages alerts and notifications"""
    
    def __init__(self, smtp_config: Dict[str, Any] = None):
        self.alerts: List[Alert] = []
        self.alert_rules: List[Dict[str, Any]] = []
        self.notification_channels: Dict[str, Callable] = {}
        
        # SMTP configuration for email alerts
        self.smtp_config = smtp_config or {}
        
        # Alert statistics
        self.alert_stats = {
            'total_alerts': 0,
            'alerts_by_severity': defaultdict(int),
            'alerts_by_type': defaultdict(int),
            'acknowledged_alerts': 0,
            'resolved_alerts': 0
        }
        
        # Setup logging
        self.logger = logging.getLogger('alert_manager')
        
        # Initialize default notification channels
        self._initialize_notification_channels()
        
        self.logger.info("AlertManager initialized")
    
    def _initialize_notification_channels(self):
        """Initialize default notification channels"""
        self.notification_channels['log'] = self._log_notification
        
        if self.smtp_config:
            self.notification_channels['email'] = self._email_notification
    
    def add_alert_rule(self, rule: Dict[str, Any]):
        """Add alert rule"""
        self.alert_rules.append(rule)
        self.logger.info(f"Added alert rule: {rule.get('name', 'unnamed')}")
    
    def create_alert(self, alert_type: AlertType, severity: AlertSeverity,
                    title: str, message: str, source: str,
                    metadata: Dict[str, Any] = None) -> Alert:
        """Create new alert"""
        alert = Alert(
            alert_id=f"{alert_type.value}_{int(time.time())}",
            alert_type=alert_type,
            severity=severity,
            title=title,
            message=message,
            timestamp=datetime.now(),
            source=source,
            metadata=metadata or {}
        )
        
        self.alerts.append(alert)
        
        # Update statistics
        self.alert_stats['total_alerts'] += 1
        self.alert_stats['alerts_by_severity'][severity.value] += 1
        self.alert_stats['alerts_by_type'][alert_type.value] += 1
        
        # Send notifications
        self._send_notifications(alert)
        
        self.logger.info(f"Alert created: {alert.alert_id} ({severity.value})")
        
        return alert
    
    def _send_notifications(self, alert: Alert):
        """Send alert notifications"""
        for channel_name, channel_func in self.notification_channels.items():
            try:
                channel_func(alert)
            except Exception as e:
                self.logger.error(f"Failed to send notification via {channel_name}: {e}")
    
    def _log_notification(self, alert: Alert):
        """Log notification"""
        log_level = {
            AlertSeverity.INFO: logging.INFO,
            AlertSeverity.WARNING: logging.WARNING,
            AlertSeverity.CRITICAL: logging.CRITICAL,
            AlertSeverity.EMERGENCY: logging.CRITICAL
        }.get(alert.severity, logging.INFO)
        
        self.logger.log(log_level, f"ALERT [{alert.severity.value}] {alert.title}: {alert.message}")
    
    def _email_notification(self, alert: Alert):
        """Email notification"""
        if not self.smtp_config:
            return
        
        try:
            msg = MIMEMultipart()
            msg['From'] = self.smtp_config.get('from_email', 'alerts@example.com')
            msg['To'] = ', '.join(self.smtp_config.get('to_emails', []))
            msg['Subject'] = f"[{alert.severity.value}] {alert.title}"
            
            body = f"""
Alert Details:
- Alert ID: {alert.alert_id}
- Type: {alert.alert_type.value}
- Severity: {alert.severity.value}
- Source: {alert.source}
- Time: {alert.timestamp}

Message:
{alert.message}

Metadata:
{json.dumps(alert.metadata, indent=2)}
            """
            
            msg.attach(MIMEText(body, 'plain'))
            
            server = smtplib.SMTP(self.smtp_config['smtp_server'], self.smtp_config.get('smtp_port', 587))
            server.starttls()
            server.login(self.smtp_config['username'], self.smtp_config['password'])
            
            server.send_message(msg)
            server.quit()
            
        except Exception as e:
            self.logger.error(f"Failed to send email notification: {e}")
    
    def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an alert"""
        for alert in self.alerts:
            if alert.alert_id == alert_id:
                alert.acknowledged = True
                self.alert_stats['acknowledged_alerts'] += 1
                self.logger.info(f"Alert acknowledged: {alert_id}")
                return True
        
        return False
    
    def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an alert"""
        for alert in self.alerts:
            if alert.alert_id == alert_id:
                alert.resolved = True
                self.alert_stats['resolved_alerts'] += 1
                self.logger.info(f"Alert resolved: {alert_id}")
                return True
        
        return False
    
    def get_active_alerts(self) -> List[Alert]:
        """Get active (unresolved) alerts"""
        return [alert for alert in self.alerts if not alert.resolved]
    
    def get_alert_summary(self) -> Dict[str, Any]:
        """Get alert summary"""
        active_alerts = self.get_active_alerts()
        
        return {
            'total_alerts': len(self.alerts),
            'active_alerts': len(active_alerts),
            'alert_statistics': self.alert_stats,
            'recent_alerts': [alert.to_dict() for alert in self.alerts[-10:]],
            'active_critical_alerts': [
                alert.to_dict() for alert in active_alerts 
                if alert.severity in [AlertSeverity.CRITICAL, AlertSeverity.EMERGENCY]
            ]
        }

class SLAMonitor:
    """Service Level Agreement monitoring"""
    
    def __init__(self, alert_manager: AlertManager):
        self.alert_manager = alert_manager
        self.sla_definitions: Dict[str, SLADefinition] = {}
        self.sla_violations: List[Dict[str, Any]] = []
        self.monitoring = False
        self.monitor_thread: Optional[threading.Thread] = None
        
        # Setup logging
        self.logger = logging.getLogger('sla_monitor')
        
        self.logger.info("SLAMonitor initialized")
    
    def add_sla(self, sla: SLADefinition):
        """Add SLA definition"""
        self.sla_definitions[sla.name] = sla
        self.logger.info(f"Added SLA: {sla.name}")
    
    def check_sla_compliance(self, metric_name: str, metric_value: float, 
                           timestamp: datetime = None) -> bool:
        """Check if metric complies with SLA"""
        timestamp = timestamp or datetime.now()
        
        if metric_name not in self.sla_definitions:
            return True  # No SLA defined
        
        sla = self.sla_definitions[metric_name]
        threshold = sla.threshold_value
        operator = sla.threshold_operator
        
        # Check compliance
        compliant = False
        
        if operator == '<':
            compliant = metric_value < threshold
        elif operator == '<=':
            compliant = metric_value <= threshold
        elif operator == '>':
            compliant = metric_value > threshold
        elif operator == '>=':
            compliant = metric_value >= threshold
        elif operator == '==':
            compliant = metric_value == threshold
        elif operator == '!=':
            compliant = metric_value != threshold
        
        # Record violation if not compliant
        if not compliant:
            violation = {
                'sla_name': sla.name,
                'metric_value': metric_value,
                'threshold_value': threshold,
                'operator': operator,
                'timestamp': timestamp,
                'severity': 'HIGH' if metric_value > threshold * 1.5 else 'MEDIUM'
            }
            
            self.sla_violations.append(violation)
            
            # Create alert
            self.alert_manager.create_alert(
                alert_type=AlertType.SLA_BREACH,
                severity=AlertSeverity.CRITICAL if violation['severity'] == 'HIGH' else AlertSeverity.WARNING,
                title=f"SLA Breach: {sla.name}",
                message=f"Metric {metric_name} = {metric_value} violates SLA threshold {operator} {threshold}",
                source="sla_monitor",
                metadata=violation
            )
            
            self.logger.warning(f"SLA violation: {sla.name} ({metric_value} {operator} {threshold})")
        
        return compliant
    
    def get_sla_report(self) -> Dict[str, Any]:
        """Get SLA compliance report"""
        total_slas = len(self.sla_definitions)
        total_violations = len(self.sla_violations)
        
        # Calculate compliance rate
        compliance_rate = 100.0
        if total_violations > 0:
            # This is a simplified calculation - in reality you'd track total checks
            compliance_rate = max(0, 100 - (total_violations / max(total_slas, 1)) * 10)
        
        return {
            'total_slas': total_slas,
            'total_violations': total_violations,
            'compliance_rate_percent': compliance_rate,
            'sla_definitions': {name: {
                'description': sla.description,
                'threshold': f"{sla.threshold_operator} {sla.threshold_value}",
                'measurement_window_minutes': sla.measurement_window_minutes
            } for name, sla in self.sla_definitions.items()},
            'recent_violations': self.sla_violations[-10:],  # Last 10 violations
            'violations_by_sla': {
                sla_name: sum(1 for v in self.sla_violations if v['sla_name'] == sla_name)
                for sla_name in self.sla_definitions.keys()
            }
        }

class WorkflowMonitor:
    """Main workflow monitoring system"""
    
    def __init__(self, smtp_config: Dict[str, Any] = None):
        # Initialize components
        self.dashboard = PerformanceDashboard()
        self.alert_manager = AlertManager(smtp_config)
        self.sla_monitor = SLAMonitor(self.alert_manager)
        
        # Resource monitoring
        self.resource_monitor_thread: Optional[threading.Thread] = None
        self.monitoring = False
        
        # Setup logging
        self.logger = logging.getLogger('workflow_monitor')
        
        # Initialize default SLAs
        self._initialize_default_slas()
        
        # Start resource monitoring
        self.start_resource_monitoring()
        
        self.logger.info("WorkflowMonitor initialized")
    
    def _initialize_default_slas(self):
        """Initialize default SLA definitions"""
        # Workflow duration SLA
        self.sla_monitor.add_sla(SLADefinition(
            name="workflow_duration",
            description="Maximum workflow execution time",
            metric_type="duration",
            threshold_value=3600.0,  # 1 hour
            threshold_operator="<",
            measurement_window_minutes=60
        ))
        
        # Task failure rate SLA
        self.sla_monitor.add_sla(SLADefinition(
            name="task_failure_rate",
            description="Maximum task failure rate",
            metric_type="error_rate",
            threshold_value=5.0,  # 5%
            threshold_operator="<",
            measurement_window_minutes=60
        ))
        
        # System memory usage SLA
        self.sla_monitor.add_sla(SLADefinition(
            name="memory_usage",
            description="Maximum memory usage",
            metric_type="resource",
            threshold_value=85.0,  # 85%
            threshold_operator="<",
            measurement_window_minutes=15
        ))
    
    def start_resource_monitoring(self):
        """Start resource monitoring"""
        if self.monitoring:
            return
        
        self.monitoring = True
        self.resource_monitor_thread = threading.Thread(
            target=self._monitor_system_resources,
            daemon=True
        )
        self.resource_monitor_thread.start()
        
        self.logger.info("Resource monitoring started")
    
    def stop_resource_monitoring(self):
        """Stop resource monitoring"""
        self.monitoring = False
        
        if self.resource_monitor_thread:
            self.resource_monitor_thread.join(timeout=2.0)
        
        self.logger.info("Resource monitoring stopped")
    
    def _monitor_system_resources(self):
        """Monitor system resources"""
        while self.monitoring:
            try:
                # Get system metrics
                cpu_percent = psutil.cpu_percent(interval=1.0)
                memory = psutil.virtual_memory()
                disk = psutil.disk_usage('/')
                
                resource_metrics = {
                    'cpu_percent': cpu_percent,
                    'memory_percent': memory.percent,
                    'memory_used_mb': memory.used / (1024 * 1024),
                    'memory_available_mb': memory.available / (1024 * 1024),
                    'disk_percent': disk.percent,
                    'disk_used_gb': disk.used / (1024 * 1024 * 1024),
                    'disk_free_gb': disk.free / (1024 * 1024 * 1024)
                }
                
                # Update dashboard
                self.dashboard.update_resource_metrics(resource_metrics)
                
                # Check SLAs
                self.sla_monitor.check_sla_compliance('memory_usage', memory.percent)
                
                # Check for resource exhaustion
                if memory.percent > 90:
                    self.alert_manager.create_alert(
                        alert_type=AlertType.RESOURCE_EXHAUSTION,
                        severity=AlertSeverity.CRITICAL,
                        title="High Memory Usage",
                        message=f"Memory usage at {memory.percent:.1f}%",
                        source="resource_monitor",
                        metadata=resource_metrics
                    )
                
                if cpu_percent > 90:
                    self.alert_manager.create_alert(
                        alert_type=AlertType.RESOURCE_EXHAUSTION,
                        severity=AlertSeverity.WARNING,
                        title="High CPU Usage",
                        message=f"CPU usage at {cpu_percent:.1f}%",
                        source="resource_monitor",
                        metadata=resource_metrics
                    )
                
                time.sleep(10)  # Monitor every 10 seconds
                
            except Exception as e:
                self.logger.error(f"Resource monitoring error: {e}")
                time.sleep(5)
    
    def report_workflow_event(self, workflow_id: str, event_type: str, 
                             data: Dict[str, Any] = None):
        """Report workflow event"""
        data = data or {}
        
        # Update dashboard
        self.dashboard.update_workflow_metrics(workflow_id, {
            'event_type': event_type,
            'status': data.get('status', 'UNKNOWN'),
            'duration_seconds': data.get('duration_seconds', 0),
            'throughput': data.get('throughput', 0),
            **data
        })
        
        # Check for alerts
        if event_type == 'workflow_failed':
            self.alert_manager.create_alert(
                alert_type=AlertType.WORKFLOW_FAILURE,
                severity=AlertSeverity.CRITICAL,
                title=f"Workflow Failed: {workflow_id}",
                message=data.get('error_message', 'Workflow execution failed'),
                source="workflow_monitor",
                metadata=data
            )
        
        # Check SLAs
        if 'duration_seconds' in data:
            self.sla_monitor.check_sla_compliance('workflow_duration', data['duration_seconds'])
    
    def report_task_event(self, task_id: str, event_type: str, 
                         data: Dict[str, Any] = None):
        """Report task event"""
        data = data or {}
        
        # Update dashboard
        self.dashboard.update_task_metrics(task_id, {
            'event_type': event_type,
            'status': data.get('status', 'UNKNOWN'),
            'duration_seconds': data.get('duration_seconds', 0),
            **data
        })
        
        # Check for alerts
        if event_type == 'task_failed':
            self.alert_manager.create_alert(
                alert_type=AlertType.TASK_FAILURE,
                severity=AlertSeverity.WARNING,
                title=f"Task Failed: {task_id}",
                message=data.get('error_message', 'Task execution failed'),
                source="workflow_monitor",
                metadata=data
            )
    
    def get_monitoring_summary(self) -> Dict[str, Any]:
        """Get comprehensive monitoring summary"""
        return {
            'timestamp': datetime.now().isoformat(),
            'dashboard_data': self.dashboard.get_dashboard_data(),
            'alert_summary': self.alert_manager.get_alert_summary(),
            'sla_report': self.sla_monitor.get_sla_report(),
            'system_health': {
                'monitoring_active': self.monitoring,
                'overall_health_score': self._calculate_health_score()
            }
        }
    
    def _calculate_health_score(self) -> float:
        """Calculate overall system health score"""
        score = 100.0
        
        # Deduct for active critical alerts
        active_alerts = self.alert_manager.get_active_alerts()
        critical_alerts = [a for a in active_alerts if a.severity == AlertSeverity.CRITICAL]
        emergency_alerts = [a for a in active_alerts if a.severity == AlertSeverity.EMERGENCY]
        
        score -= len(critical_alerts) * 10
        score -= len(emergency_alerts) * 20
        
        # Deduct for SLA violations
        sla_report = self.sla_monitor.get_sla_report()
        score -= (100 - sla_report['compliance_rate_percent']) * 0.5
        
        # Deduct for high resource usage
        if self.dashboard.resource_metrics:
            latest_resources = self.dashboard.resource_metrics[-1]
            memory_percent = latest_resources.get('memory_percent', 0)
            cpu_percent = latest_resources.get('cpu_percent', 0)
            
            if memory_percent > 85:
                score -= (memory_percent - 85) * 2
            if cpu_percent > 85:
                score -= (cpu_percent - 85) * 1
        
        return max(0, min(100, score))
    
    def shutdown(self):
        """Shutdown monitoring system"""
        self.stop_resource_monitoring()
        self.logger.info("WorkflowMonitor shut down")
