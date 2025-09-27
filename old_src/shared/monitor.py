"""
Performance monitoring and optimization recommendations
"""
import time
import psutil
import threading
from typing import Dict, List
from datetime import datetime, timedelta

class PerformanceMonitor:
    def __init__(self, interval: int = 60):
        self.interval = interval
        self.running = False
        self.monitor_thread = None
        self.history: List[Dict] = []
        self.max_history = 100  # Keep last 100 measurements
        
    def start(self):
        """Start monitoring in background thread"""
        if self.running:
            return
            
        self.running = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        print(f"[Monitor] Started performance monitoring (interval: {self.interval}s)")
        
    def stop(self):
        """Stop monitoring"""
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5.0)
        print("[Monitor] Stopped performance monitoring")
        
    def _monitor_loop(self):
        """Main monitoring loop"""
        while self.running:
            try:
                snapshot = self._take_snapshot()
                self.history.append(snapshot)
                
                # Keep only recent history
                if len(self.history) > self.max_history:
                    self.history.pop(0)
                    
                # Check for performance issues
                self._check_performance_issues(snapshot)
                
                time.sleep(self.interval)
                
            except Exception as e:
                print(f"[Monitor] Error in monitoring loop: {e}")
                time.sleep(self.interval)
                
    def _take_snapshot(self) -> Dict:
        """Take a performance snapshot"""
        try:
            # System metrics
            memory = psutil.virtual_memory()
            cpu_percent = psutil.cpu_percent(interval=1)
            disk = psutil.disk_usage('/')
            
            # Process metrics
            process = psutil.Process()
            process_memory = process.memory_info()
            process_cpu = process.cpu_percent()
            
            snapshot = {
                'timestamp': datetime.now(),
                'system': {
                    'memory_percent': memory.percent,
                    'memory_available_gb': memory.available / (1024**3),
                    'cpu_percent': cpu_percent,
                    'disk_percent': disk.percent
                },
                'process': {
                    'memory_mb': process_memory.rss / (1024**2),
                    'memory_percent': process.memory_percent(),
                    'cpu_percent': process_cpu,
                    'num_threads': process.num_threads()
                }
            }
            
            return snapshot
            
        except Exception as e:
            print(f"[Monitor] Error taking snapshot: {e}")
            return {'timestamp': datetime.now(), 'error': str(e)}
            
    def _check_performance_issues(self, snapshot: Dict):
        """Check for performance issues and provide recommendations"""
        if 'error' in snapshot:
            return
            
        issues = []
        recommendations = []
        
        # Memory issues
        if snapshot['system']['memory_percent'] > 90:
            issues.append("System memory usage is very high (>90%)")
            recommendations.append("Consider reducing cache sizes or increasing system memory")
            
        if snapshot['process']['memory_percent'] > 80:
            issues.append("Process memory usage is very high (>80%)")
            recommendations.append("Check for memory leaks in data structures")
            
        # CPU issues
        if snapshot['system']['cpu_percent'] > 90:
            issues.append("System CPU usage is very high (>90%)")
            recommendations.append("Consider reducing concurrent tasks or optimizing processing")
            
        # Disk issues
        if snapshot['system']['disk_percent'] > 90:
            issues.append("Disk usage is very high (>90%)")
            recommendations.append("Clean up old log files and temporary data")
            
        # Report issues
        if issues:
            print(f"\n[Monitor] Performance issues detected at {snapshot['timestamp']}:")
            for issue in issues:
                print(f"  - {issue}")
            for rec in recommendations:
                print(f"  - Recommendation: {rec}")
                
    def get_current_stats(self) -> Dict:
        """Get current performance statistics"""
        if not self.history:
            return {}
            
        latest = self.history[-1]
        if 'error' in latest:
            return latest
            
        # Calculate trends
        if len(self.history) >= 2:
            prev = self.history[-2]
            if 'error' not in prev:
                latest['trends'] = {
                    'memory_change_mb': latest['process']['memory_mb'] - prev['process']['memory_mb'],
                    'cpu_change': latest['process']['cpu_percent'] - prev['process']['cpu_percent']
                }
                
        return latest
        
    def get_summary(self) -> Dict:
        """Get performance summary and recommendations"""
        if not self.history:
            return {'status': 'No data available'}
            
        # Calculate averages
        valid_snapshots = [s for s in self.history if 'error' not in s]
        if not valid_snapshots:
            return {'status': 'No valid data available'}
            
        avg_memory = sum(s['process']['memory_mb'] for s in valid_snapshots) / len(valid_snapshots)
        avg_cpu = sum(s['process']['cpu_percent'] for s in valid_snapshots) / len(valid_snapshots)
        avg_system_memory = sum(s['system']['memory_percent'] for s in valid_snapshots) / len(valid_snapshots)
        
        # Determine health status
        if avg_system_memory > 85 or avg_memory > 200:
            health = "Poor"
        elif avg_system_memory > 70 or avg_memory > 150:
            health = "Fair"
        else:
            health = "Good"
            
        return {
            'status': 'Monitoring active',
            'health': health,
            'averages': {
                'memory_mb': round(avg_memory, 1),
                'cpu_percent': round(avg_cpu, 1),
                'system_memory_percent': round(avg_system_memory, 1)
            },
            'current': self.get_current_stats(),
            'recommendations': self._get_recommendations(avg_memory, avg_cpu, avg_system_memory)
        }
        
    def _get_recommendations(self, avg_memory: float, avg_cpu: float, avg_system_memory: float) -> List[str]:
        """Get optimization recommendations based on current performance"""
        recommendations = []
        
        if avg_memory > 200:
            recommendations.append("Process memory usage is high - consider reducing cache sizes")
            
        if avg_cpu > 80:
            recommendations.append("CPU usage is high - consider reducing concurrent processing tasks")
            
        if avg_system_memory > 85:
            recommendations.append("System memory pressure is high - consider upgrading RAM or reducing workload")
            
        if not recommendations:
            recommendations.append("Performance is within acceptable ranges")
            
        return recommendations

# Global monitor instance
performance_monitor = PerformanceMonitor()

def start_monitoring(interval: int = 60):
    """Start performance monitoring"""
    performance_monitor.interval = interval
    performance_monitor.start()
    
def stop_monitoring():
    """Stop performance monitoring"""
    performance_monitor.stop()
    
def get_performance_summary() -> Dict:
    """Get current performance summary"""
    return performance_monitor.get_summary()
