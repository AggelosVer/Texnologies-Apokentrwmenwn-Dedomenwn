import time
from typing import Dict, List, Optional
from collections import defaultdict
from dataclasses import dataclass, field
import statistics


@dataclass
class OperationMetrics:
    operation_name: str
    message_count: int = 0
    total_latency: float = 0.0
    latencies: List[float] = field(default_factory=list)
    success_count: int = 0
    error_count: int = 0
    total_bytes_sent: int = 0
    total_bytes_received: int = 0
    
    @property
    def average_latency(self) -> float:
        if self.message_count == 0:
            return 0.0
        return (self.total_latency / self.message_count) * 1000
    
    @property
    def median_latency(self) -> float:
        if not self.latencies:
            return 0.0
        return statistics.median(self.latencies) * 1000
    
    @property
    def min_latency(self) -> float:
        if not self.latencies:
            return 0.0
        return min(self.latencies) * 1000
    
    @property
    def max_latency(self) -> float:
        if not self.latencies:
            return 0.0
        return max(self.latencies) * 1000
    
    @property
    def success_rate(self) -> float:
        total = self.success_count + self.error_count
        if total == 0:
            return 0.0
        return (self.success_count / total) * 100
    
    def add_measurement(self, latency: float, success: bool, bytes_sent: int = 0, bytes_received: int = 0):
        self.message_count += 1
        self.total_latency += latency
        self.latencies.append(latency)
        
        if success:
            self.success_count += 1
        else:
            self.error_count += 1
        
        self.total_bytes_sent += bytes_sent
        self.total_bytes_received += bytes_received
    
    def to_dict(self) -> Dict:
        return {
            'operation': self.operation_name,
            'message_count': self.message_count,
            'avg_latency_ms': round(self.average_latency, 2),
            'median_latency_ms': round(self.median_latency, 2),
            'min_latency_ms': round(self.min_latency, 2),
            'max_latency_ms': round(self.max_latency, 2),
            'success_count': self.success_count,
            'error_count': self.error_count,
            'success_rate': round(self.success_rate, 2),
            'total_bytes_sent': self.total_bytes_sent,
            'total_bytes_received': self.total_bytes_received
        }


class NetworkMetrics:
    def __init__(self, node_address: str):
        self.node_address = node_address
        self.operation_metrics: Dict[str, OperationMetrics] = defaultdict(
            lambda: OperationMetrics(operation_name="unknown")
        )
        self.start_time = time.time()
        self.total_messages_sent = 0
        self.total_messages_received = 0
        self.active_requests: Dict[str, float] = {}
    
    def start_request(self, request_id: str, operation: str):
        self.active_requests[request_id] = time.time()
        self.total_messages_sent += 1
    
    def complete_request(
        self,
        request_id: str,
        operation: str,
        success: bool = True,
        bytes_sent: int = 0,
        bytes_received: int = 0
    ):
        if request_id not in self.active_requests:
            return
        
        start_time = self.active_requests.pop(request_id)
        latency = time.time() - start_time
        
        if operation not in self.operation_metrics:
            self.operation_metrics[operation] = OperationMetrics(operation_name=operation)
        
        self.operation_metrics[operation].add_measurement(
            latency=latency,
            success=success,
            bytes_sent=bytes_sent,
            bytes_received=bytes_received
        )
        
        self.total_messages_received += 1
    
    def record_message_sent(self, operation: str, bytes_sent: int = 0):
        self.total_messages_sent += 1
        if operation in self.operation_metrics:
            self.operation_metrics[operation].total_bytes_sent += bytes_sent
    
    def record_message_received(self, operation: str, bytes_received: int = 0):
        self.total_messages_received += 1
        if operation in self.operation_metrics:
            self.operation_metrics[operation].total_bytes_received += bytes_received
    
    def get_operation_metrics(self, operation: str) -> Optional[OperationMetrics]:
        return self.operation_metrics.get(operation)
    
    def get_all_metrics(self) -> Dict[str, OperationMetrics]:
        return dict(self.operation_metrics)
    
    def get_summary(self) -> Dict:
        elapsed_time = time.time() - self.start_time
        
        total_latency = sum(m.total_latency for m in self.operation_metrics.values())
        total_operations = sum(m.message_count for m in self.operation_metrics.values())
        total_success = sum(m.success_count for m in self.operation_metrics.values())
        total_errors = sum(m.error_count for m in self.operation_metrics.values())
        total_bytes_sent = sum(m.total_bytes_sent for m in self.operation_metrics.values())
        total_bytes_received = sum(m.total_bytes_received for m in self.operation_metrics.values())
        
        return {
            'node_address': self.node_address,
            'elapsed_time_seconds': round(elapsed_time, 2),
            'total_messages_sent': self.total_messages_sent,
            'total_messages_received': self.total_messages_received,
            'total_operations': total_operations,
            'total_success': total_success,
            'total_errors': total_errors,
            'overall_success_rate': round((total_success / max(total_operations, 1)) * 100, 2),
            'average_latency_ms': round((total_latency / max(total_operations, 1)) * 1000, 2),
            'total_bytes_sent': total_bytes_sent,
            'total_bytes_received': total_bytes_received,
            'throughput_msgs_per_sec': round(total_operations / max(elapsed_time, 0.001), 2),
            'operations': {op: metrics.to_dict() for op, metrics in self.operation_metrics.items()}
        }
    
    def reset(self):
        self.operation_metrics.clear()
        self.start_time = time.time()
        self.total_messages_sent = 0
        self.total_messages_received = 0
        self.active_requests.clear()
