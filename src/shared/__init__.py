import time
import gc
import psutil
from threading import RLock
from queue import PriorityQueue
from google.transit import gtfs_realtime_pb2
from collections import OrderedDict
from datetime import datetime, timedelta


class ThreadSafeDict:
    def __init__(self, max_size=1000, max_age_seconds=3600, max_memory_mb=100):
        self._data = OrderedDict()  # Use OrderedDict for FIFO behavior
        self._lock = RLock()
        self._max_size = max_size
        self._max_age = max_age_seconds
        self._max_memory_mb = max_memory_mb
        self._last_cleanup = time.time()
        self._cleanup_interval = 300  # Cleanup every 5 minutes
        self._memory_threshold = 0.8  # Trigger cleanup at 80% of max memory

    def _estimate_memory_usage(self) -> int:
        """Estimate memory usage of the data structure in bytes"""
        try:
            total_size = 0
            for key, (value, timestamp) in self._data.items():
                # Rough estimation: key + value + timestamp
                total_size += len(str(key)) + len(str(value)) + 8  # 8 bytes for timestamp
            return total_size
        except Exception:
            return 0

    def _should_force_cleanup(self) -> bool:
        """Check if we should force cleanup due to memory pressure"""
        try:
            # Check system memory usage
            memory = psutil.virtual_memory()
            if memory.percent > 85:  # System memory > 85%
                return True
                
            # Check our own memory usage
            our_memory_mb = self._estimate_memory_usage() / (1024 * 1024)
            if our_memory_mb > self._max_memory_mb * self._memory_threshold:
                return True
                
            return False
        except Exception:
            return False

    def _cleanup_old_entries(self):
        """Remove entries that are too old or if memory pressure is high"""
        now = time.time()
        should_cleanup = (
            now - self._last_cleanup >= self._cleanup_interval or
            self._should_force_cleanup()
        )
        
        if not should_cleanup:
            return
            
        with self._lock:
            current_time = time.time()
            # Remove old entries
            keys_to_remove = [
                k for k, (v, timestamp) in self._data.items()
                if current_time - timestamp > self._max_age
            ]
            for k in keys_to_remove:
                del self._data[k]
            
            # Force garbage collection if we removed many items
            if len(keys_to_remove) > 100:
                gc.collect()
                
            self._last_cleanup = now

    def _enforce_memory_limits(self):
        """Enforce memory limits by removing oldest entries"""
        while len(self._data) >= self._max_size:
            print(f'Clearing an item from ThreadSafeDict (size: {len(self._data)})')
            self._data.popitem(last=False)  # Remove oldest item (FIFO)
            
        # Check memory usage and remove more if needed
        while self._estimate_memory_usage() > self._max_memory_mb * 1024 * 1024:
            if len(self._data) == 0:
                break
            print(f'Memory limit reached, clearing item from ThreadSafeDict')
            self._data.popitem(last=False)

    def __iter__(self):
        with self._lock:
            return iter([(k, v) for k, (v, _) in self._data.items()])

    def __len__(self):
        with self._lock:
            return len(self._data)

    def get(self, key, default=None):
        with self._lock:
            if key in self._data:
                value, _ = self._data[key]
                return value
            return default

    def items(self):
        with self._lock:
            return [(k, v) for k, (v, _) in self._data.items()]

    def keys(self):
        with self._lock:
            return list(self._data.keys())

    def values(self):
        with self._lock:
            return [v for v, _ in self._data.values()]

    def __getitem__(self, key):
        with self._lock:
            value, _ = self._data[key]
            return value

    def __setitem__(self, key, value):
        with self._lock:
            self._data[key] = (value, time.time())
            self._enforce_memory_limits()
            self._cleanup_old_entries()

    def clear(self):
        with self._lock:
            self._data.clear()

    def update(self, new_data):
        with self._lock:
            for k, v in new_data.items():
                self[k] = v  # Use __setitem__ to maintain size limits

    def as_dict(self):
        with self._lock:
            return {k: v for k, (v, _) in self._data.items()}

    def pop(self, key, default=None):
        with self._lock:
            if key in self._data:
                value, _ = self._data.pop(key)
                return value
            return default

    def __contains__(self, key):
        with self._lock:
            return key in self._data


# Thread-safe data stores with size limits
scheduled_timings = PriorityQueue(maxsize=1000)  # Limit queue size
feed_message = gtfs_realtime_pb2.FeedMessage()
feed_message_lock = RLock()
with feed_message_lock:
    feed_message.header.gtfs_realtime_version = "2.0"
    feed_message.header.timestamp = int(time.time())

# Thread-safe shared dicts with size, age, and memory limits
routes_children = ThreadSafeDict(max_size=64, max_age_seconds=24*3600, max_memory_mb=10)  # 24 hour retention, 10MB limit
routes_parent = ThreadSafeDict(max_size=64, max_age_seconds=24*3600, max_memory_mb=10)  # 24 hour retention, 10MB limit
start_times = ThreadSafeDict(max_size=1000, max_age_seconds=24*3600, max_memory_mb=20)  # 24 hour retention, 20MB limit
times = ThreadSafeDict(max_size=1000, max_age_seconds=24*3600, max_memory_mb=20)  # 24 hour retention, 20MB limit
stop_times = ThreadSafeDict(max_size=1000, max_age_seconds=48*3600, max_memory_mb=30)  # 48 hour retention, 30MB limit
route_stops = ThreadSafeDict(max_size=64, max_age_seconds=48*3600, max_memory_mb=15)  # 48 hour retention, 15MB limit
prediction_cache = ThreadSafeDict(max_size=200, max_age_seconds=1*3600, max_memory_mb=25) # 1 hour retention, 25MB limit
