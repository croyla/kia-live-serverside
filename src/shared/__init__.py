import time
import gc
import psutil
import sys
from threading import RLock
from queue import PriorityQueue
from google.transit import gtfs_realtime_pb2
from datetime import datetime, timedelta
from typing import Dict, Any


class CacheEntry:
    """Memory-efficient cache entry using __slots__"""
    __slots__ = ['value', 'timestamp', 'access_count']
    
    def __init__(self, value: Any, timestamp: float, access_count: int = 0):
        self.value = value
        self.timestamp = timestamp
        self.access_count = access_count


class ThreadSafeDict:
    """Memory-optimized thread-safe dictionary with accurate memory tracking"""
    __slots__ = ['_data', '_lock', '_max_size', '_max_age', '_max_memory_mb', 
                 '_last_cleanup', '_cleanup_interval', '_memory_threshold']
    
    def __init__(self, max_size=1000, max_age_seconds=3600, max_memory_mb=100):
        self._data: Dict[Any, CacheEntry] = {}  # Use regular dict (Python 3.7+ preserves insertion order)
        self._lock = RLock()
        self._max_size = max_size
        self._max_age = max_age_seconds
        self._max_memory_mb = max_memory_mb
        self._last_cleanup = time.time()
        self._cleanup_interval = 300  # Cleanup every 5 minutes
        self._memory_threshold = 0.8  # Trigger cleanup at 80% of max memory

    def _estimate_memory_usage(self) -> int:
        """Accurate deep memory usage calculation using pickle.dumps"""
        try:
            import pickle
            return len(pickle.dumps(self._data))
        except Exception:
            # Enhanced fallback with aggressive multipliers for complex nested objects
            try:
                total_size = sys.getsizeof(self._data) * 3  # Conservative multiplier
                for key, entry in self._data.items():
                    total_size += sys.getsizeof(key) * 2
                    total_size += sys.getsizeof(entry) * 2
                    # Apply larger multiplier for complex values (protobuf, dicts, etc.)
                    value_size = sys.getsizeof(entry.value)
                    if hasattr(entry.value, '__dict__') or isinstance(entry.value, (dict, list)):
                        value_size *= 5  # Much larger multiplier for complex objects
                    total_size += value_size
                return total_size
            except Exception:
                return len(self._data) * 1024  # Very conservative fallback: 1KB per entry

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
                k for k, entry in self._data.items()
                if current_time - entry.timestamp > self._max_age
            ]
            for k in keys_to_remove:
                del self._data[k]
            
            # Force garbage collection if we removed many items
            if len(keys_to_remove) > 100:
                gc.collect()
                
            self._last_cleanup = now

    def _enforce_memory_limits(self):
        """Enforce memory limits by removing least recently used entries"""
        while len(self._data) >= self._max_size:
            print(f'Clearing an item from ThreadSafeDict (size: {len(self._data)})')
            # Find least recently used entry
            if self._data:
                lru_key = min(self._data.keys(), key=lambda k: self._data[k].access_count)
                del self._data[lru_key]
            else:
                break
            
        # Check memory usage and remove more if needed
        while self._estimate_memory_usage() > self._max_memory_mb * 1024 * 1024:
            if len(self._data) == 0:
                break
            print(f'Memory limit reached, clearing item from ThreadSafeDict')
            lru_key = min(self._data.keys(), key=lambda k: self._data[k].access_count)
            del self._data[lru_key]

    def __iter__(self):
        with self._lock:
            return iter([(k, entry.value) for k, entry in self._data.items()])

    def __len__(self):
        with self._lock:
            return len(self._data)

    def get(self, key, default=None):
        with self._lock:
            if key in self._data:
                entry = self._data[key]
                entry.access_count += 1
                return entry.value
            return default

    def items(self):
        with self._lock:
            return [(k, entry.value) for k, entry in self._data.items()]

    def keys(self):
        with self._lock:
            return list(self._data.keys())

    def values(self):
        with self._lock:
            return [entry.value for entry in self._data.values()]

    def __getitem__(self, key):
        with self._lock:
            entry = self._data[key]
            entry.access_count += 1
            return entry.value

    def __setitem__(self, key, value):
        with self._lock:
            self._data[key] = CacheEntry(value=value, timestamp=time.time(), access_count=0)
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
            return {k: entry.value for k, entry in self._data.items()}

    def pop(self, key, default=None):
        with self._lock:
            if key in self._data:
                entry = self._data.pop(key)
                return entry.value
            return default

    def __delitem__(self, key):
        with self._lock:
            del self._data[key]

    def __contains__(self, key):
        with self._lock:
            return key in self._data

    def memory_usage_mb(self) -> float:
        """Get current memory usage in MB"""
        with self._lock:
            return self._estimate_memory_usage() / (1024 * 1024)


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
pred_seg_cache = ThreadSafeDict(max_size=800, max_age_seconds=120*60, max_memory_mb=10) # 1 hour 30 minute retention, 10MB limit
