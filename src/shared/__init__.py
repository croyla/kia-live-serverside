import time
from threading import RLock
from queue import PriorityQueue
from google.transit import gtfs_realtime_pb2
from collections import OrderedDict
from datetime import datetime, timedelta


class ThreadSafeDict:
    def __init__(self, max_size=1000, max_age_seconds=3600):
        self._data = OrderedDict()  # Use OrderedDict for FIFO behavior
        self._lock = RLock()
        self._max_size = max_size
        self._max_age = max_age_seconds
        self._last_cleanup = time.time()
        self._cleanup_interval = 300  # Cleanup every 5 minutes

    def _cleanup_old_entries(self):
        """Remove entries that are too old"""
        now = time.time()
        if now - self._last_cleanup < self._cleanup_interval:
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
            self._last_cleanup = now

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
            # Enforce size limit by removing oldest entries
            while len(self._data) >= self._max_size:
                print('Clearing an item from ThreadSafeDict')
                self._data.popitem(last=False)  # Remove oldest item (FIFO)
            self._data[key] = (value, time.time())
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

# Thread-safe shared dicts with size and age limits
routes_children = ThreadSafeDict(max_size=64, max_age_seconds=24*3600)  # 24 hour retention
routes_parent = ThreadSafeDict(max_size=64, max_age_seconds=24*3600)  # 24 hour retention
start_times = ThreadSafeDict(max_size=1000, max_age_seconds=24*3600)  # 24 hour retention
times = ThreadSafeDict(max_size=1000, max_age_seconds=24*3600)  # 24 hour retention
stop_times = ThreadSafeDict(max_size=1000, max_age_seconds=48*3600)  # 48 hour retention
prediction_cache = ThreadSafeDict(max_size=200, max_age_seconds=1*3600) # 1 hour retention
