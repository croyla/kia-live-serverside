import time
import traceback
from datetime import datetime
import asyncio
import threading
from contextlib import asynccontextmanager
from typing import Set, Dict
import os
import psutil

from aiohttp import TCPConnector

from src.shared import scheduled_timings, routes_children, routes_parent, start_times
from src.live_data_service.live_data_getter import fetch_route_data, _get_adaptive_connector_limits
from src.live_data_service.live_data_transformer import transform_response_to_feed_entities
from src.live_data_service.feed_entity_updater import update_feed_message
from src.shared.utils import generate_trip_id_timing_map


# Use a regular set with a lock for thread safety since we can't create weak references to integers
active_parents: Set[int] = set()
active_parents_lock = threading.Lock()

# Enhanced cache for trip maps with memory management
class TripMapCache:
    def __init__(self, max_size: int = 50, max_memory_mb: int = 25):
        self._cache: Dict[int, dict] = {}
        self._lock = threading.Lock()
        self._max_size = max_size
        self._max_memory_mb = max_memory_mb
        self._last_cleanup = time.time()
        self._cleanup_interval = 1800  # 30 minutes
        
    def _estimate_memory_usage(self) -> int:
        """Estimate memory usage of cache in bytes"""
        try:
            total_size = 0
            for parent_id, trip_map in self._cache.items():
                # Rough estimation
                total_size += len(str(parent_id)) + len(str(trip_map))
            return total_size
        except Exception:
            return 0
            
    def _should_cleanup(self) -> bool:
        """Check if cleanup is needed"""
        now = time.time()
        if now - self._last_cleanup < self._cleanup_interval:
            return False
            
        # Check memory pressure
        memory_mb = self._estimate_memory_usage() / (1024 * 1024)
        if memory_mb > self._max_memory_mb * 0.8:  # 80% threshold
            return True
            
        # Check system memory
        try:
            memory = psutil.virtual_memory()
            if memory.percent > 85:
                return True
        except Exception:
            pass
            
        return False
        
    def _cleanup(self):
        """Clean up cache if needed"""
        if not self._should_cleanup():
            return
            
        with self._lock:
            # Remove oldest entries if we're over size limit
            while len(self._cache) > self._max_size:
                oldest_key = next(iter(self._cache))
                del self._cache[oldest_key]
                
            # Remove entries if we're over memory limit
            while self._estimate_memory_usage() > self._max_memory_mb * 1024 * 1024:
                if len(self._cache) == 0:
                    break
                oldest_key = next(iter(self._cache))
                del self._cache[oldest_key]
                
            self._last_cleanup = time.time()
            
    def get(self, parent_id: int) -> dict:
        """Get trip map from cache"""
        with self._lock:
            self._cleanup()
            return self._cache.get(parent_id)
            
    def set(self, parent_id: int, trip_map: dict):
        """Set trip map in cache with memory management"""
        with self._lock:
            self._cleanup()
            self._cache[parent_id] = trip_map
            
    def pop(self, parent_id: int, default=None):
        """Remove and return trip map from cache"""
        with self._lock:
            return self._cache.pop(parent_id, default)
            
    def clear(self):
        """Clear all cache entries"""
        with self._lock:
            self._cache.clear()
            
    def size(self) -> int:
        """Get current cache size"""
        with self._lock:
            return len(self._cache)
            
    def memory_usage_mb(self) -> float:
        """Get current memory usage in MB"""
        with self._lock:
            return self._estimate_memory_usage() / (1024 * 1024)

# Use enhanced cache
trip_map_cache = TripMapCache(max_size=50, max_memory_mb=25)

def _get_adaptive_fetch_timeout():
    """Get fetch timeout scaled by hardware performance"""
    base_timeout = float(os.getenv("KIA_FETCH_TIMEOUT", "120"))
    
    # Import here to avoid circular imports
    try:
        from src.live_data_service.live_data_getter import _detect_hardware_performance
        perf_mult = _detect_hardware_performance()
        
        # Slower hardware gets longer timeouts
        if perf_mult < 1.0:
            scale_factor = 1.0 / perf_mult
            timeout = int(base_timeout * scale_factor)
            print(f"[Receiver] Hardware scaling: {perf_mult:.2f}x → {scale_factor:.2f}x longer fetch timeout")
        else:
            timeout = int(base_timeout)
            
        # Ensure reasonable bounds
        timeout = max(60, min(timeout, 600))  # between 1-10 minutes
        print(f"[Receiver] Fetch timeout: {timeout}s")
        return timeout
        
    except Exception as e:
        print(f"[Receiver] Hardware detection failed: {e}, using default {base_timeout}s")
        return int(base_timeout)

def _get_processing_semaphore():
    """Get semaphore to limit concurrent processing tasks"""
    try:
        from src.live_data_service.live_data_getter import _calculate_resource_allocation
        allocation = _calculate_resource_allocation()
        # Increase processing capacity significantly to reduce delays
        max_processing = min(allocation['max_processing_concurrent'] * 2, 20)  # Double capacity, cap at 20
        
        # Environment override
        max_processing = int(os.getenv("KIA_MAX_PROCESSING_CONCURRENT", max_processing))
        
        print(f"[Processing] Max concurrent processing tasks: {max_processing}")
        return asyncio.Semaphore(max_processing)
        
    except Exception as e:
        print(f"[Processing] Resource detection failed: {e}, using increased default semaphore(10)")
        return asyncio.Semaphore(10)  # Increased default

FETCH_TIMEOUT = _get_adaptive_fetch_timeout()
PROCESSING_SEMAPHORE = _get_processing_semaphore()

@asynccontextmanager
async def managed_polling(parent_id: int):
    """Context manager to ensure proper cleanup of active_parents"""
    try:
        with active_parents_lock:
            active_parents.add(parent_id)
        yield
    finally:
        with active_parents_lock:
            active_parents.discard(parent_id)

async def cleanup_trip_map_cache():
    """Periodically cleanup the trip map cache and monitor memory"""
    while True:
        await asyncio.sleep(1800)  # 30 minutes
        
        # Log cache statistics
        cache_size = trip_map_cache.size()
        cache_memory = trip_map_cache.memory_usage_mb()
        print(f"[Cache] Size: {cache_size}, Memory: {cache_memory:.1f}MB")
        
        # Force cleanup if needed
        if cache_memory > 20:  # Force cleanup if over 20MB
            print(f"[Cache] Forcing cleanup due to memory usage: {cache_memory:.1f}MB")
            trip_map_cache.clear()
            
        # System memory check
        try:
            memory = psutil.virtual_memory()
            if memory.percent > 90:
                print(f"[Cache] System memory high ({memory.percent:.1f}%), clearing cache")
                trip_map_cache.clear()
        except Exception:
            pass

async def live_data_receiver_loop():
    """
    Consumes scheduled_timings queue and starts polling tasks for each unique parent_id.
    Ensures only one polling task per parent_id at a time.
    """
    # Use adaptive connector limits based on hardware
    connector_config = _get_adaptive_connector_limits()
    shared_connector = TCPConnector(**connector_config)
    
    print(f"[Receiver] Using connector: {connector_config}")
    
    # Start cache cleanup task
    asyncio.create_task(cleanup_trip_map_cache())
    
    while True:
        try:
            if scheduled_timings.empty():
                await asyncio.sleep(1)
                continue

            scheduled_time, job = scheduled_timings.queue[0]  # Peek without removing
            now = datetime.now()

            if now.timestamp() >= scheduled_time.timestamp():
                _, job = scheduled_timings.get()  # Now remove
                parent_id = job["parent_id"]

                with active_parents_lock:
                    if parent_id in active_parents:
                        continue  # Already polling this parent
                time.sleep(0.5) # Sleep for 100ms before scheduling an API query
                asyncio.create_task(poll_route_parent_until_done(parent_id, shared_connector))
            else:
                await asyncio.sleep(1)
        except Exception as e:
            print(f"Error in live_data_receiver_loop: {e}")
        finally:
            await asyncio.sleep(0.001)

async def poll_route_parent_until_done(parent_id: int, connector):
    """
    Polls the BMTC API every 20s for a given route parent_id.
    Stops after 2 consecutive polls return no matching live trip data.
    """
    async with managed_polling(parent_id):
        # Get or create trip map from cache
        trip_map = trip_map_cache.get(parent_id)
        if trip_map is None:
            child_routes = routes_children.as_dict()
            start_time_data = start_times.as_dict()
            trip_map = generate_trip_id_timing_map(start_time_data, child_routes)
            trip_map_cache.set(parent_id, trip_map)
        print(f"[Polling] Started polling for parent_id={parent_id}")
        empty_tries = 0
        MAX_EMPTY_TRIES = 10

        try:
            while True:
                try:
                    # Use built-in timeout handling instead of wait_for to avoid task cancellation
                    data = await fetch_route_data(parent_id, connector=connector)

                    if not data:
                        print(f"[Polling] [{datetime.now().strftime('%d-%m %H:%M:%S')}] No data for parent_id={parent_id}")
                        empty_tries += 1
                    else:
                        # Use semaphore to limit concurrent processing
                        async with PROCESSING_SEMAPHORE:
                            matching_jobs = []
                            for route_key, child_id in routes_children.items():
                                if routes_parent.get(route_key) != parent_id:
                                    continue

                                for trip_entry in trip_map.get(route_key, []):
                                    matching_jobs.append({
                                        "trip_id": trip_entry["trip"],
                                        "trip_time": datetime.strptime(trip_entry['start'], "%H:%M:%S"),
                                        "route_id": str(child_id),
                                        "parent_id": parent_id
                                    })

                            found_match = False
                            all_entities = []

                            for job in matching_jobs:
                                entities = transform_response_to_feed_entities(data, job)
                                if entities:
                                    found_match = True
                                    all_entities.extend(entities)

                            if found_match:
                                update_feed_message(all_entities)
                                empty_tries = 0
                            else:
                                print(f"FAILED TO UPDATE FEED MESSAGE NO MATCHES FOUND {all_entities}")
                                empty_tries += 1

                    if empty_tries >= MAX_EMPTY_TRIES:
                        print(f"[Polling] [{datetime.now().strftime('%d-%m %H:%M:%S')}] No matches after {MAX_EMPTY_TRIES} tries. Stopping {parent_id}.")
                        break

                    await asyncio.sleep(30) # sleep for 30 seconds
                except asyncio.CancelledError:
                    # Task was cancelled, exit gracefully
                    print(f"[Polling] Task cancelled for parent_id={parent_id}")
                    break
                except asyncio.TimeoutError:
                    traceback.print_exc()
                    print(f"[Polling] Timeout while fetching data for parent_id={parent_id}")
                    await asyncio.sleep(5)
                    empty_tries += 1
                except Exception as e:
                    traceback.print_exc()
                    print(f"[Polling] Error while polling parent_id={parent_id}: {e}")
                    await asyncio.sleep(5)
                    empty_tries += 1
        finally:
            # Cleanup cache when done with this parent_id
            trip_map_cache.pop(parent_id)
