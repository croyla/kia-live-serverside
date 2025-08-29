import os
import traceback
import asyncio
import time
import random
import json
import multiprocessing
import psutil

import aiohttp

KIA_API_BASE = os.getenv("KIA_BMTC_API_URL", "https://bmtcmobileapi.karnataka.gov.in/WebAPI")
HEADERS = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Content-Type': 'application/json',
    'User-Agent': 'insomnia/1.0.0',
    'lan': 'en',
    'deviceType': 'WEB',
    'Connection': 'close',
    'Accept-Encoding': 'identity'
}
_MIN_GAP_S = 2.2

def _detect_hardware_performance():
    """Detect hardware capabilities and return performance multiplier"""
    try:
        # CPU cores and frequency
        cpu_count = multiprocessing.cpu_count()
        cpu_freq = psutil.cpu_freq()
        cpu_mhz = cpu_freq.current if cpu_freq else 2000  # fallback
        
        # Memory
        memory = psutil.virtual_memory()
        memory_gb = memory.total / (1024**3)
        
        # Calculate performance score (higher = faster hardware)
        cpu_score = min(cpu_count * (cpu_mhz / 2000), 8.0)  # cap at 8x
        memory_score = min(memory_gb / 4.0, 4.0)  # cap at 4x
        performance_multiplier = (cpu_score + memory_score) / 2.0
        
        #print(f"[Hardware] CPU: {cpu_count} cores @ {cpu_mhz:.0f}MHz, RAM: {memory_gb:.1f}GB, Performance: {performance_multiplier:.2f}x")
        return max(0.5, min(performance_multiplier, 3.0))  # clamp between 0.5x and 3x
        
    except Exception as e:
        print(f"[Hardware] Detection failed: {e}, using default 1.0x")
        return 1.0

def _calculate_resource_allocation():
    """Calculate optimal resource allocation for network vs processing"""
    try:
        cpu_count = multiprocessing.cpu_count()
        memory_gb = psutil.virtual_memory().total / (1024**3)
        
        # Reserve cores for network I/O (minimum 1, maximum 2)
        network_cores = min(2, max(1, cpu_count // 4))
        
        # Reserve cores for data processing (remaining cores)
        processing_cores = max(1, cpu_count - network_cores)
        
        # Calculate memory allocation (reserve 25% for network operations)
        network_memory_gb = memory_gb * 0.25
        processing_memory_gb = memory_gb * 0.75
        
        # Calculate concurrency limits
        max_network_concurrent = network_cores * 2  # 2 tasks per network core
        max_processing_concurrent = processing_cores * 3  # 3 tasks per processing core
        
        allocation = {
            'network_cores': network_cores,
            'processing_cores': processing_cores,
            'network_memory_gb': network_memory_gb,
            'processing_memory_gb': processing_memory_gb,
            'max_network_concurrent': max_network_concurrent,
            'max_processing_concurrent': max_processing_concurrent,
            'total_cores': cpu_count,
            'total_memory_gb': memory_gb
        }
        
        print(f"[Resource] Network: {network_cores} cores, {network_memory_gb:.1f}GB, {max_network_concurrent} concurrent")
        print(f"[Resource] Processing: {processing_cores} cores, {processing_memory_gb:.1f}GB, {max_processing_concurrent} concurrent")
        
        return allocation
        
    except Exception as e:
        print(f"[Resource] Detection failed: {e}, using conservative defaults")
        return {
            'network_cores': 1,
            'processing_cores': 1,
            'network_memory_gb': 1.0,
            'processing_memory_gb': 2.0,
            'max_network_concurrent': 2,
            'max_processing_concurrent': 3,
            'total_cores': 2,
            'total_memory_gb': 3.0
        }

def _get_adaptive_timeouts():
    """Get timeouts scaled by hardware performance and environment overrides"""
    # Environment variable overrides (for manual tuning)
    connect_timeout = float(os.getenv("KIA_CONNECT_TIMEOUT", "10"))
    sock_read_timeout = float(os.getenv("KIA_SOCK_READ_TIMEOUT", "30"))
    total_timeout = float(os.getenv("KIA_TOTAL_TIMEOUT", "60"))
    
    # Hardware-based scaling
    perf_mult = _detect_hardware_performance()
    
    # Scale timeouts: slower hardware gets longer timeouts
    if perf_mult < 1.0:
        # Slower hardware: increase timeouts
        scale_factor = 1.0 / perf_mult
        connect_timeout = int(connect_timeout * scale_factor)
        sock_read_timeout = int(sock_read_timeout * scale_factor)
        total_timeout = int(total_timeout * scale_factor)
        print(f"[Timeouts] Hardware scaling: {perf_mult:.2f}x → {scale_factor:.2f}x longer timeouts")
    
    # Ensure reasonable bounds
    connect_timeout = max(20, min(connect_timeout, 30))
    sock_read_timeout = max(15, min(sock_read_timeout, 120))
    total_timeout = max(30, min(total_timeout, 300))
    
    # print(f"[Timeouts] Final: connect={connect_timeout}s, read={sock_read_timeout}s, total={total_timeout}s")
    return aiohttp.ClientTimeout(connect=connect_timeout, sock_read=sock_read_timeout, total=total_timeout)

def _get_adaptive_connector_limits():
    """Get connector limits based on resource allocation"""
    allocation = _calculate_resource_allocation()
    
    # Network connector should respect network core limits
    max_connections = allocation['max_network_concurrent']
    max_per_host = min(2, max_connections // 2)  # Max 2 connections per host
    
    # Environment overrides
    max_connections = int(os.getenv("KIA_MAX_CONNECTIONS", max_connections))
    max_per_host = int(os.getenv("KIA_MAX_PER_HOST", max_per_host))
    
    print(f"[Connector] Max connections: {max_connections}, per host: {max_per_host}")
    
    return {
        'limit': max_connections,
        'limit_per_host': max_per_host,
        'force_close': True,
        'enable_cleanup_closed': True,
    }

class RateLimiter:
    def __init__(self):
        self._lock = None
        self._last_call_ts = 0.0

    def _get_lock(self):
        if self._lock is None:
            try:
                self._lock = asyncio.Lock()
            except RuntimeError:
                # If we can't create a lock in this event loop, create a new one
                self._lock = asyncio.Lock()
        return self._lock

    async def wait_if_needed(self):
        """Wait for rate limiting, but handle cancellation gracefully"""
        lock = self._get_lock()
        try:
            async with lock:
                now = time.monotonic()
                wait = max(0.0, self._last_call_ts + _MIN_GAP_S - now)
                if wait > 0:
                    # Use shield to prevent cancellation during critical rate limiting
                    await asyncio.shield(asyncio.sleep(wait + random.uniform(0, 0.2)))
                self._last_call_ts = time.monotonic()
        except asyncio.CancelledError:
            # If we get cancelled during rate limiting, just return
            # The outer function will handle the cancellation
            raise

_rate_limiter = RateLimiter()

async def _rate_limited_post(session, url, **kwargs):
    """Rate-limited POST with cancellation handling"""
    try:
        await _rate_limiter.wait_if_needed()
        return await session.post(url, **kwargs)
    except asyncio.CancelledError:
        # Re-raise cancellation to let outer function handle it
        raise

async def fetch_route_data(parent_id: int, connector, timeout_override=None) -> list:
    """
    Fetch route data with built-in timeout handling and cancellation safety.
    
    Args:
        parent_id: Route ID to fetch
        connector: aiohttp connector
        timeout_override: Optional timeout override (for testing)
    """
    url = f"{KIA_API_BASE}/SearchByRouteDetails_v4"
    payload = {
        "routeid": parent_id,
        "servicetypeid": 0
    }
    
    # Use timeout override if provided, otherwise get adaptive timeouts
    if timeout_override:
        timeout = timeout_override
    else:
        timeout = _get_adaptive_timeouts()
    
    max_attempts = 3
    base_backoff = 0.5
    
    try:
        async with aiohttp.ClientSession(connector=connector, connector_owner=False, timeout=timeout) as session:
            for attempt in range(1, max_attempts + 1):
                try:
                    # Use shield to prevent cancellation during the actual HTTP request
                    resp = await asyncio.shield(_rate_limited_post(session, url, json=payload, headers=HEADERS))
                    
                    async with resp as resp:
                        if resp.status != 200:
                            # Retry on transient server errors
                            if 500 <= resp.status < 600:
                                raise aiohttp.ClientResponseError(
                                    request_info=resp.request_info,
                                    status=resp.status,
                                    message=f"Server error {resp.status}",
                                    headers=resp.headers
                                )
                            print(f"[Getter] Error {resp.status} for parent_id {parent_id}")
                            return []

                        try:
                            json_data = await resp.json()
                        except (aiohttp.ClientPayloadError, aiohttp.ContentTypeError):
                            # Fallback to text and try manual JSON parse
                            raw = await resp.text(errors='ignore')
                            try:
                                json_data = json.loads(raw)
                            except Exception:
                                raise

                        if not json_data.get("issuccess", False):
                            print(f"[Getter] API error: {json_data.get('message')}")
                            return []

                        combined_data = []
                        for direction in ["up", "down"]:
                            if direction in json_data:
                                combined_data.extend(json_data[direction].get("data", []))

                        return combined_data
                        
                except asyncio.CancelledError:
                    # Task was cancelled, don't retry
                    print(f"[Getter] Task cancelled for route {parent_id}")
                    raise
                    
                except (aiohttp.ClientPayloadError, aiohttp.ServerDisconnectedError, aiohttp.ClientOSError, asyncio.TimeoutError, ConnectionResetError, aiohttp.ClientResponseError) as e:
                    is_last = attempt == max_attempts
                    if is_last:
                        traceback.print_exc()
                        print(f"[Getter] Exception fetching live data for route {parent_id}: {e}")
                        return []
                    
                    # Use shield for retry sleep to prevent cancellation
                    try:
                        sleep_s = base_backoff * (2 ** (attempt - 1)) + random.uniform(0, 0.2)
                        await asyncio.shield(asyncio.sleep(sleep_s))
                    except asyncio.CancelledError:
                        # If sleep gets cancelled, don't retry
                        raise
                        
            return []
            
    except asyncio.CancelledError:
        # Re-raise cancellation to let caller handle it
        raise
    except Exception as e:
        traceback.print_exc()
        print(f"[Getter] Exception fetching live data for route {parent_id}: {e}")
        return []
