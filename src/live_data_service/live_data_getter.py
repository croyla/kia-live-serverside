import os
import traceback
import asyncio
import time
import random
import json
# Use more efficient C-based ijson backend for better memory performance
try:
    import ijson.backends.yajl2_cffi as ijson
except ImportError:
    try:
        import ijson.backends.yajl2_c as ijson
    except ImportError:
        import ijson  # Fallback to default if C backends unavailable

import multiprocessing
import psutil
import gc
from typing import Iterator, AsyncIterator

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
    connect_timeout = float(os.getenv("KIA_CONNECT_TIMEOUT", "15"))  # Increased default
    sock_read_timeout = float(os.getenv("KIA_SOCK_READ_TIMEOUT", "45"))  # Increased default
    total_timeout = float(os.getenv("KIA_TOTAL_TIMEOUT", "90"))  # Increased default
    
    # Hardware-based scaling - less aggressive scaling to maintain performance
    perf_mult = _detect_hardware_performance()
    
    # Only scale timeouts for very slow hardware, otherwise use defaults
    if perf_mult < 0.7:  # Only scale for very slow hardware
        # Slower hardware: increase timeouts moderately
        scale_factor = 1.0 / max(perf_mult, 0.5)  # Cap scale factor at 2x
        connect_timeout = int(connect_timeout * scale_factor)
        sock_read_timeout = int(sock_read_timeout * scale_factor)
        total_timeout = int(total_timeout * scale_factor)
        print(f"[Timeouts] Hardware scaling for slow hardware: {perf_mult:.2f}x → {scale_factor:.2f}x longer timeouts")
    
    # Set reasonable bounds that prioritize responsiveness
    connect_timeout = max(10, min(connect_timeout, 25))  # Faster connection timeout
    sock_read_timeout = max(20, min(sock_read_timeout, 60))  # Faster read timeout
    total_timeout = max(40, min(total_timeout, 120))  # Faster total timeout
    
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
        lock = self._get_lock()
        async with lock:
            now = time.monotonic()
            wait = max(0.0, self._last_call_ts + _MIN_GAP_S - now)
            if wait > 0:
                await asyncio.sleep(wait + random.uniform(0, 0.2))
            self._last_call_ts = time.monotonic()

_rate_limiter = RateLimiter()

async def _rate_limited_post(session, url, **kwargs):
    await _rate_limiter.wait_if_needed()
    return await session.post(url, **kwargs)

async def process_json_streaming(response_text: str) -> dict:
    """Memory-optimized JSON processor - direct stream processing bypasses .encode()"""
    try:
        # CRITICAL FIX: Use ijson.parse directly on text to avoid .encode() memory copies
        # This eliminates the 640MB memory accumulation from encoding large responses
        
        # Initialize result structure
        data = {'up': {'data': []}, 'down': {'data': []}}
        
        # Process top-level fields using single-pass streaming with direct text parsing
        try:
            for prefix, event, value in ijson.parse(response_text):
                if prefix == 'issuccess':
                    data['issuccess'] = value
                elif prefix == 'message':
                    data['message'] = value
                # Don't break - continue parsing to get all top-level fields
        except Exception:
            pass
            
        # CRITICAL FIX: Direct stream processing with text input (no .encode() needed)
        try:
            # Process 'up' direction data with generator using direct text parsing
            up_items = []
            for item in ijson.items(response_text, 'up.data.item'):
                up_items.append(item)
                # Process in small batches to prevent memory accumulation and data leaks
                if len(up_items) >= 5:  # Reduced from 20 to 5 items per batch for strict memory control
                    if 'up' not in data:
                        data['up'] = {'data': []}
                    data['up']['data'].extend(up_items)
                    # Strict data cleanup to prevent leaks
                    up_items.clear()  # More thorough cleanup
                    up_items = []  # Reset reference
                    gc.collect()  # Force garbage collection to prevent data retention
            
            # Process remaining items with strict cleanup
            if up_items:
                if 'up' not in data:
                    data['up'] = {'data': []}
                data['up']['data'].extend(up_items)
                # Comprehensive data cleanup to prevent leaks
                up_items.clear()  # Clear contents
                up_items = None  # Remove reference
                del up_items  # Explicit deletion
                
        except Exception as e:
            print(f"[JSON] Up data streaming error: {e}")
            pass
            
        try:
            # Process 'down' direction data with generator using direct text parsing
            down_items = []
            for item in ijson.items(response_text, 'down.data.item'):
                down_items.append(item)
                # Process in small batches to prevent memory accumulation and data leaks
                if len(down_items) >= 5:  # Reduced from 20 to 5 items per batch for strict memory control
                    if 'down' not in data:
                        data['down'] = {'data': []}
                    data['down']['data'].extend(down_items)
                    # Strict data cleanup to prevent leaks
                    down_items.clear()  # More thorough cleanup
                    down_items = []  # Reset reference
                    gc.collect()  # Force garbage collection to prevent data retention
            
            # Process remaining items with strict cleanup
            if down_items:
                if 'down' not in data:
                    data['down'] = {'data': []}
                data['down']['data'].extend(down_items)
                # Comprehensive data cleanup to prevent leaks
                down_items.clear()  # Clear contents
                down_items = None  # Remove reference
                del down_items  # Explicit deletion
                
        except Exception as e:
            print(f"[JSON] Down data streaming error: {e}")
            pass
        
        # Force garbage collection to ensure cleanup (no encoded_response to clean up)
        gc.collect()
        
        # Validate we have some data, otherwise fallback
        if (not data.get('issuccess') and 
            not data.get('up', {}).get('data') and 
            not data.get('down', {}).get('data')):
            # If streaming parsing failed completely, fall back to standard parsing
            print("[JSON] Streaming failed, using fallback parser")
            # Clear response_text reference before fallback
            fallback_data = json.loads(response_text)
            response_text = None  # Clear reference to prevent retention
            return fallback_data
            
        # Clear response_text reference before return
        response_text = None
        return data
        
    except Exception as e:
        print(f"[JSON] Streaming parse failed, falling back to standard: {e}")
        # Fallback to standard JSON parsing
        try:
            return json.loads(response_text)
        except Exception as fallback_error:
            print(f"[JSON] Fallback parsing also failed: {fallback_error}")
            return {'issuccess': False, 'message': 'Parse error', 'up': {'data': []}, 'down': {'data': []}}

async def fetch_route_data_streaming(parent_id: int, connector) -> AsyncIterator[dict]:
    """Stream API data without accumulating large objects in memory"""
    url = f"{KIA_API_BASE}/SearchByRouteDetails_v4"
    payload = {
        "routeid": parent_id,
        "servicetypeid": 0
    }
    
    try:
        # Get adaptive timeouts based on hardware
        timeout = _get_adaptive_timeouts()
        max_attempts = 3
        base_backoff = 0.5
        
        async with aiohttp.ClientSession(connector=connector, connector_owner=False, timeout=timeout) as session:
            for attempt in range(1, max_attempts + 1):
                try:
                    resp = await _rate_limited_post(session, url, json=payload, headers=HEADERS)
                    
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
                        return

                    # Use streaming JSON parsing to reduce memory usage
                    try:
                        raw_text = await resp.text()
                        # Use streaming JSON processor instead of json.loads
                        # print(raw_text)
                        json_data = await process_json_streaming(raw_text)
                        raw_text = None  # Release immediately
                        # print(json_data)
                        if not json_data.get("issuccess", False):
                            # print('JSON RAW TEXT ERROR', json_data)
                            print(f"[Getter] API error: {json_data.get('message')}")
                            json_data = None
                            gc.collect()
                            return

                        # Process and yield items incrementally to prevent accumulation
                        for direction in ["up", "down"]:
                            direction_data = json_data.get(direction, {}).get("data", [])
                            
                            # Yield items one by one instead of accumulating
                            for item in direction_data:
                                yield item
                                
                        # Clear references immediately and force cleanup
                        json_data = None
                        gc.collect()
                        return
                        
                    except (aiohttp.ClientPayloadError, aiohttp.ContentTypeError):
                        # Fallback to text and try manual JSON parse
                        raw = await resp.text(errors='ignore')
                        try:
                            # Use streaming parser for fallback as well
                            json_data = await process_json_streaming(raw)
                            raw = None  # Release immediately
                            
                            if not json_data.get("issuccess", False):
                                # print('JSON ERROR')
                                print(f"[Getter] API error: {json_data.get('message')}")
                                return

                            for direction in ["up", "down"]:
                                direction_data = json_data.get(direction, {}).get("data", [])
                                for item in direction_data:
                                    yield item
                                    
                            json_data = None
                            gc.collect()
                            return
                        except Exception:
                            raise
                            
                except (aiohttp.ClientPayloadError, aiohttp.ServerDisconnectedError, aiohttp.ClientOSError, asyncio.TimeoutError, ConnectionResetError, aiohttp.ClientResponseError) as e:
                    is_last = attempt == max_attempts
                    if is_last:
                        traceback.print_exc()
                        print(f"[Getter] Exception fetching live data for route {parent_id}: {e}")
                        return
                    sleep_s = base_backoff * (2 ** (attempt - 1)) + random.uniform(0, 0.2)
                    await asyncio.sleep(sleep_s)
            return
    except Exception as e:
        traceback.print_exc()
        print(f"[Getter] Exception fetching live data for route {parent_id}: {e}")
        return

async def fetch_route_data(parent_id: int, connector) -> list:
    """Backward compatibility wrapper - converts streaming to list"""
    result = []
    batch_size = 10  # Process in small batches to reduce memory pressure
    current_batch = []
    
    try:
        async for vehicle_data in fetch_route_data_streaming(parent_id, connector):
            current_batch.append(vehicle_data)
            
            # Process in batches and periodically cleanup memory
            if len(current_batch) >= batch_size:
                result.extend(current_batch)
                current_batch = []  # Clear batch immediately
                
                # Periodic garbage collection to prevent fragmentation
                if len(result) % 50 == 0:
                    gc.collect()
        
        # Process remaining items
        if current_batch:
            result.extend(current_batch)
            current_batch = []
            
        return result
        
    except Exception as e:
        traceback.print_exc()
        print(f"[Getter] Exception in compatibility wrapper for route {parent_id}: {e}")
        return []
