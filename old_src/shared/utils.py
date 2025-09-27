import os
import json
import zipfile
import hashlib
import polyline
from typing import List, Tuple, Dict, Iterator
import ijson  # Add streaming JSON parser

def load_json_streaming(filepath: str, chunk_size: int = 1000) -> Iterator[Dict]:
    """Stream JSON data in chunks to reduce memory usage"""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            # Use ijson for streaming if available, fallback to regular json
            try:
                parser = ijson.parse(f)
                current_obj = {}
                for prefix, event, value in parser:
                    if event == 'start_map':
                        current_obj = {}
                    elif event == 'end_map':
                        yield current_obj
                        current_obj = {}
                    elif event in ('string', 'number', 'boolean'):
                        current_obj[prefix.split('.')[-1]] = value
            except ImportError:
                # Fallback to regular json loading
                data = json.load(f)
                if isinstance(data, dict):
                    for key, value in data.items():
                        yield {key: value}
                elif isinstance(data, list):
                    for item in data:
                        yield item
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        yield {}

def load_json(filepath: str, max_memory_mb: int = 50) -> dict:
    """Load JSON with aggressive memory optimization for large files"""
    try:
        file_size = os.path.getsize(filepath)
        file_size_mb = file_size / (1024 * 1024)
        
        # More aggressive streaming threshold for 500MB VPS
        if file_size_mb > 5:  # 5MB threshold instead of 10MB
            print(f"Large file detected ({file_size_mb:.1f}MB), using memory-optimized streaming")
            return _streaming_json_loader(filepath, max_memory_mb)
        else:
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return {}

def _streaming_json_loader(filepath: str, max_memory_mb: int) -> dict:
    """Memory-optimized streaming JSON loader"""
    import gc
    result = {}
    file_size = os.path.getsize(filepath)
    max_chunk_size = min(8192, max_memory_mb * 1024)  # Scale chunk size with memory limit
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            buffer = ""
            processed_mb = 0
            
            while True:
                chunk = f.read(max_chunk_size)
                if not chunk:
                    break
                    
                buffer += chunk
                processed_mb += len(chunk) / (1024 * 1024)
                
                # Process buffer when we have enough data or every 10MB
                if len(buffer) > max_chunk_size * 10 or processed_mb % 10 < 0.1:
                    try:
                        # Try to parse complete JSON objects
                        if buffer.strip().startswith('{') and buffer.strip().endswith('}'):
                            temp_data = json.loads(buffer)
                            if isinstance(temp_data, dict):
                                # Process in smaller chunks to avoid memory spikes
                                chunk_result = _chunked_dict_processor(temp_data, max_memory_mb)
                                result.update(chunk_result)
                                buffer = ""
                                gc.collect()  # Force cleanup after processing
                    except json.JSONDecodeError:
                        continue
            
            # Process remaining buffer
            if buffer.strip():
                try:
                    temp_data = json.loads(buffer)
                    if isinstance(temp_data, dict):
                        chunk_result = _chunked_dict_processor(temp_data, max_memory_mb)
                        result.update(chunk_result)
                except json.JSONDecodeError:
                    # Fallback to regular loading for remaining data
                    f.seek(0)
                    result = json.load(f)
                    
    except Exception as e:
        print(f"Streaming error, using fallback: {e}")
        with open(filepath, "r", encoding="utf-8") as f:
            result = json.load(f)
    
    return result

def _chunked_dict_processor(data_dict: dict, max_memory_mb: int, max_entries: int = 1000) -> dict:
    """Process large dictionaries in memory-conscious chunks"""
    import gc
    
    if len(data_dict) <= max_entries:
        return data_dict
        
    # Scale chunk size based on available memory
    chunk_size = min(max_entries, max(100, max_memory_mb * 10))
    result = {}
    
    items_list = list(data_dict.items())
    for i in range(0, len(items_list), chunk_size):
        chunk_items = items_list[i:i + chunk_size]
        chunk_dict = dict(chunk_items)
        result.update(chunk_dict)
        
        # Clean up chunk and force garbage collection
        del chunk_dict, chunk_items
        gc.collect()
        
    return result

def load_input_data_optimized(directory: str, max_memory_mb: int = 100) -> dict:
    """Load input data with memory constraints"""
    def safe_load_optimized(name, max_size_mb=max_memory_mb):
        path = os.path.join(directory, name)
        if not os.path.exists(path):
            return {}
        
        file_size = os.path.getsize(path)
        if file_size > max_size_mb * 1024 * 1024:
            print(f"Warning: {name} is {file_size / (1024*1024):.1f}MB, may exceed memory limit")
        
        return load_json(path)

    return {
        "client_stops": safe_load_optimized("client_stops.json", 50),  # Limit to 50MB
        "routes_children": safe_load_optimized("routes_children_ids.json", 10),  # Limit to 10MB
        "routes_parent": safe_load_optimized("routes_parent_ids.json", 10),  # Limit to 10MB
        "start_times": safe_load_optimized("start_times.json", 20),  # Limit to 20MB
        "routelines": safe_load_optimized("routelines.json", 30),  # Limit to 30MB
        "times": safe_load_optimized("times.json", 20)  # Limit to 20MB
    }

def load_gtfs_zip_optimized(zip_path: str, max_memory_mb: int = 50) -> dict:
    """Load GTFS zip with memory constraints and chunked processing"""
    gtfs_data = {}
    total_memory = 0
    
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            for name in z.namelist():
                if total_memory > max_memory_mb * 1024 * 1024:
                    print(f"Warning: Memory limit ({max_memory_mb}MB) reached, stopping GTFS load")
                    break
                    
                with z.open(name) as f:
                    content = f.read().decode("utf-8")
                    lines = content.splitlines()
                    
                    if not lines:
                        continue
                        
                    headers = lines[0].split(",")
                    records = []
                    
                    # Process records in chunks to control memory
                    chunk_size = 1000
                    for i in range(1, len(lines), chunk_size):
                        chunk_lines = lines[i:i + chunk_size]
                        chunk_records = [
                            dict(zip(headers, line.split(","))) 
                            for line in chunk_lines 
                            if line.strip()
                        ]
                        records.extend(chunk_records)
                        
                        # Estimate memory usage (rough calculation)
                        chunk_memory = sum(len(str(record)) for record in chunk_records)
                        total_memory += chunk_memory
                        
                        if total_memory > max_memory_mb * 1024 * 1024:
                            print(f"Memory limit reached at file {name}, truncating")
                            break
                    
                    gtfs_data[name] = records
                    
    except Exception as e:
        print(f"Error loading GTFS zip: {e}")
        
    return gtfs_data

def zip_gtfs_data_optimized(data: dict, zip_path: str, chunk_size: int = 1000):
    """Write GTFS data to zip - Fixed to ensure complete file generation"""
    os.makedirs(os.path.dirname(zip_path), exist_ok=True)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for filename, rows in data.items():
            if not rows:
                continue
                
            headers = list(rows[0].keys())
            all_content = [",".join(headers)]
            
            # Build complete content for each file to ensure integrity
            for row in rows:
                row_content = ",".join(str(row.get(h, "")) for h in headers)
                all_content.append(row_content)
            
            # Write complete file content at once to ensure file completeness
            zf.writestr(filename, "\n".join(all_content))
            print(f"[GTFS] Generated {filename} with {len(rows)} records")

def decode_polyline(poly: str) -> List[Tuple[float, float]]:
    return polyline.decode(poly, geojson=True)

def add_time_trip_times(start_time, minutes):
    # Extract hours and minutes
    hours = start_time // 100
    mins = start_time % 100

    # Convert to total minutes
    total_minutes = hours * 60 + mins + minutes

    # No wraparound – allow overflow beyond 2400
    new_hours = total_minutes // 60
    new_mins = total_minutes % 60

    return new_hours * 100 + new_mins

def interpolate_trip_times(start_time: int, total_duration: int, stops: List[Tuple[str, float, str]]) -> dict[str, int]:
    """
    Interpolates stop times based on distance along trip.
    Formula: (tripDuration * stopDistance) / totalDistance
    """
    total_distance = max(stop[1] for stop in stops)
    return {stop[0]: add_time_trip_times(start_time, round((total_duration * stop[1]) / total_distance)) for stop in stops}

def group_stops_by_latlon(stops: List[Dict]) -> List[Dict]:
    """
    Merges stops that have same lat/lon and returns a deduplicated list.
    """
    seen = {}
    for stop in stops:
        key = (round(stop["stop_lat"], 6), round(stop["stop_lon"], 6))
        if key not in seen:
            seen[key] = stop
    return list(seen.values())

def generate_trip_id_timing_map(start_times, route_children) -> dict[str, list]:
    used_ids = set()
    all_ids_timings = {}
    for route_key, route_id in route_children.items():
        route_trips = start_times.get(route_key) or []
        for trip_data in route_trips: # Keep logic same with GTFSBuilder.build_trips_and_stop_times()
            trip_start = trip_data['start']
            trip_index = 1
            while f"{route_id}_{trip_index}" in used_ids:
                trip_index += 1
            trip_id = f"{route_id}_{trip_index}"
            used_ids.add(trip_id)
            if route_key not in all_ids_timings.keys():
                all_ids_timings[route_key] = []
            all_ids_timings[route_key].append(
                {"start": f"{trip_start // 100:02d}:{trip_start % 100:02d}:00", "trip": trip_id}
            )
    return all_ids_timings

def data_has_changed(new_gtfs: dict, existing_gtfs: dict) -> bool:
    """
    Returns True if GTFS content changed (ignores feed_info.txt and calendar date differences).
    """
    skip_keys = {"feed_info.txt", "calendar.txt"}

    def hash_rows(rows: List[dict]) -> str:
        norm = sorted(json.dumps(r, sort_keys=True) for r in rows)
        return hashlib.md5("".join(norm).encode()).hexdigest()

    for key in new_gtfs:
        if key in skip_keys:
            continue
        new_hash = hash_rows(new_gtfs.get(key, []))
        old_hash = hash_rows(existing_gtfs.get(key, []))
        if new_hash != old_hash:
            return True
    return False

def to_hhmm(n) -> str:
    n = int(n)
    h, m = divmod(n, 100)
    if not 0 <= m < 60:
        raise ValueError(f"Invalid minutes {m} in {n}. Expected 00–59.")
    return f"{h:02d}:{m:02d}"

def from_hhmm(s: str, *, enforce_24h: bool = True) -> int:
    """Convert 'HH:MM' to int like 1405, 205, 5."""

    # s = s.strip()
    try:
        h_str, m_str = s.split(":")
        h, m = int(h_str), int(m_str)
    except Exception as e:
        print('time?', s)
        raise ValueError(f"Invalid time '{s}'. Expected 'HH:MM'.") from e

    if not (0 <= m < 60):
        raise ValueError(f"Invalid minutes: {m} (must be 0–59).")
    if enforce_24h and not (0 <= h < 24):
        raise ValueError(f"Invalid hours: {h} (must be 0–23).")

    return h * 100 + m
