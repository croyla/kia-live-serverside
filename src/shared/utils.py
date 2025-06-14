import os
import json
import zipfile
import hashlib
import polyline
from typing import List, Tuple, Dict

def load_json(filepath: str) -> dict:
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)

def load_input_data(directory: str) -> dict:
    def safe_load(name):
        path = os.path.join(directory, name)
        return load_json(path) if os.path.exists(path) else {}

    return {
        "client_stops": safe_load("client_stops.json"),
        "routes_children": safe_load("routes_children_ids.json"),
        "routes_parent": safe_load("routes_parent_ids.json"),
        "start_times": safe_load("start_times.json"),
        "routelines": safe_load("routelines.json"),
        "times": safe_load("times.json")
    }

def load_gtfs_zip(zip_path: str) -> dict:
    gtfs_data = {}
    with zipfile.ZipFile(zip_path, "r") as z:
        for name in z.namelist():
            with z.open(name) as f:
                lines = f.read().decode("utf-8").splitlines()
                headers = lines[0].split(",")
                records = [dict(zip(headers, line.split(","))) for line in lines[1:] if line.strip()]
                gtfs_data[name] = records
    return gtfs_data

def zip_gtfs_data(data: dict, zip_path: str):
    os.makedirs(os.path.dirname(zip_path), exist_ok=True)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for filename, rows in data.items():
            if not rows:
                continue
            headers = list(rows[0].keys())
            content = [",".join(headers)]
            for row in rows:
                content.append(",".join(str(row.get(h, "")) for h in headers))
            zf.writestr(filename, "\n".join(content))

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

def interpolate_trip_times(start_time: int, total_duration: int, stops: List[Tuple[str, float, str]]) -> List[int]:
    """
    Interpolates stop times based on distance along trip.
    Formula: (tripDuration * stopDistance) / totalDistance
    """
    total_distance = max(stop[1] for stop in stops)
    return [add_time_trip_times(start_time, round((total_duration * stop[1]) / total_distance)) for stop in stops]

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
