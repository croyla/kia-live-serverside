import json
import traceback
import time
from typing import Dict, List, Optional

import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial, lru_cache

from google.transit import gtfs_realtime_pb2
from datetime import datetime, timedelta
import pytz

from old_src.shared.config import DB_PATH
from old_src.shared.db import insert_vehicle_data, insert_vehicle_position
from old_src.shared.utils import to_hhmm, from_hhmm
from datetime import date
from old_src.shared.memory_config import get_memory_config

from old_src.shared import ThreadSafeDict, times, routes_children, predict_times, prediction_cache, route_stops, \
    pred_seg_cache, static_enriched_times
from collections import deque

local_tz = pytz.timezone("Asia/Kolkata")
# Entity storage with 15-minute TTL and memory limits to prevent accumulation

# Performance optimizations - global caches to avoid rebuilding
_routename_by_id_cache = {}
_last_routes_cache_update = 0
_prediction_batch_queue = []
_vehicle_batch_queue = []

@lru_cache(maxsize=1000)
def parse_local_time_cached(hhmm: str) -> Optional[int]:
    """Cached version of parse_local_time for performance"""
    if not hhmm or ":" not in hhmm:
        return None
    try:
        hh, mm = map(int, hhmm.split(":"))
        now = datetime.now(local_tz)
        t = now.replace(hour=hh, minute=mm, second=0, microsecond=0)

        # If parsed time is too far in the past, assume next day
        if t < now - timedelta(hours=6):
            t += timedelta(days=1)
        # If parsed time is too far in the future, assume previous day  
        if t > now + timedelta(hours=18):
            t -= timedelta(days=1)
        return int(t.timestamp())
    except Exception:
        return None

def get_cached_routename_by_id() -> Dict[str, str]:
    """Get cached route name mapping, rebuild only if needed"""
    global _routename_by_id_cache, _last_routes_cache_update
    
    # Check if cache needs update (every 5 minutes)
    current_time = time.time()
    if current_time - _last_routes_cache_update > 300:  # 5 minutes
        _routename_by_id_cache = {str(v): k for k, v in routes_children.items()}
        _last_routes_cache_update = current_time
    
    return _routename_by_id_cache


def transform_response_to_feed_entities(api_data: list, job: dict) -> list:
    all_entities = ThreadSafeDict(max_size=25, max_age_seconds=10*60, max_memory_mb=10)  # 15 min TTL, 50MB limit
    route_id = job["route_id"]
    trip_time = job["trip_time"]
    trip_id = job["trip_id"]
    match_window = timedelta(minutes=2)
    # print(f"RECEIVED CALL TO TRANSFORM RESPONSE TO FEED ENTITIES")
    # print(f"Received processing API DATA {api_data}")
    vehicle_groups = {}
    vehicles = set()
    for stop in api_data:
        if str(stop.get("routeid")) != str(route_id):
            continue

        vehicle_list = stop.get("vehicleDetails", [])
        # print('vehicle list', vehicle_list)
        for vehicle in vehicle_list:
            vehicle_id = str(vehicle.get("vehicleid"))
            # print(vehicle)
            if not vehicle_id:
                continue
            # if not vehicle_id in vehicles:
            #     print(f"Found vehicle {vehicle_id} for route {route_id}")
            #     vehicles.add(vehicle_id)
            sch_time_str = vehicle.get("sch_tripstarttime")
            if not sch_time_str:
                continue

            try:
                hh, mm = map(int, sch_time_str.split(":"))
                sch_trip_time = trip_time.replace(hour=hh, minute=mm, second=0, microsecond=0)
            except Exception:
                continue

            if abs(sch_trip_time - trip_time) > match_window:
                # print(f"Passing on vehicle {vehicle_id} {route_id} {sch_trip_time} {trip_time}")
                continue

            # Initialize group if needed
            if vehicle_id not in vehicle_groups:
                vehicle_groups[vehicle_id] = {
                    "vehicle": vehicle,
                    "stops": []
                }

            # Copy per-stop schedule into stop structure
            stop_copy = stop.copy()
            stop_copy["sch_arrivaltime"] = vehicle.get("sch_arrivaltime")
            stop_copy["sch_departuretime"] = vehicle.get("sch_departuretime")
            stop_copy["actual_arrivaltime"] = vehicle.get("actual_arrivaltime")
            stop_copy["actual_departuretime"] = vehicle.get("actual_departuretime")
            vehicle_groups[vehicle_id]["stops"].append(stop_copy)
    if trip_id in all_entities:
        del all_entities[trip_id]
        all_entities.pop(trip_id)
    # print(vehicle_groups)
    # Step 2: Build GTFS-RT FeedEntities in parallel (one per vehicle)
    if vehicle_groups:
        # Get hardware-appropriate thread pool size with increased worker capacity
        config = get_memory_config()
        max_workers = min(len(vehicle_groups), config.get('max_parallel_workers', 8))
        # Ensure minimum workers for responsiveness
        max_workers = max(max_workers, 4)

        # print(f"[Transformer] Processing {len(vehicle_groups)} vehicles with {max_workers} workers (increased for better performance)")
        # print("BUILDING FEED ENTITIES TRANSFORM RESPONSE TO FEED ENTITIES")
        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Create partial function with fixed arguments
            build_entity_partial = partial(
                build_feed_entity_wrapper,
                trip_id=trip_id,
                route_id=route_id
            )

            # Submit all vehicle processing tasks
            future_to_vehicle = {
                executor.submit(build_entity_partial, bundle): vehicle_id
                for vehicle_id, bundle in vehicle_groups.items()
            }

            # Collect results as they complete
            successful_entities = []
            for future in as_completed(future_to_vehicle):
                vehicle_id = future_to_vehicle[future]
                try:
                    entity = future.result()
                    if entity:
                        successful_entities.append(entity)
                        # print(f"[Transformer] Successfully built entity for vehicle {vehicle_id}")
                    else:
                        print(f"[Transformer] Failed to build entity for vehicle {vehicle_id}")
                except Exception as e:
                    print(f"[Transformer] Error building entity for vehicle {vehicle_id}: {e}")
                    traceback.print_exc()

            # Store the first successful entity (or create a combined one if needed)
            if successful_entities:
                # For now, store the first entity. In the future, we could combine multiple vehicles
                all_entities[trip_id] = successful_entities[0]
                # print(f"[Transformer] Stored entity for trip {trip_id} (from {len(successful_entities)} vehicles)")
            else:
                print(f"[Transformer] No successful entities built for trip {trip_id}")
    del api_data
    return all_entities.values()


def build_feed_entity_wrapper(bundle: dict, trip_id: str, route_id: str):
    """Wrapper function for building feed entities with error handling"""
    try:
        # print("BUILD FEED ENTITY WRAPPER")
        return build_feed_entity(bundle["vehicle"], trip_id, route_id, bundle["stops"])
    except Exception as e:
        print(f"[Transformer] Error in build_feed_entity_wrapper: {e}")
        traceback.print_exc()
        return None

def build_feed_entity(vehicle: dict, trip_id: str, route_id: str, stops: list):
    # ex = time.time()
    # print("BUILDING FEED ENTITY")
    # print('vehicle', vehicle)
    # print('route_id', route_id)
    # print('trip_id', trip_id)
    # print('stops', stops)

    # Early validation
    if not stops:
        print(f"[Transformer] No stops provided for vehicle {vehicle.get('vehicleid', 'unknown')}")
        return None

    entity = gtfs_realtime_pb2.FeedEntity()
    entity.id = f"veh_{vehicle['vehicleid']}"

    # Use cached route lookups to avoid repeated dictionary rebuilds
    routename_by_id = get_cached_routename_by_id()
    route_key = routename_by_id.get(str(route_id))
    trip_times = None
    predicted_scheduled = None

    if route_key and route_key in times:
        times_list = list(times[route_key]) if isinstance(times[route_key], list) else []
        times_by_start = {to_hhmm(trip['start']): trip for trip in times_list}
        desired_start = stops[0]['sch_departuretime']
        if desired_start in times_by_start:
            try:
                trip_times = {str(stop['stop_id']): to_hhmm(stop['stop_time']) for stop in times_by_start[desired_start]['stops']}
                # Filter stops that have trip times
                stops = [stop for stop in stops if str(stop['stationid']) in trip_times]
            except:
                traceback.print_exc()
        else:
            # Try exact match in static_enriched_times
            try:
                desired_start_int = from_hhmm(desired_start)
                enriched = static_enriched_times.get(route_key, None)
                if enriched:
                    for entry in list(enriched):
                        if entry.get('start') == desired_start_int:
                            trip_times = {str(stop['stop_id']): to_hhmm(stop['stop_time']) for stop in entry.get('stops', [])}
                            stops = [stop for stop in stops if str(stop['stationid']) in trip_times]
                            break
            except:
                traceback.print_exc()

        # Filter stops based on static feed stop IDs
        if route_key in route_stops:
            static_feed_stop_ids = {str(s["stop_id"]) for s in route_stops[route_key]["stops"]}
            stops = [stop for stop in stops if str(stop["stationid"]) in static_feed_stop_ids]

    # Early return if no valid stops remain
    if not stops:
        print(f"[Transformer] No valid stops remaining for vehicle {vehicle.get('vehicleid', 'unknown')}")
        return None
    trip_update = entity.trip_update
    trip_update.trip.trip_id = trip_id
    trip_update.trip.route_id = str(route_id)
    trip_update.vehicle.id = str(vehicle["vehicleid"])
    trip_update.vehicle.label = vehicle.get("vehiclenumber", "")
    pass_point = 0
    last_act_dep = 0
    predicted_scheduled = None # Run predictions against scheduled times if missing in times object
    if not trip_times:
        try:
            # Prefer static schedule (times + static_enriched_times), then fallback to predict_times
            static_predictions = calculate_static_predictions(route_key, stops[0]['sch_departuretime'], stops)
            
            if static_predictions:
                # Convert HH:MM format back to time integers for compatibility
                predicted_scheduled = {}
                for stop_id, time_str in static_predictions.items():
                    predicted_scheduled[str(stop_id)] = from_hhmm(time_str)
                # Enrich bounded store (do not mutate times)
                if route_key:
                    entry = {
                        "start": from_hhmm(stops[0]['sch_departuretime']),
                        "stops": [{"stop_id": s["stationid"], "stop_time": predicted_scheduled[str(s["stationid"])]} for s in stops if str(s["stationid"]) in predicted_scheduled]
                    }
                    existing = static_enriched_times.get(route_key, None)
                    dq = existing if existing is not None else deque(maxlen=8)
                    sig = (entry["start"], tuple(sorted(str(x["stop_id"]) for x in entry["stops"])))
                    existing_sigs = set((e["start"], tuple(sorted(str(x["stop_id"]) for x in e["stops"]))) for e in list(dq))
                    if sig not in existing_sigs:
                        dq.append(entry)
                        static_enriched_times[route_key] = dq
            else:
                predicted_scheduled = {}
                # Fallback to predict_times
                try:
                    data_input = [{
                        "route_id": route_id,
                        "trip_start": stops[0]['sch_departuretime'],
                        "stops": [{
                            "stop_id": str(s["stationid"]),
                            "stop_loc": [s["centerlat"], s["centerlong"]]
                        } for s in stops]
                    }]
                    predictions = predict_times.predict_stop_times_segmental(data_input=data_input, db_path=DB_PATH)
                    if predictions:
                        first = predictions[0]
                        for it in first.get("stops", []):
                            predicted_scheduled[str(it["stop_id"])] = it["stop_time"]
                        # Enrich bounded store for future matches
                        entry = {
                            "start": from_hhmm(stops[0]['sch_departuretime']),
                            "stops": [{"stop_id": it["stop_id"], "stop_time": it["stop_time"]} for it in first.get("stops", [])]
                        }
                        existing = static_enriched_times.get(route_key, None)
                        dq = existing if existing is not None else deque(maxlen=8)
                        sig = (entry["start"], tuple(sorted(str(x["stop_id"]) for x in entry["stops"])))
                        existing_sigs = set((e["start"], tuple(sorted(str(x["stop_id"]) for x in e["stops"]))) for e in list(dq))
                        if sig not in existing_sigs:
                            dq.append(entry)
                            static_enriched_times[route_key] = dq
                except Exception:
                    traceback.print_exc()
                    predicted_scheduled = {}
        except:
            traceback.print_exc()
            predicted_scheduled = {}
    for stop in stops: # Get the last stop the bus passed, so that we can add delays necessary for stops after the last stop
        # Keep original values for data prediction database
        stop["orig_sch_arrivaltime"], stop["orig_sch_departuretime"], stop["orig_actual_arrivaltime"], stop["orig_actual_departuretime"] = stop["sch_arrivaltime"], stop["sch_departuretime"], stop["actual_arrivaltime"], stop["actual_departuretime"]
        stop["stationid"] = str(stop['stationid']) # Normalise all values to string, instead of having integers
        act_arr = parse_local_time_cached(stop.get("actual_arrivaltime") or "")
        act_dep = parse_local_time_cached(stop.get("actual_departuretime") or "")
        if trip_times: # If we have trip times replace scheduled times with our own
            if str(stop.get("stationid")) in trip_times:
                stop['sch_arrivaltime'] = trip_times[stop.get("stationid")]
                stop['sch_departuretime'] = trip_times[stop.get("stationid")]
            else:
                if not predicted_scheduled:
                    static_predictions = calculate_static_predictions(route_key, stops[0]['sch_departuretime'], stops)
                    if static_predictions:
                        predicted_scheduled = {str(k): from_hhmm(v) for k, v in static_predictions.items()}
                    else:
                        try:
                            data_input = [{
                                "route_id": route_id,
                                "trip_start": stops[0]['sch_departuretime'],
                                "stops": [{
                                    "stop_id": str(s["stationid"]),
                                    "stop_loc": [s["centerlat"], s["centerlong"]]
                                } for s in stops]
                            }]
                            predictions = predict_times.predict_stop_times_segmental(data_input=data_input, db_path=DB_PATH)
                            if predictions:
                                first = predictions[0]
                                predicted_scheduled = {str(it["stop_id"]): it["stop_time"] for it in first.get("stops", [])}
                                # Enrich bounded store
                                entry = {
                                    "start": from_hhmm(stops[0]['sch_departuretime']),
                                    "stops": [{"stop_id": it["stop_id"], "stop_time": it["stop_time"]} for it in first.get("stops", [])]
                                }
                                existing = static_enriched_times.get(route_key, None)
                                dq = existing if existing is not None else deque(maxlen=8)
                                sig = (entry["start"], tuple(sorted(str(x["stop_id"]) for x in entry["stops"])))
                                existing_sigs = set((e["start"], tuple(sorted(str(x["stop_id"]) for x in e["stops"]))) for e in list(dq))
                                if sig not in existing_sigs:
                                    dq.append(entry)
                                    static_enriched_times[route_key] = dq
                            else:
                                predicted_scheduled = {}
                        except Exception:
                            traceback.print_exc()
                            predicted_scheduled = {}
                stop['sch_arrivaltime'] = to_hhmm(predicted_scheduled[stop.get("stationid")])
                stop['sch_departuretime'] = to_hhmm(predicted_scheduled[stop.get("stationid")])
        else:
            print(f'TRIP TIMES {True if trip_times else False}, PREDICTED_SCHEDULED {predicted_scheduled}')
            print(f"times has key? {route_key in times}")
            print(f"times has start? {times[route_key]}")
            stop['sch_arrivaltime'] = to_hhmm(predicted_scheduled[stop.get("stationid")])
            stop['sch_departuretime'] = to_hhmm(predicted_scheduled[stop.get("stationid")])
        if act_dep and act_arr:
            pass_point = 0
            last_act_dep = act_dep
            continue
        if pass_point == 0:
            pass_point = str(stop.get("stationid", ""))
    predicted_with_delays = None # Run predictions based on actual times, use cache if pass point is same
    if pass_point != 0:
        if (route_id, stops[0]['sch_departuretime']) in prediction_cache \
                and pass_point == prediction_cache[(route_id, stops[0]['sch_departuretime'])]['pass_point']:
            predicted_with_delays = prediction_cache[(route_id, stops[0]['sch_departuretime'])]['predictions']
        else:
            cleaned_stops = []
            for i, stop in enumerate(stops): # Clean stops of skipped stops
                if i == 0:
                    cleaned_stops.append({
                        "stop_id": stop["stationid"],
                        "time": from_hhmm(s=stop.get('actual_departuretime') or stop.get('sch_departuretime') or stop.get('sch_arrivaltime')),
                        "stop_loc": [stop["centerlat"], stop["centerlong"]]
                    })
                elif not stop.get('actual_arrivaltime') and not stop.get('actual_departuretime'):
                    cleaned_stops.append({"stop_id": stop["stationid"]})
                elif stop["stationid"] == pass_point or (parse_local_time_cached(stop.get("actual_arrivaltime") or "") and not stop.get("actual_departuretime")):
                    cleaned_stops.extend([{"stop_id": s["stationid"]} for s in stops[i:]])
                elif parse_local_time(stop.get("actual_departuretime")):
                    cleaned_stops.append({
                        "stop_id": stop["stationid"],
                        "stop_loc": [stop["centerlat"], stop['centerlong']],
                        "time": from_hhmm(stop.get('actual_departuretime'))
                    })
            predicted_with_delays = pred_seg_cache_partial(data_input={
                "route_id": route_id,
                "trip_start": stops[0]["sch_departuretime"],
                "stops": cleaned_stops
            }, db_path=DB_PATH, target_date=datetime.now().strftime('%Y-%m-%d'))
            prediction_cache[(route_id, stops[0]['sch_departuretime'])] = {
                "pass_point": pass_point,
                "predictions": predicted_with_delays
            }
    else:
        if (route_id, stops[0]['sch_departuretime']) in prediction_cache:
            del prediction_cache[(route_id, stops[0]['sch_departuretime'])]
#     if pass_point:
#         print(f"""
# Determined Pass Point for trip_id {trip_id}: {pass_point}
# Following is returned stops:
# {json.dumps({"stops_arr": [{"stop_id": s["stationid"], "sch_arr": s['sch_arrivaltime'], "sch_dep": s['sch_departuretime'], "act_arr": s['actual_arrivaltime'], "act_dep": s['actual_departuretime']} for s in stops]}, indent=4)}
#     """)
    prev_act_arr, prev_sch_arr, prev_sch_dep, prev_act_dep = 0, 0, 0, 0
    for stop in stops:  # if stop hasnt been passed, then ensure scheduled_arrival_time is later than previous scheduled_arrival_time and is later than time.now()
        stop_id = str(stop.get("stationid", ""))
        if predicted_with_delays and str(stop_id) in predicted_with_delays:
            stop['actual_arrivaltime'] = predicted_with_delays[str(stop_id)]
            stop['actual_departuretime'] = predicted_with_delays[str(stop_id)]
        sch_arr = parse_local_time(stop.get("sch_arrivaltime"))
        sch_dep = parse_local_time(stop.get("sch_departuretime"))
        act_arr = parse_local_time(stop.get("actual_arrivaltime"))
        act_dep = parse_local_time(stop.get("actual_departuretime"))
        if act_arr:
            prev_act_arr = act_arr
        if act_dep:
            prev_act_dep = act_dep
        if not sch_arr and not sch_dep:
            continue  # skip if we don’t even have scheduled arrival
        sch_dep = sch_dep if sch_dep else sch_arr
        sch_arr = sch_arr if sch_arr else sch_dep
        act_arr = act_arr if act_arr else prev_act_arr + (sch_arr - prev_sch_arr) # Add proper delays for "Skipped" stops
        act_dep = act_dep if act_dep else prev_act_dep + (sch_dep - prev_sch_dep)
        prev_sch_arr = sch_arr
        prev_sch_dep = sch_dep
        act_arr = act_arr  # Will be 0 if the bus has passed this stop or stop hasn't been reached but bus is expected to be on time
        act_dep = act_dep 

        act_dep = act_arr if act_dep < act_arr else act_dep

        # if pass_point == stop_id or delay != 0: # Add delays if the bus hasn't passed the stop
        #     while act_dep < datetime.now().timestamp() or act_arr < datetime.now().timestamp() or act_arr < last_act_dep:
        #         act_arr += 120
        #         act_dep += 120
        #         delay += 120

        stu = trip_update.stop_time_update.add()
        stu.stop_id = stop_id

        stu.arrival.time = act_arr if act_arr else sch_arr
        if act_arr:
            stu.arrival.delay = int(act_arr - sch_arr)

        if sch_dep:
            stu.departure.time = act_dep if act_dep else sch_dep
            if act_dep:
                stu.departure.delay = int(act_dep - sch_dep)
    trip_completed = False
    if stops:
        last_stop = stops[-1]
        if last_stop.get("orig_actual_arrivaltime") and last_stop.get("orig_actual_departuretime"):
            trip_completed = True
            if (route_id, stops[0]['sch_departuretime']) in prediction_cache:
                prediction_cache.pop((route_id, stops[0]['sch_departuretime']))
            for stop in stops:
                if not stop.get("orig_actual_arrivaltime") or not stop.get("orig_actual_departuretime"):
                    continue  # only save fully completed stops

                insert_vehicle_data({
                    "stop_id": str(stop.get("stationid", "")),
                    "trip_id": str(trip_id),
                    "route_id": str(route_id),
                    "date": str(date.today()),
                    "actual_arrival": stop.get("orig_actual_arrivaltime"),
                    "actual_departure": stop.get("orig_actual_departuretime"),
                    "scheduled_arrival": stop.get("orig_sch_arrivaltime"),
                    "scheduled_departure": stop.get("orig_sch_departuretime")
                })
    # Vehicle position
    vehicle_position = entity.vehicle
    vehicle_position.trip.CopyFrom(trip_update.trip)
    vehicle_position.vehicle.id = str(vehicle["vehicleid"])
    vehicle_position.vehicle.label = vehicle.get("vehiclenumber", "")
    vehicle_position.position.latitude = float(vehicle.get("centerlat", 0.0))
    vehicle_position.position.longitude = float(vehicle.get("centerlong", 0.0))
    vehicle_position.position.bearing = float(vehicle.get("heading", 0.0))
    vehicle_position.timestamp = int(datetime.strptime(vehicle['lastrefreshon'], '%d-%m-%Y %H:%M:%S').timestamp())

    # Persist unique vehicle position for ongoing trips (trip not complete)
    try:
        if not trip_completed:
            insert_vehicle_position({
                "vehicle_id": str(vehicle.get("vehicleid")),
                "trip_id": str(trip_id),
                "route_id": str(route_id),
                "lat": float(vehicle.get("centerlat", 0.0)),
                "lon": float(vehicle.get("centerlong", 0.0)),
                "bearing": float(vehicle.get("heading", 0.0)),
                "timestamp": vehicle_position.timestamp,
                "speed": float(vehicle.get("speed", 0.0)) if vehicle.get("speed") is not None else None,
                "status": vehicle.get("status")
            })
    except Exception:
        traceback.print_exc()
    # print("BUILDING FEED ENTITY 9")
    # print(f"TIME TAKEN TO BUILD FEED ENTITY {time.time() - ex}")
    return entity


def parse_local_time(hhmm: str) -> int or None:
    if not hhmm or ":" not in hhmm:
        return None
    try:
        hh, mm = map(int, hhmm.split(":"))
        now = datetime.now(local_tz)
        t = now.replace(hour=hh, minute=mm, second=0, microsecond=0)

        # If parsed time is too far in the past, assume next day
        if t < now - timedelta(hours=6):
            t += timedelta(days=1)
        # If parsed time is too far in the future, assume previous day
        if t > now + timedelta(hours=6):
            t -= timedelta(days=1)
        return int(t.timestamp())
    except Exception:
        return None

def int_time_to_minutes(int_time: int) -> int:
    """
    Convert int_time format to total minutes from midnight.
    Examples: 1405 -> 845 minutes (14*60 + 5), 5 -> 5 minutes (0*60 + 5), 115 -> 75 minutes (1*60 + 15)
    """
    if int_time < 100:
        # Single or double digit means minutes only (e.g., 5 -> 00:05, 55 -> 00:55)
        return int_time
    else:
        # Three or four digits: HHMM format
        hours = int_time // 100
        minutes = int_time % 100
        return hours * 60 + minutes

def calculate_static_predictions(route_key: str, trip_start: str, stops: list, pass_point: str = None, last_act_dep: int = 0) -> dict:
    """
    Calculate predictions using static times from the times dictionary.
    This replaces expensive predict_times calls with lightweight static schedule calculations.
    Handles midnight crossings properly (e.g., 23:30 -> 00:30).
    """
    if not route_key:
        return {}
    # Build candidate trip lists from static 'times' and bounded 'static_enriched_times'
    source_trips = []
    try:
        if route_key in times and times[route_key]:
            source_trips.extend(list(times[route_key]))
    except Exception:
        pass
    try:
        enriched = static_enriched_times.get(route_key, None)
        if enriched:
            source_trips.extend(list(enriched))
    except Exception:
        pass
    if not source_trips:
        return {}
    
    # Find matching trip in static times
    times_by_start = {}
    for trip in source_trips:
        try:
            # 'start' is HHMM int; normalize to HH:MM string key
            times_by_start[to_hhmm(trip['start'])] = trip
        except Exception:
            continue
    if trip_start not in times_by_start:
        # Find nearest start time and calculate time difference
        if not times_by_start:
            return {}
        
        # Convert trip_start to minutes for comparison
        try:
            trip_start_minutes = from_hhmm(trip_start)
        except:
            return {}
        
        # Find the closest start time by comparing time differences
        nearest_start = None
        min_diff = float('inf')
        
        for available_start in times_by_start.keys():
            try:
                available_start_minutes = from_hhmm(available_start)
                # Calculate absolute time difference, handling midnight crossings
                diff = abs(available_start_minutes - trip_start_minutes)
                # Handle midnight crossing (e.g., 23:50 vs 00:10 should be 20 min, not 1420 min)
                if diff > 12 * 60:  # More than 12 hours suggests midnight crossing
                    diff = 24 * 60 - diff  # Use the shorter path around midnight
                
                if diff < min_diff:
                    min_diff = diff
                    nearest_start = available_start
            except:
                continue
        
        if not nearest_start or min_diff > 60:  # Don't use if difference is more than 1 hour
            return {}
        
        static_trip = times_by_start[nearest_start]
        time_offset_minutes = trip_start_minutes - from_hhmm(nearest_start)
        
        # Apply time offset to all stops in the static trip
        adjusted_stops_map = {}
        for stop in static_trip['stops']:
            original_time = stop['stop_time']
            adjusted_time = original_time + time_offset_minutes
            # Ensure times stay within valid 24-hour range
            adjusted_time = adjusted_time % (24 * 60)
            adjusted_stops_map[str(stop['stop_id'])] = adjusted_time
        
        static_stops_map = adjusted_stops_map
    else:
        static_trip = times_by_start[trip_start]
        static_stops_map = {str(stop['stop_id']): stop['stop_time'] for stop in static_trip['stops']}
    
    # If no pass point, return static times converted to HH:MM format
    if not pass_point:
        return {stop_id: to_hhmm(stop_time) for stop_id, stop_time in static_stops_map.items()}
    
    # Calculate durations between consecutive stops from static schedule using proper int_time conversion
    static_durations = {}
    static_stop_list = static_trip['stops']
    for i in range(len(static_stop_list) - 1):
        current_stop = static_stop_list[i]
        next_stop = static_stop_list[i + 1]
        
        # Convert int_time to minutes properly, handling midnight crossings
        current_minutes = int_time_to_minutes(current_stop['stop_time'])
        next_minutes = int_time_to_minutes(next_stop['stop_time'])
        
        duration_minutes = next_minutes - current_minutes
        # Handle midnight crossings (e.g., 23:55 -> 00:05 should be 10 minutes, not -1435)
        if duration_minutes < 0:
            duration_minutes += 24 * 60  # Add 24 hours worth of minutes
        
        # Sanity check: duration should be reasonable (1 min to 3 hours)
        if duration_minutes < 1:
            duration_minutes = 1
        elif duration_minutes > 180:  # More than 3 hours suggests calculation error
            duration_minutes = 5  # Default fallback
        
        static_durations[(str(current_stop['stop_id']), str(next_stop['stop_id']))] = duration_minutes
    
    # Find pass point index in our stops list
    pass_point_idx = None
    passing = False
    for i, stop in enumerate(stops):
        if str(stop.get('stop_id', '')) == pass_point:
            pass_point_idx = i
            break
    
    if pass_point_idx is None:
        return {}
    
    # Start predictions from the last actual departure time, ensuring it's at least current time + 1 minute
    current_time = last_act_dep
    result = {}
    
    # Calculate predictions for all stops from pass point onwards
    for i in range(pass_point_idx, len(stops)):
        stop = stops[i]
        stop_id = str(stop.get('stop_id', ''))
        
        if i == pass_point_idx:
            # For pass point, start from current_time
            if i > 0:
                prev_stop_id = str(stops[i-1].get('stationid', ''))
                duration_key = (prev_stop_id, stop_id)
                
                if duration_key in static_durations:
                    duration_minutes = static_durations[duration_key]
                    current_time += duration_minutes * 60  # Convert to seconds
                else:
                    # Fallback: add 2 minutes if no static duration available
                    current_time += 120  # 2 minutes
            if current_time < datetime.now().timestamp():
                current_time = datetime.now().timestamp() + 60
            result[stop_id] = datetime.fromtimestamp(current_time).strftime("%H:%M")
        else:
            # Calculate duration from previous stop using static schedule
            prev_stop_id = str(stops[i-1].get('stop_id', ''))
            duration_key = (prev_stop_id, stop_id)
            
            if duration_key in static_durations:
                duration_minutes = static_durations[duration_key]
                current_time += duration_minutes * 60  # Convert to seconds
            else:
                # Fallback: add 2 minutes if no static duration available
                current_time += 120  # 2 minutes
            
            # Ensure time is always progressing forward
            result[stop_id] = datetime.fromtimestamp(current_time).strftime("%H:%M")
    return result

def pred_seg_cache_partial(data_input, db_path, target_date):
    """
    Optimized prediction using static schedule data instead of expensive predict_times calls.
    """

    route_id = str(data_input.get('route_id', ''))
    stops = data_input['stops'].copy()
    
    # Get route key for static times lookup
    routename_by_id = get_cached_routename_by_id()
    route_key = routename_by_id.get(str(route_id))
    
    if not route_key:
        return {}
    
    # Find pass point (first stop without actual time)
    pass_point = None
    passing = False
    last_act_dep = 0
    
    for stop in stops:
        if 'time' in stop:
            last_act_dep = max(last_act_dep, stop['time'])
            passing = False
        elif not passing:
            pass_point = str(stop.get('stop_id', ''))
            passing = True
    
    # Use static predictions instead of expensive predict_times calls
    trip_start = data_input.get('trip_start', '')
    static_predictions = calculate_static_predictions(route_key, trip_start, stops, pass_point, last_act_dep)
    return static_predictions
