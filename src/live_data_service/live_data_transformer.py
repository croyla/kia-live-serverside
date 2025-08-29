import json
import traceback

from google.transit import gtfs_realtime_pb2
from datetime import datetime, timedelta
import pytz

from src.shared.config import DB_PATH
from src.shared.db import insert_vehicle_data, insert_vehicle_position
from src.shared.utils import to_hhmm, from_hhmm
from datetime import date


from src.shared import ThreadSafeDict, times, routes_children, predict_times, prediction_cache, route_stops

local_tz = pytz.timezone("Asia/Kolkata")
all_entities = ThreadSafeDict()


def transform_response_to_feed_entities(api_data: list, job: dict) -> list:
    route_id = job["route_id"]
    trip_time = job["trip_time"]
    trip_id = job["trip_id"]
    match_window = timedelta(minutes=1)
    # print(f"Received processing API DATA {api_data}")
    vehicle_groups = {}
    vehicles = set()
    for stop in api_data:
        if str(stop.get("routeid")) != str(route_id):
            continue

        vehicle_list = stop.get("vehicleDetails", [])
        for vehicle in vehicle_list:
            vehicle_id = str(vehicle.get("vehicleid"))
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
    all_entities.pop(trip_id)
    # Step 2: Build GTFS-RT FeedEntities (one per vehicle)
    for vehicle_id, bundle in vehicle_groups.items():
        try:
            entity = build_feed_entity(bundle["vehicle"], trip_id, route_id, bundle["stops"])
            all_entities[trip_id] = entity
        except:
            traceback.print_exc()
    return all_entities.values()

def build_feed_entity(vehicle: dict, trip_id: str, route_id: str, stops: list):
    # print('vehicle', vehicle)
    # print('route_id', route_id)
    # print('trip_id', trip_id)
    # print('stops', stops)
    entity = gtfs_realtime_pb2.FeedEntity()
    entity.id = f"veh_{vehicle['vehicleid']}"
    routename_by_id = {str(v): k for k, v in routes_children.items()}
    trip_times = None
    if stops:
        if route_id in routename_by_id:
            if routename_by_id[route_id] in times:
                times_by_start = {to_hhmm(trip['start']): trip for trip in times[routename_by_id[route_id]]}
                if stops[0]['sch_departuretime'] in times_by_start:
                    try:
                        trip_times = {str(stop['stop_id']): to_hhmm(stop['stop_time']) for stop in times_by_start[stops[0]['sch_departuretime']]['stops']}
                        for idx, stop in enumerate(stops):
                            if str(stop['stationid']) not in trip_times:
                                stops.pop(idx)
                    except:
                        traceback.print_exc()
                        print(times_by_start[stops[0]['sch_departuretime']]['stops'], 'times')
            else:
                times[routename_by_id[route_id]] = []
            static_feed_stop_ids = {str(s["stop_id"]) for s in route_stops[routename_by_id[route_id]]["stops"]}
            stops = [stop for stop in stops if str(stop["stationid"]) in static_feed_stop_ids]


    trip_update = entity.trip_update
    trip_update.trip.trip_id = trip_id
    trip_update.trip.route_id = str(route_id)
    trip_update.vehicle.id = str(vehicle["vehicleid"])
    trip_update.vehicle.label = vehicle.get("vehiclenumber", "")
    # print(f"Transforming feed entity from API data for vehicle {trip_update.vehicle.label}, route id {route_id}, {trip_id}")
    pass_point = 0
    last_act_dep = 0
    predicted_scheduled = None # Run predictions against scheduled times if missing in times object
    if not trip_times:
        pred_time = predict_times.predict_stop_times_segmental(data_input=[{
            "route_id": str(route_id),
            "trip_start": stops[0]['sch_departuretime'],
            "stops": [{
                "stop_id": s["stationid"],
                "stop_loc": [s["centerlat"], s["centerlong"]]
            } for s in stops]
        }], db_path=DB_PATH, output_path=None, target_date=datetime.now().strftime('%Y-%m-%d'))
        predicted_scheduled = {str(stop['stop_id']): stop['stop_time'] for stop in pred_time[0]['stops']}
        times[routename_by_id[route_id]].append({
            "start": from_hhmm(stops[0]['sch_departuretime']),
            "stops": [{"stop_id": s["stationid"], "stop_time": predicted_scheduled[str(s["stationid"])]} for s in
                      stops]
        })
    for stop in stops: # Get the last stop the bus passed, so that we can add delays necessary for stops after the last stop
        # Keep original values for data prediction database
        stop["orig_sch_arrivaltime"], stop["orig_sch_departuretime"], stop["orig_actual_arrivaltime"], stop["orig_actual_departuretime"] = stop["sch_arrivaltime"], stop["sch_departuretime"], stop["actual_arrivaltime"], stop["actual_departuretime"]
        stop["stationid"] = str(stop['stationid']) # Normalise all values to string, instead of having integers
        act_arr = parse_local_time(stop.get("actual_arrivaltime"))
        act_dep = parse_local_time(stop.get("actual_departuretime"))
        if trip_times: # If we have trip times replace scheduled times with our own
            if str(stop.get("stationid")) in trip_times:
                stop['sch_arrivaltime'] = trip_times[stop.get("stationid")]
                stop['sch_departuretime'] = trip_times[stop.get("stationid")]
            else:
                if not predicted_scheduled:
                    pred_time = predict_times.predict_stop_times_segmental(data_input=[{
                        "route_id": str(route_id),
                        "trip_start": stops[0]['sch_departuretime'],
                        "stops": [{
                            "stop_id": s["stationid"],
                            "stop_loc": [s["centerlat"], s["centerlong"]]
                        } for s in stops]
                    }], db_path=DB_PATH, output_path=None, target_date=datetime.now().strftime('%Y-%m-%d'))
                    predicted_scheduled = {str(stop['stop_id']): stop['stop_time'] for stop in pred_time[0]['stops']}
                    times[routename_by_id[route_id]].append({
                        "start": from_hhmm(stops[0]['sch_departuretime']),
                        "stops": [{"stop_id": s["stationid"], "stop_time": predicted_scheduled[s["stationid"]]} for s in
                                  stops]
                    })
                stop['sch_arrivaltime'] = to_hhmm(predicted_scheduled[stop.get("stationid")])
                stop['sch_departuretime'] = to_hhmm(predicted_scheduled[stop.get("stationid")])
        else:
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
            # print(f"Bypassing predicting delays with trip {trip_id}")
            predicted_with_delays = prediction_cache[(route_id, stops[0]['sch_departuretime'])]['predictions']
        else:
            cleaned_stops = []
            for i, stop in enumerate(stops): # Clean stops of skipped stops
                if i == 0:
                    # print(stop)
                    # print('timecheck', stop.get('actual_departuretime') or stop.get('sch_departuretime') or stop.get('sch_arrivaltime'))
                    cleaned_stops.append({
                        "stop_id": stop["stationid"],
                        "time": from_hhmm(s=stop.get('actual_departuretime') or stop.get('sch_departuretime') or stop.get('sch_arrivaltime')),
                        "stop_loc": [stop["centerlat"], stop["centerlong"]]
                    })
                elif stop["stationid"] == pass_point or (parse_local_time(stop.get("actual_arrivaltime")) and not stop.get("actual_departuretime")):
                    cleaned_stops.extend([{"stop_id": s["stationid"]} for s in stops[i:]])
                elif parse_local_time(stop.get("actual_departuretime")):
                    cleaned_stops.append({
                        "stop_id": stop["stationid"],
                        "stop_loc": [stop["centerlat"], stop['centerlong']],
                        "time": from_hhmm(stop.get('actual_departuretime'))
                    })
            predicted_with_delays = {str(s["stop_id"]): to_hhmm(s["stop_time"]) for s in predict_times.predict_stop_times_partial(data_input=[{
                "route_id": route_id,
                "trip_start": stops[0]["sch_departuretime"],
                "stops": cleaned_stops
                # Convert cleaned stops to compatible format, with actual_arrival used where available
            }], db_path=DB_PATH, output_path=None, target_date=datetime.now().strftime('%Y-%m-%d'))[0]['stops']}
            prediction_cache[(route_id, stops[0]['sch_departuretime'])] = {
                "pass_point": pass_point,
                "predictions": predicted_with_delays
            }
    else:
        if (route_id, stops[0]['sch_departuretime']) in prediction_cache:
            prediction_cache.pop((route_id, stops[0]['sch_departuretime']))
    delay = 0
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
        act_arr = act_arr + delay # Will be 0 if the bus has passed this stop or stop hasn't been reached but bus is expected to be on time
        act_dep = act_dep + delay

        act_dep = act_arr if act_dep < act_arr else act_dep

        if pass_point == stop_id or delay != 0: # Add delays if the bus hasn't passed the stop
            while act_dep < datetime.now().timestamp() or act_arr < datetime.now().timestamp() or act_arr < last_act_dep:
                act_arr += 120
                act_dep += 120
                delay += 120

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
        if t > now - timedelta(hours=6):
            t -= timedelta(days=1)
        return int(t.timestamp())
    except Exception:
        return None

