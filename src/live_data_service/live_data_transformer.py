from google.transit import gtfs_realtime_pb2
from datetime import datetime, timedelta
import pytz
from src.shared.db import insert_vehicle_data
from datetime import date


from src.shared import ThreadSafeDict

local_tz = pytz.timezone("Asia/Kolkata")
all_entities = ThreadSafeDict()


def transform_response_to_feed_entities(api_data: list, job: dict) -> list:
    route_id = job["route_id"]
    trip_time = job["trip_time"]
    trip_id = job["trip_id"]
    match_window = timedelta(minutes=2)

    vehicle_groups = {}

    for stop in api_data:
        if str(stop.get("routeid")) != str(route_id):
            continue

        vehicle_list = stop.get("vehicleDetails", [])
        for vehicle in vehicle_list:
            vehicle_id = str(vehicle.get("vehicleid"))
            if not vehicle_id:
                continue

            sch_time_str = vehicle.get("sch_tripstarttime")
            if not sch_time_str:
                continue

            try:
                hh, mm = map(int, sch_time_str.split(":"))
                sch_trip_time = trip_time.replace(hour=hh, minute=mm, second=0, microsecond=0)
            except Exception:
                continue

            if abs(sch_trip_time - trip_time) > match_window:
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
        entity = build_feed_entity(bundle["vehicle"], trip_id, route_id, bundle["stops"])
        all_entities[trip_id] = entity
    return all_entities.values()


def build_feed_entity(vehicle: dict, trip_id: str, route_id: str, stops: list):
    entity = gtfs_realtime_pb2.FeedEntity()
    entity.id = f"veh_{vehicle['vehicleid']}"

    trip_update = entity.trip_update
    trip_update.trip.trip_id = trip_id
    trip_update.trip.route_id = str(route_id)
    trip_update.vehicle.id = str(vehicle["vehicleid"])
    trip_update.vehicle.label = vehicle.get("vehiclenumber", "")

    for stop in stops:
        stop_id = str(stop.get("stationid", ""))
        sch_arr = parse_local_time(stop.get("sch_arrivaltime"))
        sch_dep = parse_local_time(stop.get("sch_departuretime"))
        act_arr = parse_local_time(stop.get("actual_arrivaltime"))
        act_dep = parse_local_time(stop.get("actual_departuretime"))

        if not sch_arr:
            continue  # skip if we don’t even have scheduled arrival

        stu = trip_update.stop_time_update.add()
        stu.stop_id = stop_id

        stu.arrival.time = act_arr if act_arr else sch_arr
        if act_arr:
            stu.arrival.delay = int(act_arr - sch_arr)

        if sch_dep:
            stu.departure.time = act_dep if act_dep else sch_dep
            if act_dep:
                stu.departure.delay = int(act_dep - sch_dep)

    if stops:
        last_stop = stops[-1]
    if last_stop.get("actual_arrivaltime") and last_stop.get("actual_departuretime"):
        for stop in stops:
            if not stop.get("actual_arrivaltime") or not stop.get("actual_departuretime"):
                continue  # only save fully completed stops

            insert_vehicle_data({
                "stop_id": str(stop.get("stationid", "")),
                "trip_id": str(trip_id),
                "route_id": str(route_id),
                "date": str(date.today()),
                "actual_arrival": stop.get("actual_arrivaltime"),
                "actual_departure": stop.get("actual_departuretime"),
                "scheduled_arrival": stop.get("sch_arrivaltime"),
                "scheduled_departure": stop.get("sch_departuretime")
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

        return int(t.timestamp())
    except Exception:
        return None


