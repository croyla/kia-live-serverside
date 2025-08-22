import hashlib
from urllib import parse
from datetime import datetime, timedelta
from src.shared.utils import (
    decode_polyline,
    interpolate_trip_times, add_time_trip_times
)


def build_gtfs_dataset(input_data: dict) -> dict:
    """
    Returns a dictionary of GTFS files: { 'agency.txt': [...], ... }
    """
    client_stops = input_data["client_stops"]
    routes_children = input_data["routes_children"]
    routes_parent = input_data["routes_parent"]
    start_times = input_data["start_times"]
    routelines = input_data["routelines"]
    times = input_data["times"]

    all_stops, stop_id_map, translations = build_stops(client_stops)
    agency = build_agency()
    feed_info = build_feed_info()
    calendar = build_calendar()
    routes, route_translations = build_routes(client_stops, routes_children)
    shapes, routes_shapes_map = build_shapes(routelines, routes_children)
    trips, stop_times, trip_translations = build_trips_and_stop_times(
        client_stops, start_times, times, routes_children, stop_id_map, routes_shapes_map
    )

    translations.extend(route_translations)
    translations.extend(trip_translations)

    return {
        "agency.txt": agency,
        "feed_info.txt": feed_info,
        "calendar.txt": calendar,
        "routes.txt": routes,
        "shapes.txt": shapes,
        "stops.txt": all_stops,
        "trips.txt": trips,
        "stop_times.txt": stop_times,
        "translations.txt": translations,
    }


def build_agency():
    return [{
        "agency_id": "BMTC",
        "agency_name": "Bengaluru Metropolitan Transport Corporation",
        "agency_url": "https://mybmtc.karnataka.gov.in/",
        "agency_timezone": "Asia/Kolkata",
        "agency_phone": "7760991269",
        "agency_fare_url": "https://nammabmtcapp.karnataka.gov.in/commuter/fare-calculator"
    }]


def build_feed_info():
    now = datetime.now()
    return [{
        "feed_publisher_name": "Bengawalk",
        "feed_publisher_url": "https://bengawalk.com/",
        "feed_contact_email": "hello@bengawalk.com",
        "feed_lang": "en",
        "feed_version": hashlib.md5(str(datetime.now()).encode()).hexdigest()[:8],
        "feed_start_date": now.strftime("%Y%m%d"),
        "feed_end_date": (now + timedelta(days=365)).strftime("%Y%m%d")
    }]


def build_calendar():
    now = datetime.now()
    return [{
        "service_id": "ALL",
        "monday": '1', "tuesday": '1', "wednesday": '1', "thursday": '1',
        "friday": '1', "saturday": '1', "sunday": '1',
        "start_date": now.strftime("%Y%m%d"),
        "end_date": (now + timedelta(days=365)).strftime("%Y%m%d")
    }]


def build_routes(client_stops, routes_children):
    routes = []
    translations = []

    for key, route_id in routes_children.items():
        route_short = key.replace(" UP", "").replace(" DOWN", "")
        if key not in client_stops:
            continue
        stops = client_stops[key]["stops"]
        route_long = f"{stops[0]['name']} to {stops[-1]['name']}"
        route_long_kn = f"{stops[0]['name_kn']} ಇಂದ {stops[-1]['name_kn']} ಇಗೆ"

        routes.append({
            "route_id": str(route_id),
            "route_short_name": route_short,
            "route_long_name": route_long,
            "route_type": '3',  # Bus
            "agency_id": "BMTC"
        })

        translations.append({
            "table_name": "routes",
            "field_name": "route_long_name",
            "record_id": str(route_id),
            "language": "kn",
            "translation": route_long_kn
        })

    return routes, translations


def build_shapes(routelines, routes_children):
    shapes = []
    routes_shapes_map = {}
    for key, polyline in routelines.items():
        if key not in routes_children:
            continue
        shape_id = f"sh_{routes_children[key]}"
        routes_shapes_map[routes_children[key]] = shape_id
        points = decode_polyline(parse.unquote(polyline, encoding='utf-8', errors='replace'))
        for i, (lat, lon) in enumerate(points):
            shapes.append({
                "shape_id": shape_id,
                "shape_pt_lat": str(lat),
                "shape_pt_lon": str(lon),
                "shape_pt_sequence": str(i + 1)
            })
    return shapes, routes_shapes_map


def build_stops(client_stops):
    seen = {}
    stops = []
    translations = {}
    stop_id_map = {}  # (lat, lon, name) → stop_id
    appended = set()

    for route_data in client_stops.values():
        for stop in route_data["stops"]:
            key = (round(stop["loc"][0], 6), round(stop["loc"][1], 6), stop["name"]) if not 'stop_id' in stop else stop['stop_id']
            if key in seen:
                if "stop_id" in stop:
                    stop_id_map[key] = stop["stop_id"]
                continue

            stop_id = stop.get("stop_id") or f"gen_{len(stops) + 1}"
            stop_id_map[key] = stop_id
            if stop_id in appended:
                continue
            seen[key] = True

            stops.append({
                "stop_id": str(stop_id),
                "stop_name": stop["name"],
                "stop_lat": str(stop["loc"][0]),
                "stop_lon": str(stop["loc"][1]),
            })

            translations[stop_id] = {
                "table_name": "stops",
                "field_name": "stop_name",
                "record_id": str(stop_id),
                "language": "kn",
                "translation": stop["name_kn"]
            }
            appended.add(stop_id)

    return stops, stop_id_map, list(translations.values())


def build_trips_and_stop_times(client_stops, start_times, times_data, routes_children, stop_id_map, routes_shapes_map):
    trips = []
    stop_times = []
    translations = []

    for route_key, route_id in routes_children.items():
        if route_key not in client_stops:
            continue
        stops = client_stops[route_key]["stops"]
        stop_points = [
            (
                stop_id_map[(round(s["loc"][0], 6), round(s["loc"][1], 6), s["name"])] if 'stop_id' not in s else s['stop_id'],
                s["distance"],
                s["name"]
            )
            for s in stops
        ]
        stop_points.sort(key=lambda x: x[1])  # sort by distance

        route_trips = times_data.get(route_key) or []
        fallback_trips = start_times.get(route_key) or []

        if not route_trips:
            route_trips = [
                {"start": t["start"], "stops": None, "duration": t["duration"]}
                for t in fallback_trips
            ]

        used_ids = set()
        for i, trip_data in enumerate(route_trips):
            trip_start = trip_data["start"]
            trip_duration = trip_data.get("duration") or fallback_trips[i]["duration"]
            trip_index = 1
            while f"{route_id}_{trip_index}" in used_ids:
                trip_index += 1
            trip_id = f"{route_id}_{trip_index}"
            used_ids.add(trip_id)

            trips.append({
                "trip_id": trip_id,
                "route_id": str(route_id),
                "shape_id": routes_shapes_map[route_id],
                "service_id": "ALL"
            })

            times = trip_data.get("stops")
            if not times:
                times = interpolate_trip_times(
                    trip_start, trip_duration, stop_points
                )
            else:
                times = {s['stop_id']: s['stop_time'] for s in times}
            prev_stop_id = None
            for j, (stop_id, distance, name) in enumerate(stop_points):
                dep_time = times[stop_id]
                prev_dep = times[prev_stop_id] if j != 0 else None
                prev_stop_id = stop_id
                if prev_dep == dep_time:
                    times[stop_id] = add_time_trip_times(dep_time, 1)
                    dep_time = add_time_trip_times(dep_time, 1)
                dep_time_str = f"{dep_time // 100:02d}:{dep_time % 100:02d}:10"
                arr_time_str = f"{dep_time // 100:02d}:{dep_time % 100:02d}:00"
                stop_times.append({
                    "trip_id": trip_id,
                    "stop_id": str(stop_id),
                    "stop_sequence": str(j + 1),
                    "departure_time": dep_time_str,
                    "arrival_time": arr_time_str,
                    "timepoint": str(1 if j == 0 or j == len(stop_points) - 1 else 0)
                })

            translations.append({
                "table_name": "trips",
                "field_name": "trip_headsign",
                "record_id": trip_id,
                "language": "kn",
                "translation": stop_points[-1][2]  # last stop name
            })

    return trips, stop_times, translations
