import datetime
import os
import json
import sys

import requests
import polyline
import urllib.parse
import geopy.distance

from old_src.live_data_service.live_data_transformer import parse_local_time
from old_src.shared import predict_times
from old_src.shared.utils import to_hhmm, from_hhmm

# Directory setup
GENERATED_IN_DIR = 'generated_in'
if not os.path.exists(GENERATED_IN_DIR):
    os.makedirs(GENERATED_IN_DIR)

# API endpoints and headers
SEARCH_ROUTE_URL = "https://bmtcmobileapi.karnataka.gov.in/WebAPI/SearchRoute_v2/"
ROUTE_DETAILS_URL = "https://bmtcmobileapi.karnataka.gov.in/WebAPI/SearchByRouteDetails_v4/"
GET_ALL_ROUTES_URL = "https://bmtcmobileapi.karnataka.gov.in/WebAPI/GetAllRouteList/"
ROUTE_POINTS_URL = "https://bmtcmobileapi.karnataka.gov.in/WebAPI/RoutePoints/"
SCHEDULE_URL = "https://bmtcmobileapi.karnataka.gov.in/WebAPI/GetTimetableByRouteid_v3/"
HEADERS = {
    'Accept': '*/*',
    'User-Agent': 'BrowsePy/1.0',
    'deviceType': 'WEB',
    'lan': 'en',
    'Content-Type': 'application/json'
}

# Function to query the API and return JSON data
def query_api(url, data=None):
    headers = HEADERS if data is not None else {'Content-Length': '0', **HEADERS}
    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        return response.json()
    else:
        print(data)
        print(url)
        print(headers)
        response.raise_for_status()
def save_child_routes():
    # Query the GetAllRouteList endpoint and filter for routes with routeno starting with "KIA-"
    all_routes_data = query_api(GET_ALL_ROUTES_URL)
    kia_routes = [route for route in all_routes_data['data'] if route['routeno'].startswith('KIA-')]

    # Save route children IDs to a JSON file
    route_children_ids = {route['routeno']: route['routeid'] for route in kia_routes}
    with open(os.path.join(GENERATED_IN_DIR, 'route_children_ids.json'), 'w') as f:
        json.dump(route_children_ids, f, indent=4)
    print("Route children IDs have been saved to JSON file.")

def save_route_parent_ids():
    # Read the 'in/routes_parent_ids.json' file
    with open('in/routes_parent_ids.json', 'r') as f:
        routes_parent_ids = json.load(f)

    # Read the 'generated_in/route_children_ids.json' file
    with open(os.path.join(GENERATED_IN_DIR, 'route_children_ids.json'), 'r') as f:
        route_children_ids = json.load(f)

    # Query the provided curl endpoint
    data = {"routetext": "KIA-"}
    response = query_api(SEARCH_ROUTE_URL, data=data)
    routes_data = response['data']

    # Create a dictionary to store the generated routeparentids
    generated_routeparentids = {}

    # Iterate over the routes_data and check for the presence of routeparentids
    for route in routes_data:
        routeno = route['routeno']
        routeparentid = route['routeparentid']

        # Add the required directions to the generated routeparentids
        for direction in [' UP', ' DOWN']:
            route_with_direction = routeno + direction
            if route_with_direction in route_children_ids:
                generated_routeparentids[route_with_direction] = routeparentid
            else:
                print(f"Warning: Route {route_with_direction} does not have a children id, skipping saving routeparentid.")

    # Save the generated routeparentids to a JSON file
    with open(os.path.join(GENERATED_IN_DIR, 'route_parent_ids.json'), 'w') as f:
        json.dump(generated_routeparentids, f, indent=4)

    print("Generated routeparentids have been saved to JSON file.")
def generate_client_stops():
    if '-s' not in sys.argv:
        print("Not saving stops, as -s flag is missing.")
        return
    # Read route IDs
    with open(os.path.join(GENERATED_IN_DIR, 'route_parent_ids.json'), 'r') as f:
        route_parent_ids = json.load(f)
    with open(os.path.join(GENERATED_IN_DIR, 'route_children_ids.json'), 'r') as f:
        route_children_ids = json.load(f)
    route_children = {y: x for x, y in route_children_ids.items()}
    # Read existing stops
    with open(os.path.join('in', 'client_stops.json'), 'r', encoding='utf-8') as f:
        client_stops_orig = json.load(f)
    orig_stop_loc = {stop['stop_id']: stop['loc'] for dist_stops in client_stops_orig.values() for stop in dist_stops['stops']}
    orig_kn = {stop['stop_id']: stop['name_kn'] for dist_stops in client_stops_orig.values() for stop in dist_stops['stops']}
    orig_phone = {}
    for dist_stops in client_stops_orig.values():
        for stop in dist_stops['stops']:
            if 'phone' in stop.keys() and not stop['phone'] == '':
                orig_phone[stop['stop_id']] = stop['phone']
            if 'contact' in stop.keys() and not stop['contact'] == '':
                orig_phone[stop['stop_id']] = stop['contact']
            elif stop['stop_id'] in orig_stop_loc:
                orig_stop_loc.pop(stop['stop_id'])

    client_stops = {}
    seen = set()
    for route, routeid in route_children_ids.items():
        routeparent = route_parent_ids[route]
        if routeparent in seen:
            continue
        seen.add(routeparent)
        data = {"routeid": routeparent, "servicetypeid": 0}
        api_response = query_api(ROUTE_DETAILS_URL, data)
        total_entries = []
        for d in ['up', 'down']:
            total_entries.extend(api_response[d]['data'])
        entries = {}
        for e in total_entries:
            if e['routeid'] not in entries:
                entries[e['routeid']] = []
            entries[e['routeid']].append(e)
        for k, v in entries.items():
            entries[k] = sorted(v, key=lambda x: x['distance_on_station'])
        # print(entries)
        for ch, stop_list in entries.items():
            total_distance = 0.0
            stops = []
            for stop_data in stop_list:
                if "FLY" in stop_data['stationname']: # Skip "FLY" stops, as these are flyover stops
                    continue
                stop = {
                    "distance": stop_data['distance_on_station'],
                    "name": stop_data['stationname'],
                    "name_kn": orig_kn[stop_data['stationid']] if stop_data['stationid'] in orig_kn else '',
                    "loc": orig_stop_loc[stop_data['stationid']] if stop_data['stationid'] in orig_stop_loc else [stop_data['centerlat'], stop_data['centerlong']],
                    "phone": orig_phone[stop_data['stationid']] if stop_data['stationid'] in orig_phone else '',
                    "stop_id": stop_data['stationid']
                }
                stops.append(stop)
                total_distance = max(total_distance, stop_data['distance_on_station'])
            client_stops[route_children[ch]] = {
                "stops": stops,
                "totalDistance": total_distance
            }
    with open(os.path.join(GENERATED_IN_DIR, 'client_stops.json'), 'w', encoding='utf-8') as f:
        json.dump(client_stops, f, indent=4, ensure_ascii=False)

    print("Client stops have been generated and saved to JSON file.")

def generate_google_polylines():
    if '-r' not in sys.argv:
        print('Skipping routelines.json, as -r flag is not provided.')
        return
    print("Saving routelines")
    with open(os.path.join(GENERATED_IN_DIR, 'route_children_ids.json'), 'r') as f:
        route_children_ids = json.load(f)
    polylines = {}
    for route, route_id in route_children_ids.items():
        response = query_api(ROUTE_POINTS_URL, {"routeid": route_id})
        if 'data' not in response:
            print(f"Skipping {route}")
            continue
        # Convert to float (lat, lon) tuples
        latlng = [(float(p["latitude"]), float(p["longitude"])) for p in response['data']]

        # Encode to polyline
        encoded_polyline = polyline.encode(latlng, geojson=True, precision=5)

        # Percent-encode the polyline
        percent_encoded = urllib.parse.quote(encoded_polyline, safe='')
        polylines[route] = percent_encoded

    with open(os.path.join(GENERATED_IN_DIR, 'routelines.json'), 'w') as f:
        json.dump(polylines, f, indent=4, ensure_ascii=False)

    print("Saved polylines to routelines.json")


def populate_distances():
    if '-r' not in sys.argv or '-s' not in sys.argv:
        return
    print("Populating missing distances with polyline + stop matches")


    def haversine(lat1, lon1, lat2, lon2):
        coords_1 = (lat1, lon1)
        coords_2 = (lat2, lon2)
        return geopy.distance.great_circle(coords_1, coords_2).km


    # Function to calculate the distance from a stop to the nearest point on the polyline
    def find_nearest_point_index(stop_lat, stop_lon, polyline):
        min_distance = float('inf')
        nearest_point_index = -1
        # Find the closest polyline point by calculating distance to each polyline point
        for i, (point_lat, point_lon) in enumerate(polyline):
            distance = haversine(stop_lat, stop_lon, point_lat, point_lon)
            if distance < min_distance:
                min_distance = distance
                nearest_point_index = i

        return nearest_point_index


    with open(os.path.join(GENERATED_IN_DIR, 'client_stops.json'), 'r') as f:
        client_stops = json.load(f)
    with open(os.path.join(GENERATED_IN_DIR, 'routelines.json'), 'r') as f:
        routelines = json.load(f)
    for routename, route in client_stops.items():
        if route['totalDistance'] != 0.0:
            continue
        if routename not in routelines:
            continue
        stops = route['stops']
        line = polyline.decode(urllib.parse.unquote(routelines[routename], encoding='utf-8', errors='replace'))
        distances = [0]  # Initialize first stop's distance as 0
        total_distance = 0
        for i in range(1, len(stops)):
            stop_a, stop_b = stops[i-1], stops[i]
            for j in range(find_nearest_point_index(stop_a['loc'][1], stop_a['loc'][0], line), find_nearest_point_index(stop_b['loc'][1], stop_b['loc'][0], line)):
                point_a = line[j]
                point_b = line[j + 1]

                # Calculate the distance between consecutive polyline points
                total_distance += haversine(point_a[0], point_a[1], point_b[0], point_b[1])

            distances.append(total_distance)  # Append the cumulative distance to the stop

        for i, stop in enumerate(stops):
            stop["distance"] = distances[i]
        route['totalDistance'] = distances[-1]
    with open(os.path.join(GENERATED_IN_DIR, 'client_stops.json'), 'w') as f:
        json.dump(client_stops, f, indent=4, ensure_ascii=False)
    print("Updated client_stops.json")



def generate_timings_tsv():
    if '-t' not in sys.argv:
        print('Skipping timings.tsv generation, as -t flag is not provided.')
        return
    print("Generating timings.tsv")
    with open(os.path.join(GENERATED_IN_DIR, 'route_children_ids.json'), 'r') as f:
        route_children_ids = json.load(f)
    time_rows = []
    for route, route_id in route_children_ids.items():
        times = {}
        times_maxed_duration = {'times': []}
        tmr = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        response = query_api(SCHEDULE_URL, {
            "routeid": route_id,
            "starttime": f"{tmr} 00:01",
            "endtime": f"{tmr} 23:59",
            "current_date": f"{tmr}"
        })
        if 'data' in response and len(response['data']) > 0:
            trips = response['data'][0]['tripdetails']
            for trip in trips:
                start = datetime.timedelta(hours=int(trip['starttime'].split(':')[0]), minutes=int(trip['starttime'].split(':')[1]))
                end = datetime.timedelta(hours=int(trip['endtime'].split(':')[0]), minutes=int(trip['endtime'].split(':')[1]))
                if end < start:
                    end += datetime.timedelta(days=1)

                duration = f"{(((end.days * 86400) + (end - start).seconds) // 3600):02}:{((((end.days * 86400) + (end - start).seconds) // 60) % 60):02}"
                duration_time = datetime.timedelta(hours=(((end.days * 86400) + (end - start).seconds) // 3600), minutes=((((end.days * 86400) + (end - start).seconds) // 60) % 60))
                times_maxed_duration['duration'] = max(duration_time, times_maxed_duration.get('duration', datetime.timedelta(seconds=0)))
                times_maxed_duration['times'].append(f"{(start.seconds // 3600):02}:{((start.seconds // 60) % 60):02}")
                # if duration not in times:
                #     times[duration] = []
                # times[duration].append(f"{(start.seconds // 3600):02}:{((start.seconds // 60) % 60):02}")
            times[f"{(times_maxed_duration['duration'].seconds // 3600):02}:{(((times_maxed_duration['duration'].seconds // 60) % 60)):02}"] = times_maxed_duration['times']
            for duration, starts in times.items():
                time_str = ""
                for start in starts:
                    time_str += f' {start}'
                time_rows.append({"duration": duration, "time": time_str.strip(), "route_no": route})
    with open(os.path.join(GENERATED_IN_DIR, 'timings.tsv'), 'w') as f:
        f.write("route_no\ttime\tduration\t\n") # Write headers
        for row in time_rows:
            f.write(f"{row['route_no']}\t{row['time']}\t{row['duration']}\t\n")
    print('Wrote timings from API to TSV file.')

def generate_times_json():
    if '-tdb' not in sys.argv:
        print("Skipping times.json generation, -tdb flag is not provided. Ensure live_data.db is present before running with flag.")
    with open(os.path.join(GENERATED_IN_DIR, 'client_stops.json'), 'r') as f:
        client_stops = json.load(f)
    with open(os.path.join(GENERATED_IN_DIR, 'route_children_ids.json'), 'r') as f:
        route_children_ids = json.load(f)
    with open(os.path.join(GENERATED_IN_DIR, 'timings.tsv'), 'r') as f:
        f.readline() # Skip headers
        timings = {row.split('\t')[0]: [r for r in row.split('\t')[1].split(' ')] for row in f.readlines(-1)}
    [timings.pop(r) if r not in route_children_ids else '' for r in timings.copy().keys()]
    [timings.pop(r) if r not in client_stops else '' for r in timings.copy().keys()]
    all_time_predictions = predict_times.predict_stop_times_segmental(
        data_input=[
            { # r = timings key, routename
                "route_id": route_children_ids[r],
                "trip_start": t,
                "stops": [{"stop_id": s['stop_id'], "stop_loc": s['loc']} for s in client_stops[r]['stops']]
            }
            for r, ts in timings.items() for t in ts
        ], db_path='db/live_data.db', output_path=None, target_date=datetime.datetime.now().strftime('%Y-%m-%d'))
    times = {}
    routename_by_id = {v: k for k, v in route_children_ids.items()}
    for entries in all_time_predictions:
        if routename_by_id[entries['route_id']] not in times:
            times[routename_by_id[entries['route_id']]] = []
        end_time = parse_local_time(to_hhmm(entries['stops'][-1]['stop_time']))
        start_time = parse_local_time(entries['trip_start'])
        if end_time < start_time:
            end_time += 86400
        duration = (end_time - start_time) / 60
        output = {
            'route_id': entries['route_id'],
            'duration': duration,
            'start': from_hhmm(entries['trip_start']),
            'stops': entries['stops']
        }
        times[routename_by_id[entries['route_id']]].append(output)
    with open(os.path.join(GENERATED_IN_DIR, 'times.json'), 'w') as f:
        json.dump(times, f, indent=4, ensure_ascii=False)
    print('Written out times to times.json')

# Remove routes that do not have timings information or are missing partial data
def clean_files():
    pass

def copy_files_to_in():
    if '-c' not in sys.argv:
        print("Not copying / overwriting files to in directory, as -c flag is not provided.")

    # client_stops.json -> client_stops.json && helpers/construct_stops/client_stops.json
    # route_children_ids.json -> routes_children_ids.json
    # route_parent_ids.json -> routes_parent_ids.json
    # routelines.json -> routelines.json
    # times.json -> times.json
    # timings.tsv -> helpers/construct_timings/timings.tsv



# Save child route ids
save_child_routes()

# Save parent route ids
save_route_parent_ids()

# generate updated client_stops
generate_client_stops()

# generate polylines
generate_google_polylines()

# Populate distances where distances are missing from API response
populate_distances()

# Generate timings.tsv file
generate_timings_tsv()

# Generate times.json with stop-by-stop timings (in future
generate_times_json()

# Clean entries without timing info
clean_files()

# Copy / Overwrite relevant files in /in folder
copy_files_to_in()
