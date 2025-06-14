import json
import os
from math import radians, sin, cos, sqrt, atan2
from src.shared.config import CLIENT_STOPS_PATH, API_RESPONSES_DIR


inverted_routes = []

def load_client_stops(client_stops_path):
    """Load the client stops data."""
    with open(client_stops_path, 'r') as file:
        return json.load(file)


def load_api_response(api_path):
    """Load the API response from the given path."""
    with open(api_path, 'r') as file:
        return json.load(file)


def haversine(lat1, lon1, lat2, lon2):
    """Calculate the Haversine distance between two latitude/longitude points."""
    R = 6371.0  # Radius of the Earth in kilometers

    # Convert latitude and longitude from degrees to radians
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    # Difference between coordinates
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    # Haversine formula
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return R * c  # Resulting distance in kilometers


def update_client_stops(client_stops, api_responses_dir):
    """Recreate client stops based on API data, matched by closest location."""
    updated_client_stops = {}

    for key, stop_data in client_stops.items():
        # Determine the correct API response file path
        api_path = os.path.join(api_responses_dir, f"{key}.json")

        # Load API response
        try:
            api_response = load_api_response(api_path)
        except FileNotFoundError:
            print(f"API response for {key} not found, skipping...")
            continue
        is_inverted = key.split(' ')[0] in inverted_routes
        data_key = 'up' if 'UP' in key else 'down' if 'DOWN' in key else ''
        data_key = data_key if not is_inverted \
            else 'up' if data_key == 'down' \
            else 'down' if data_key == 'up' \
            else data_key
        # Decide which section (up or down) to use based on the data
        data_section = api_response.get(data_key, {}).get('data')

        if data_section is None:
            print(f"No data found for {key}, skipping...")
            continue

        # Create a new entry for the client stop
        updated_stops = []
        existing_stops_set = set()  # Track stops already in client_stops (by name)

        # Process the stops from client_stops first
        for stop in stop_data['stops']:
            closest_stop = None
            min_distance = float('inf')

            # Check for the closest stop in the API data based on location
            for api_stop in data_section:
                api_lat, api_lon = api_stop['centerlat'], api_stop['centerlong']
                stop_lat, stop_lon = stop['loc']

                # Calculate the distance between the stop in client_stops and the API stop
                distance = haversine(stop_lat, stop_lon, api_lat, api_lon)

                # Track the closest stop
                if distance < min_distance:
                    min_distance = distance
                    closest_stop = api_stop

            if closest_stop:
                if 'name_kn' in stop and stop['name'] == stop['name_kn']:
                    stop['name_kn'] = None
                    stop.pop('name_kn')
                stop['stop_id'] = closest_stop.get('stationid')
                stop['name_kn'] = stop.get('name_kn', closest_stop.get('stationname_kn', closest_stop.get('stationname', '')))  # Populate name_kn if missing
                stop['name'] = stop.get('name')
                stop['distance'] = closest_stop.get('distance_on_station', stop['distance'])
                existing_stops_set.add(closest_stop['stationname'])  # Add the stop name to the set

            updated_stops.append(stop)

        # Add any new stops from the API that are not already in client_stops
        for api_stop in data_section:
            if api_stop['stationname'] not in existing_stops_set:
                # Create a new stop from API data and insert it at the correct position based on sequence
                new_stop = {
                    'name': api_stop['stationname'],
                    'name_kn': api_stop.get('stationname', ''),
                    'loc': [api_stop['centerlat'], api_stop['centerlong']],  # Assuming API data is [lon, lat]
                    'stop_id': api_stop.get('stationid'),
                    'distance': api_stop['distance_on_station']  # You can adjust distance logic if needed
                }
                # Insert the new stop at the correct position based on sequence in API data
                updated_stops.append(new_stop)
        updated_stops.sort(key=lambda stop: float(stop.get('distance', float('inf'))))  # Handle "N/A" or non-numeric distances
        updated_client_stops[key] = {
            'stops': updated_stops,
            'totalDistance': updated_stops[-1].get('distance')
        }

    return updated_client_stops


def save_updated_client_stops(updated_client_stops, output_path):
    """Save the updated client stops to a new JSON file."""
    with open(output_path, 'w', encoding='utf-8') as file:
        json.dump(updated_client_stops, file, indent=4, ensure_ascii=False)
    print(f"JSON saved to {output_path}")


def main():
    # Path to the existing client_stops.json
    client_stops_path = CLIENT_STOPS_PATH

    # Directory where the API responses are stored
    api_responses_dir = API_RESPONSES_DIR

    # Path where the updated client_stops.json should be saved
    output_path = CLIENT_STOPS_PATH  # or another constant if needed

    # Load the client stops data
    client_stops = load_client_stops(client_stops_path)

    # Update client stops with stop_id, name_kn, and other data
    updated_client_stops = update_client_stops(client_stops, api_responses_dir)

    if updated_client_stops:
        # Backup the original file before saving
        import shutil
        backup_path = output_path + '.bak'
        shutil.copy(output_path, backup_path)
        print(f"Backup saved to {backup_path}")

        # Save the updated client stops
        save_updated_client_stops(updated_client_stops, output_path)
        print(f"Updated {len(updated_client_stops)} routes.")
    else:
        print("⚠️ No updated stops generated. Skipping save to avoid overwriting existing file.")

if __name__ == '__main__':
    main()
