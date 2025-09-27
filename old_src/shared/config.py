import os

# Root of the project
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))

# Common paths used throughout the project
DB_PATH = os.path.join(BASE_DIR, "db", "live_data.db")
CLIENT_STOPS_PATH = os.path.join(BASE_DIR, "in", "client_stops.json")
API_RESPONSES_DIR = os.path.join(BASE_DIR, "in", "helpers", "construct_stops", "api_responses")
TSV_PATH = os.path.join(BASE_DIR, "in", "helpers", "construct_timings", "timings.tsv")
JSON_PATH = os.path.join(BASE_DIR, "in", "start_times.json")
IN_DIR = os.path.join(BASE_DIR, "in")
OUT_DIR = os.path.join(BASE_DIR, "out")
OUT_ZIP = os.path.join(OUT_DIR, "gtfs.zip")
