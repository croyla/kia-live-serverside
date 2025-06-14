import os
import sys
import threading
import time
from datetime import datetime

from src.local_file_service.gtfs_builder import build_gtfs_dataset
from src.shared import new_client_stops, timings_tsv
from src.shared.utils import load_gtfs_zip, load_input_data, data_has_changed, zip_gtfs_data
import src.shared as rt_state
from src.shared.config import TSV_PATH, JSON_PATH, IN_DIR, OUT_DIR, OUT_ZIP


def process_once():
    # Load current input files
    print("Updating input data...")

    timings_tsv.process_tsv_to_json(TSV_PATH, JSON_PATH)

    new_client_stops.main()
    print("Loading input data...")
    input_data = load_input_data(IN_DIR)
    # === Update shared variables ===
    rt_state.routes_children.clear()
    rt_state.routes_parent.clear()
    rt_state.start_times.clear()
    rt_state.routes_children.update(input_data["routes_children"])
    rt_state.routes_parent.update(input_data["routes_parent"])
    rt_state.start_times.update(input_data["start_times"])

    # Load existing GTFS zip
    print("Loading GTFS data...")
    if os.path.exists(OUT_ZIP):
        existing_gtfs = load_gtfs_zip(OUT_ZIP)
    else:
        existing_gtfs = {}

    # Build new GTFS from input
    print("Building new GTFS data...")
    new_gtfs = build_gtfs_dataset(input_data)
    # Compare GTFS content (excluding feed_info.txt version, calendar end_date, etc.)
    print("Comparing with existing GTFS...")
    if data_has_changed(new_gtfs, existing_gtfs):
        print("Changes detected. Saving new GTFS.zip...")
        feed_info = new_gtfs['feed_info.txt']
        zip_gtfs_data(new_gtfs, OUT_ZIP)
        with open(os.path.join(OUT_DIR, "feed_info.txt"), "w", encoding='utf-8') as f:
            f.write(feed_info[0]['feed_version'])
    else:
        print("No changes detected. Skipping update.")


class LocalFileService:
    def __init__(self):
        self.interval = 24 * 60 * 60  # Run daily

    def start(self):
        thread = threading.Thread(target=self.run_daily_loop, daemon=True)
        thread.start()

    def run_daily_loop(self):
        while True:
            try:
                print(f"[{datetime.now()}] Running local_file_service...")
                process_once()
            except Exception as e:
                print(f"Error in local_file_service: {e}")
            time.sleep(self.interval)

