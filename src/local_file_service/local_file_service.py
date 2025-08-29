import os
import threading
import time
import psutil
from datetime import datetime

from src.local_file_service.gtfs_builder import build_gtfs_dataset
from src.shared import new_client_stops, timings_tsv
from src.shared.utils import load_gtfs_zip_optimized, load_input_data_optimized, data_has_changed, zip_gtfs_data_optimized
import src.shared as rt_state
from src.shared.config import TSV_PATH, JSON_PATH, IN_DIR, OUT_DIR, OUT_ZIP


def get_memory_usage_mb() -> float:
    """Get current memory usage in MB"""
    try:
        process = psutil.Process()
        return process.memory_info().rss / (1024 * 1024)
    except Exception:
        return 0.0


def process_once_optimized(max_memory_mb: int = 200):
    """Process with memory constraints and monitoring"""
    initial_memory = get_memory_usage_mb()
    print(f"[Process] Initial memory usage: {initial_memory:.1f}MB")
    
    try:
        # Load current input files with memory limits
        print("Updating input data...")
        timings_tsv.process_tsv_to_json(TSV_PATH, JSON_PATH)

        new_client_stops.main()
        print("Loading input data with memory constraints...")
        
        # Use optimized loading with memory limits
        input_data = load_input_data_optimized(IN_DIR, max_memory_mb=max_memory_mb)
        
        current_memory = get_memory_usage_mb()
        print(f"[Process] After loading input data: {current_memory:.1f}MB (+{current_memory - initial_memory:.1f}MB)")
        
        # === Update shared variables ===
        rt_state.routes_children.clear()
        rt_state.routes_parent.clear()
        rt_state.start_times.clear()
        rt_state.routes_children.update(input_data["routes_children"])
        rt_state.routes_parent.update(input_data["routes_parent"])
        rt_state.start_times.update(input_data["start_times"])
        rt_state.times.update(input_data['times'])
        rt_state.route_stops.update(input_data['client_stops'])
        
        # Load existing GTFS zip with memory constraints
        print("Loading GTFS data with memory limits...")
        if os.path.exists(OUT_ZIP):
            existing_gtfs = load_gtfs_zip_optimized(OUT_ZIP, max_memory_mb=50)
        else:
            existing_gtfs = {}

        current_memory = get_memory_usage_mb()
        print(f"[Process] After loading GTFS: {current_memory:.1f}MB (+{current_memory - initial_memory:.1f}MB)")

        # Build new GTFS from input
        print("Building new GTFS data...")
        new_gtfs = build_gtfs_dataset(input_data)
        
        current_memory = get_memory_usage_mb()
        print(f"[Process] After building GTFS: {current_memory:.1f}MB (+{current_memory - initial_memory:.1f}MB)")
        
        # Compare GTFS content (excluding feed_info.txt version, calendar end_date, etc.)
        print("Comparing with existing GTFS...")
        if data_has_changed(new_gtfs, existing_gtfs):
            print("Changes detected. Saving new GTFS.zip...")
            feed_info = new_gtfs['feed_info.txt']
            zip_gtfs_data_optimized(new_gtfs, OUT_ZIP)
            with open(os.path.join(OUT_DIR, "feed_info.txt"), "w", encoding='utf-8') as f:
                f.write(feed_info[0]['feed_version'])
        else:
            print("No changes detected. Skipping update.")
            
        # Clear large objects to free memory
        del input_data, new_gtfs, existing_gtfs
        
        final_memory = get_memory_usage_mb()
        print(f"[Process] Final memory usage: {final_memory:.1f}MB (peak: +{final_memory - initial_memory:.1f}MB)")
        
    except Exception as e:
        print(f"Error in process_once_optimized: {e}")
        raise


def process_once():
    """Legacy function - redirects to optimized version"""
    process_once_optimized()


class LocalFileService:
    def __init__(self, interval=24 * 60 * 60, max_memory_mb=200):
        self.interval = interval
        self.max_memory_mb = max_memory_mb

    def start(self):
        thread = threading.Thread(target=self.run_daily_loop, daemon=True)
        thread.start()

    def run_daily_loop(self):
        while True:
            try:
                print(f"[{datetime.now()}] Running local_file_service with memory limit: {self.max_memory_mb}MB...")
                process_once_optimized(self.max_memory_mb)
            except Exception as e:
                print(f"Error in local_file_service: {e}")
            time.sleep(self.interval)

