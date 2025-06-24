import os
import threading
import asyncio

import signal
import atexit

from src.local_file_service.local_file_service import process_once, LocalFileService
from src.live_data_service.live_data_scheduler import schedule_thread
from src.live_data_service.live_data_receiver import live_data_receiver_loop
from src.web_service import run_web_service
from src.shared.db import initialize_database

# Global state for cleanup
running_threads = []
cleanup_lock = threading.Lock()
is_shutting_down = threading.Event()

def cleanup_resources():
    """Clean up resources on shutdown"""
    print("[main] Cleaning up resources...")
    is_shutting_down.set()
    
    # Wait for threads to finish
    with cleanup_lock:
        for thread in running_threads:
            if thread.is_alive():
                thread.join(timeout=5.0)
                
    print("[main] Cleanup complete")

def add_thread(thread: threading.Thread):
    """Add thread to cleanup list"""
    with cleanup_lock:
        running_threads.append(thread)




def main():
    # Register cleanup handlers
    atexit.register(cleanup_resources)
    signal.signal(signal.SIGINT, lambda s, f: cleanup_resources())
    signal.signal(signal.SIGTERM, lambda s, f: cleanup_resources())
    
    print("[main] Starting GTFS Live Data System")
    initialize_database()

    # Step 1: Run local_file_service once to load initial state
    print("[main] Running initial local_file_service pass...")
    process_once()

    # Step 2: Start local_file_service loop in background thread
    print("[main] Starting local_file_service loop...")
    local_service = LocalFileService()
    local_service_thread = threading.Thread(
        target=local_service.run_daily_loop,
        daemon=True,
        name="local_file_service"
    )
    add_thread(local_service_thread)
    local_service_thread.start()

    # Step 3: Start live_data_scheduler in background thread
    print("[main] Starting live_data_scheduler...")
    scheduler_thread = threading.Thread(
        target=schedule_thread,
        daemon=True,
        name="live_data_scheduler"
    )
    add_thread(scheduler_thread)
    scheduler_thread.start()

    # Step 4: Start live_data_receiver_loop in asyncio background thread
    print("[main] Starting live_data_receiver_loop...")
    receiver_thread = threading.Thread(
        target=lambda: asyncio.run(live_data_receiver_loop()),
        daemon=True,
        name="live_data_receiver"
    )
    add_thread(receiver_thread)
    receiver_thread.start()

    # Step 5: Start web service (this will block)
    try:
        run_web_service()
    except KeyboardInterrupt:
        print("[main] Received shutdown signal")
    finally:
        cleanup_resources()

if __name__ == "__main__":
    print(os.getcwd())
    main()
