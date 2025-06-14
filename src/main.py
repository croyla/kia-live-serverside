import threading
import asyncio
from src.local_file_service.local_file_service import process_once, LocalFileService
from src.live_data_service.live_data_scheduler import schedule_thread
from src.live_data_service.live_data_receiver import live_data_receiver_loop
from src.web_service import run_web_service
from src.shared.db import initialize_database

def main():
    print("[main] Starting GTFS Live Data System")
    initialize_database()

    # Step 1: Run local_file_service once to load initial state
    print("[main] Running initial local_file_service pass...")
    process_once()

    # Step 2: Start local_file_service loop in background thread
    print("[main] Starting local_file_service loop...")
    LocalFileService().start()

    # Step 3: Start live_data_scheduler in background thread
    print("[main] Starting live_data_scheduler...")
    scheduler_thread = threading.Thread(target=schedule_thread, daemon=True)
    scheduler_thread.start()

    # Step 4: Start live_data_receiver_loop in asyncio background thread
    print("[main] Starting live_data_receiver_loop...")
    receiver_thread = threading.Thread(
        target=lambda: asyncio.run(live_data_receiver_loop()),
        daemon=True
    )
    receiver_thread.start()

    run_web_service()

if __name__ == "__main__":
    main()
    # schedule_thread()
