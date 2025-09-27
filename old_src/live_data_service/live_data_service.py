import threading
import asyncio
import time
from live_data_scheduler import schedule_thread
from live_data_receiver import live_data_receiver_loop


def start_scheduler_thread():
    """
    Launch the scheduler thread to populate the scheduled_timings queue daily.
    """
    thread = threading.Thread(
        target=schedule_thread,
        name="live_data_scheduler",
        daemon=True
    )
    thread.start()
    print("[LiveDataService] Scheduler thread started.")


def start_receiver_thread():
    """
    Launch the receiver in a dedicated asyncio event loop.
    """
    def runner():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(live_data_receiver_loop())
        except Exception as e:
            print(f"[LiveDataService] live_data_receiver_loop crashed: {e}")
        finally:
            loop.close()

    thread = threading.Thread(
        target=runner,
        name="live_data_receiver",
        daemon=True
    )
    thread.start()
    print("[LiveDataService] Receiver thread started.")


def start_live_data_service():
    """
    Main entry point to start the full live data service.
    """
    print("[LiveDataService] Starting...")
    start_scheduler_thread()
    start_receiver_thread()
    print("[LiveDataService] All components launched.")
