import os
import threading
import asyncio
import gc
import traceback

import signal
import atexit

from src.local_file_service.local_file_service import process_once, LocalFileService
from src.live_data_service.live_data_scheduler import schedule_thread
from src.live_data_service.live_data_receiver import live_data_receiver_loop
from src.web_service import run_web_service
from src.shared.db import initialize_database
from src.shared.memory_config import get_memory_config, get_hardware_profile
from src.shared.monitor import start_monitoring, stop_monitoring, get_performance_summary

# Global state for cleanup
running_threads = []
cleanup_lock = threading.Lock()
is_shutting_down = threading.Event()

class ManagedEventLoop:
    """Manages AsyncIO event loop with proper task cleanup to prevent memory accumulation"""
    
    def __init__(self):
        self._loop = None
        self._restart_count = 0
        self._max_restarts = 100  # Restart loop every 100 cycles
        self._shutdown_event = threading.Event()
        
    def signal_shutdown(self):
        """Signal the event loop to shut down gracefully"""
        self._shutdown_event.set()
        
    async def run_receiver_loop(self):
        """Run receiver loop with proper task cleanup"""
        if self._loop is None:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
        
        try:
            # Import here to avoid circular imports
            from src.live_data_service.live_data_receiver import live_data_receiver_loop, cancel_all_tasks
            
            await live_data_receiver_loop()
        except Exception as e:
            print(f"[EventLoop] Error in receiver loop: {e}")
            traceback.print_exc()
            raise
        finally:
            print("[EventLoop] Cleaning up event loop...")
            
            # Cancel all remaining tasks before closing loop
            try:
                from src.live_data_service.live_data_receiver import cancel_all_tasks
                await cancel_all_tasks()
            except Exception as e:
                print(f"[EventLoop] Error cancelling tasks: {e}")
            
            self._restart_count += 1
            
            # Periodically restart event loop to prevent memory accumulation
            if self._restart_count >= self._max_restarts:
                print(f"[EventLoop] Restarting event loop after {self._max_restarts} cycles")
                
                # Proper cleanup before closing
                pending_tasks = [task for task in asyncio.all_tasks(self._loop) if not task.done()]
                if pending_tasks:
                    print(f"[EventLoop] Cancelling {len(pending_tasks)} pending tasks")
                    for task in pending_tasks:
                        task.cancel()
                    
                    # Wait for tasks to cancel
                    try:
                        await asyncio.gather(*pending_tasks, return_exceptions=True)
                    except Exception as e:
                        print(f"[EventLoop] Error during task cleanup: {e}")
                
                self._loop.close()
                self._loop = None
                self._restart_count = 0
                gc.collect()

# Create global managed loop instance
managed_loop = ManagedEventLoop()

def cleanup_resources():
    """Clean up resources on shutdown"""
    print("[main] Cleaning up resources...")
    is_shutting_down.set()
    
    # Stop performance monitoring
    stop_monitoring()
    
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

def print_system_info():
    """Print system information and configuration"""
    try:
        import psutil
        
        # Hardware detection
        memory_gb = psutil.virtual_memory().total / (1024**3)
        cpu_count = psutil.cpu_count()
        
        print(f"[System] Hardware: {cpu_count} CPU cores, {memory_gb:.1f}GB RAM")
        
        # Memory configuration
        profile = get_hardware_profile()
        config = get_memory_config(profile)
        
        print(f"[System] Detected profile: {profile}")
        print(f"[System] Memory limit: {config['max_memory_mb']}MB")
        print(f"[System] Max concurrent tasks: {config['max_concurrent_tasks']}")
        print(f"[System] Chunk size: {config['chunk_size']}")
        
    except Exception as e:
        print(f"[System] Error detecting hardware: {e}")

def main():
    # Register cleanup handlers
    atexit.register(cleanup_resources)
    signal.signal(signal.SIGINT, lambda s, f: cleanup_resources())
    signal.signal(signal.SIGTERM, lambda s, f: cleanup_resources())
    
    print("[main] Starting GTFS Live Data System")
    
    # Print system information
    print_system_info()
    
    # Start performance monitoring
    start_monitoring(interval=120)  # Monitor every 2 minutes
    
    initialize_database()

    # Step 1: Run local_file_service once to load initial state
    print("[main] Running initial local_file_service pass...")
    process_once()

    # Step 2: Start local_file_service loop in background thread
    print("[main] Starting local_file_service loop...")
    config = get_memory_config()
    local_service = LocalFileService(max_memory_mb=config['max_memory_mb'])
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

    # Step 4: Start live_data_receiver_loop in asyncio background thread with managed event loop
    print("[main] Starting live_data_receiver_loop...")
    receiver_thread = threading.Thread(
        target=lambda: asyncio.run(managed_loop.run_receiver_loop()),
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
