import traceback
from datetime import datetime
import asyncio
import threading
from contextlib import asynccontextmanager
from typing import Set, Dict

from aiohttp import TCPConnector

from src.shared import scheduled_timings, routes_children, routes_parent, start_times
from src.live_data_service.live_data_getter import fetch_route_data
from src.live_data_service.live_data_transformer import transform_response_to_feed_entities
from src.live_data_service.feed_entity_updater import update_feed_message
from src.shared.utils import generate_trip_id_timing_map


# Use a regular set with a lock for thread safety since we can't create weak references to integers
active_parents: Set[int] = set()
active_parents_lock = threading.Lock()

# Cache for trip maps to reduce memory allocations
trip_map_cache: Dict[int, dict] = {}
CACHE_CLEANUP_INTERVAL = 3600  # 1 hour
FETCH_TIMEOUT = 120  # seconds

@asynccontextmanager
async def managed_polling(parent_id: int):
    """Context manager to ensure proper cleanup of active_parents"""
    try:
        with active_parents_lock:
            active_parents.add(parent_id)
        yield
    finally:
        with active_parents_lock:
            active_parents.discard(parent_id)

async def cleanup_trip_map_cache():
    """Periodically cleanup the trip map cache"""
    while True:
        await asyncio.sleep(CACHE_CLEANUP_INTERVAL)
        trip_map_cache.clear()

async def live_data_receiver_loop():
    """
    Consumes scheduled_timings queue and starts polling tasks for each unique parent_id.
    Ensures only one polling task per parent_id at a time.
    """
    shared_connector = TCPConnector(limit=1, force_close=True)
    # Start cache cleanup task
    asyncio.create_task(cleanup_trip_map_cache())
    
    while True:
        try:
            if scheduled_timings.empty():
                await asyncio.sleep(1)
                continue

            scheduled_time, job = scheduled_timings.queue[0]  # Peek without removing
            now = datetime.now()

            if now.timestamp() >= scheduled_time.timestamp():
                _, job = scheduled_timings.get()  # Now remove
                parent_id = job["parent_id"]

                with active_parents_lock:
                    if parent_id in active_parents:
                        continue  # Already polling this parent

                asyncio.create_task(poll_route_parent_until_done(parent_id, shared_connector))
            else:
                await asyncio.sleep(1)
        except Exception as e:
            print(f"Error in live_data_receiver_loop: {e}")
        finally:
            await asyncio.sleep(0.001)

async def poll_route_parent_until_done(parent_id: int, connector):
    """
    Polls the BMTC API every 20s for a given route parent_id.
    Stops after 2 consecutive polls return no matching live trip data.
    """
    async with managed_polling(parent_id):
        # Get or create trip map from cache
        if parent_id not in trip_map_cache:
            child_routes = routes_children.as_dict()
            start_time_data = start_times.as_dict()
            trip_map_cache[parent_id] = generate_trip_id_timing_map(start_time_data, child_routes)
        
        trip_map = trip_map_cache[parent_id]
        print(f"[Polling] Started polling for parent_id={parent_id}")
        empty_tries = 0
        MAX_EMPTY_TRIES = 10

        try:
            while True:
                try:
                    # Use wait_for instead of timeout context manager
                    data = await asyncio.wait_for(fetch_route_data(parent_id, connector=connector), timeout=FETCH_TIMEOUT)

                    if not data:
                        print(f"[Polling] [{datetime.now().strftime('%d-%m %H:%M:%S')}] No data for parent_id={parent_id}")
                        empty_tries += 1
                    else:
                        matching_jobs = []
                        for route_key, child_id in routes_children.items():
                            if routes_parent.get(route_key) != parent_id:
                                continue

                            for trip_entry in trip_map.get(route_key, []):
                                matching_jobs.append({
                                    "trip_id": trip_entry["trip"],
                                    "trip_time": datetime.strptime(trip_entry['start'], "%H:%M:%S"),
                                    "route_id": str(child_id),
                                    "parent_id": parent_id
                                })

                        found_match = False
                        all_entities = []

                        for job in matching_jobs:
                            entities = transform_response_to_feed_entities(data, job)
                            if entities:
                                found_match = True
                                all_entities.extend(entities)

                        if found_match:
                            update_feed_message(all_entities)
                            empty_tries = 0
                        else:
                            empty_tries += 1

                    if empty_tries >= MAX_EMPTY_TRIES:
                        print(f"[Polling] [{datetime.now().strftime('%d-%m %H:%M:%S')}] No matches after {MAX_EMPTY_TRIES} tries. Stopping {parent_id}.")
                        break

                    await asyncio.sleep(30) # Sleep for 30 seconds
                except asyncio.TimeoutError:
                    # traceback.print_exc()
                    print(f"[Polling] Timeout while fetching data for parent_id={parent_id}")
                    empty_tries += 1
                except Exception as e:
                    traceback.print_exc()
                    print(f"[Polling] Error while polling parent_id={parent_id}: {e}")
                    empty_tries += 1
        finally:
            # Cleanup cache when done with this parent_id
            trip_map_cache.pop(parent_id, None)
