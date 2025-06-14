from datetime import datetime
import asyncio

from src.shared import scheduled_timings, routes_children, routes_parent, start_times
from src.live_data_service.live_data_getter import fetch_route_data
from src.live_data_service.live_data_transformer import transform_response_to_feed_entities
from src.live_data_service.feed_entity_updater import update_feed_message
from src.shared.utils import generate_trip_id_timing_map


# Set of active parent_ids currently being polled
active_parents = set()


async def live_data_receiver_loop():
    """
    Consumes scheduled_timings queue and starts polling tasks for each unique parent_id.
    Ensures only one polling task per parent_id at a time.
    """
    while True:
        if scheduled_timings.empty():
            await asyncio.sleep(1)
            continue

        scheduled_time, job = scheduled_timings.queue[0]  # Peek without removing

        now = datetime.now()
        if now >= scheduled_time:
            _, job = scheduled_timings.get()  # Now remove
            parent_id = job["parent_id"]

            if parent_id in active_parents:
                continue  # Already polling this parent

            active_parents.add(parent_id)
            asyncio.create_task(poll_route_parent_until_done(parent_id))
        else:
            await asyncio.sleep(1)


async def poll_route_parent_until_done(parent_id: int):
    """
    Polls the BMTC API every 20s for a given route parent_id.
    Stops after 2 consecutive polls return no matching live trip data.
    """
    child_routes = routes_children.as_dict()
    start_time_data = start_times.as_dict()
    trip_map = generate_trip_id_timing_map(start_time_data, child_routes)

    print(f"[Polling] Started polling for parent_id={parent_id}")
    empty_tries = 0
    MAX_EMPTY_TRIES = 2

    while True:
        data = await fetch_route_data(parent_id)

        if not data:
            print(f"[Polling] [{datetime.now().strftime('%d-%m %H:%M:%S')}] No data for parent_id={parent_id}")
            empty_tries += 1
        else:
            matching_jobs = []
            for route_key, child_id in child_routes.items():
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
            active_parents.remove(parent_id)
            break

        await asyncio.sleep(20)
