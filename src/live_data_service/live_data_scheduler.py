import os
import time
from datetime import datetime, timedelta
from src.shared.utils import generate_trip_id_timing_map
from src.shared import scheduled_timings, start_times, routes_children, routes_parent
import traceback

QUERY_INTERVAL = int(os.getenv("KIA_QUERY_INTERVAL", 5))  # minutes
QUERY_AMOUNT = int(os.getenv("KIA_QUERY_AMOUNT", 2))      # before/after count
TOTAL_QUERIES = 2 * QUERY_AMOUNT + 1                      # total per trip

def schedule_thread():
    now = datetime.now()
    print(f"[{now}] Running live_data_scheduler...")
    try:
        populate_schedule(all_now=True)
        populate_schedule()
    except Exception as e:
        print(f"Error in live_data_scheduler: {e}")
        traceback.print_exc()
    while True:
        now = datetime.now()
        if now.hour == 0 and now.minute >= 10 and now.minute < 15:  # run at ~00:10 AM
            print(f"[{now}] Running live_data_scheduler...")
            try:
                populate_schedule()
            except Exception as e:
                print(f"Error in live_data_scheduler: {e}")
            time.sleep(60 * 60 * 1)  # prevent re-running for an hour
        else:
            time.sleep(30)

def populate_schedule(all_now=False):
    trip_map = generate_trip_id_timing_map(start_times, routes_children)
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow = today + timedelta(days=1)
    queried_times = set()

    for route_key, trips in trip_map.items():
        child_id = routes_children.get(route_key)
        parent_id = routes_parent.get(route_key)

        if not (child_id and parent_id):
            continue

        for trip_entry in trips:
            hh, mm, ss = map(int, trip_entry["start"].split(":"))
            trip_time = today.replace(hour=hh, minute=mm)
            if trip_time <= datetime.now():
                trip_time += timedelta(days=1)
            if all_now:
                query_time = datetime.now()
                while query_time in queried_times:
                    query_time -= timedelta(seconds=1)
                queried_times.add(query_time)
                scheduled_timings.put(
                    (
                        query_time,
                        {
                            "trip_id": trip_entry["trip"],
                            "trip_time": trip_time,
                            "route_id": str(child_id),
                            "parent_id": int(parent_id)
                        }
                    )
                )
                continue

            for offset in range(-QUERY_AMOUNT, QUERY_AMOUNT + 1):
                query_time = trip_time + timedelta(minutes=offset * QUERY_INTERVAL)
                if today <= query_time < tomorrow + timedelta(days=1):
                    while query_time in queried_times:
                        query_time += timedelta(seconds=1)
                    queried_times.add(query_time)
                    scheduled_timings.put(
                        (
                            query_time,
                            {
                                "trip_id": trip_entry["trip"],
                                "trip_time": trip_time,
                                "route_id": str(child_id),
                                "parent_id": int(parent_id)
                            }
                        )
                    )
