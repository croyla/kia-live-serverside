from datetime import datetime
from src.live_data_service.live_data_transformer import transform_response_to_feed_entities
from src.shared.db import initialize_database
import pytz

initialize_database()

api_data = [
    {
        "routeid": "1234",
        "stationid": "stop1",
        "vehicleDetails": [{
            "vehicleid": "veh001",
            "vehiclenumber": "MH12AB1234",
            "sch_tripstarttime": "17:00",
            "sch_arrivaltime": "17:10",
            "sch_departuretime": "17:15",
            "actual_arrivaltime": "17:11",
            "actual_departuretime": "17:16",
            "centerlat": 18.5204,
            "centerlong": 73.8567,
            "heading": 90,
            "lastrefreshon": "07-06-2025 17:10:00"
        }]
    }
]

job = {
    "route_id": "1234",
    "trip_id": "trip_001",
    "trip_time": datetime.now(pytz.timezone("Asia/Kolkata")).replace(hour=17, minute=0, second=0, microsecond=0)
}

entities = transform_response_to_feed_entities(api_data, job)
print(f"Generated {len(entities)} GTFS-RT entities.")

# Optional: print out each entity
for e in entities:
    print(e)
