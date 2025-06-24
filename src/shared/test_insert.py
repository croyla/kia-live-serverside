from src.shared.db import initialize_database, insert_vehicle_data

# Create the table if not exists
initialize_database()

# Insert dummy data
insert_vehicle_data({
    "vehicle_id": "hello_world",
    "trip_id": "trip_123",
    "route_id": "route_456",
    "lat": 12.34,
    "lon": 56.78,
    "timestamp": 1234567890,
    "speed": 30.0,
    "stop_id": "stop_1",
    "status": "IN_TRANSIT"
})

print("Inserted hello world row successfully.")
