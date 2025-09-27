import sqlite3
import os
from typing import Dict
from old_src.shared.config import DB_PATH

def get_connection():
    return sqlite3.connect(DB_PATH)

def initialize_database():
    # Ensure the directory for the database exists
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    with get_connection() as conn:
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS completed_stop_times (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stop_id TEXT,
                trip_id TEXT,
                route_id TEXT,
                date TEXT,
                actual_arrival TEXT,
                actual_departure TEXT,
                scheduled_arrival TEXT,
                scheduled_departure TEXT,
                UNIQUE(stop_id, trip_id, date)
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS vehicle_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vehicle_id TEXT,
                trip_id TEXT,
                route_id TEXT,
                lat REAL,
                lon REAL,
                bearing REAL,
                timestamp INTEGER,
                speed REAL,
                status TEXT,
                UNIQUE(vehicle_id, timestamp)
            )
        ''')
        # Also enforce uniqueness by (trip_id, timestamp) to avoid duplicates within a trip
        c.execute('''
            CREATE UNIQUE INDEX IF NOT EXISTS idx_vehicle_positions_trip_ts
            ON vehicle_positions(trip_id, timestamp)
        ''')
        conn.commit()

def insert_vehicle_data(data: Dict):
    with get_connection() as conn:
        c = conn.cursor()
        try:
            c.execute('''
                INSERT OR IGNORE INTO completed_stop_times (
                    stop_id, trip_id, route_id, date,
                    actual_arrival, actual_departure,
                    scheduled_arrival, scheduled_departure
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data["stop_id"],
                data["trip_id"],
                data["route_id"],
                data["date"],
                data["actual_arrival"],
                data["actual_departure"],
                data["scheduled_arrival"],
                data["scheduled_departure"]
            ))
            conn.commit()
        except Exception as e:
            print(f"Error inserting data for stop_id={data['stop_id']}, trip_id={data['trip_id']}: {e}")

def insert_vehicle_position(data: Dict):
    with get_connection() as conn:
        c = conn.cursor()
        try:
            c.execute('''
                INSERT OR IGNORE INTO vehicle_positions (
                    vehicle_id, trip_id, route_id, lat, lon, bearing, timestamp, speed, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data.get("vehicle_id"),
                data.get("trip_id"),
                data.get("route_id"),
                data.get("lat"),
                data.get("lon"),
                data.get("bearing"),
                data.get("timestamp"),
                data.get("speed"),
                data.get("status"),
            ))
            conn.commit()
        except Exception as e:
            print(f"Error inserting vehicle position for vehicle_id={data.get('vehicle_id')}, trip_id={data.get('trip_id')}: {e}")

