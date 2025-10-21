"""
Database service for the new architecture.
Handles all database logging operations with connection pooling and memory management.
"""

import sqlite3
import asyncio
import aiosqlite
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime, date, timedelta
import logging

from src.core.config import ApplicationConfig
from src.data.models.vehicle import Vehicle
from src.data.models.trip import Trip, TripStop

logger = logging.getLogger(__name__)


class DatabaseService:
    """Handle all database logging operations with SQLite"""
    
    def __init__(self, config: ApplicationConfig):
        self.config = config
        self.db_path = config.db_path
        self.connection_pool_size = 5
        self._pool_semaphore = asyncio.Semaphore(self.connection_pool_size)
        self._initialized = False
        
    async def initialize(self):
        """Initialize database schema"""
        if self._initialized:
            return
            
        # Ensure the directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        async with aiosqlite.connect(str(self.db_path)) as conn:
            await conn.execute('''
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
            
            await conn.execute('''
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
                    UNIQUE(vehicle_id, trip_id, route_id, lat, lon, timestamp)
                )
            ''')

            # Create index for efficient querying by trip_id and timestamp
            await conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_vehicle_positions_trip_ts
                ON vehicle_positions(trip_id, timestamp)
            ''')

            # Create index for efficient querying by vehicle_id, route_id and timestamp
            await conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_vehicle_positions_vehicle_route_ts
                ON vehicle_positions(vehicle_id, route_id, timestamp)
            ''')
            
            await conn.commit()
            
        self._initialized = True
        logger.info(f"Database initialized at {self.db_path}")

    async def log_completed_trip_stop(self, stop_data: Dict[str, Any]):
        """Log actual arrival/departure times for completed stops"""
        # print("LOGGING COMPLETED TRIPS")
        async with self._pool_semaphore:
            try:
                async with aiosqlite.connect(str(self.db_path)) as conn:
                    await conn.execute('''
                        INSERT OR IGNORE INTO completed_stop_times (
                            stop_id, trip_id, route_id, date,
                            actual_arrival, actual_departure,
                            scheduled_arrival, scheduled_departure
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        stop_data["stop_id"],
                        stop_data["trip_id"],
                        stop_data["route_id"],
                        stop_data["date"],
                        stop_data["actual_arrival"],
                        stop_data["actual_departure"],
                        stop_data["scheduled_arrival"],
                        stop_data["scheduled_departure"]
                    ))
                    await conn.commit()
                    
            except Exception as e:
                logger.error(f"Error inserting completed stop data for stop_id={stop_data.get('stop_id')}, trip_id={stop_data.get('trip_id')}: {e}")

    async def log_vehicle_position(self, position_data: Dict[str, Any]):
        """Log vehicle position for ongoing trips"""
        print("LOGGING INCOMPLETE TRIPS")
        async with self._pool_semaphore:
            try:
                async with aiosqlite.connect(str(self.db_path)) as conn:
                    await conn.execute('''
                        INSERT OR IGNORE INTO vehicle_positions (
                            vehicle_id, trip_id, route_id, lat, lon, bearing, timestamp, speed, status
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        position_data.get("vehicle_id"),
                        position_data.get("trip_id"),
                        position_data.get("route_id"),
                        position_data.get("lat"),
                        position_data.get("lon"),
                        position_data.get("bearing"),
                        position_data.get("timestamp"),
                        position_data.get("speed"),
                        position_data.get("status"),
                    ))
                    await conn.commit()
                    
            except Exception as e:
                logger.error(f"Error inserting vehicle position for vehicle_id={position_data.get('vehicle_id')}, trip_id={position_data.get('trip_id')}: {e}")

    async def batch_insert_positions(self, positions: List[Dict[str, Any]]):
        """Batch insert vehicle positions for efficiency"""
        if not positions:
            return
            
        async with self._pool_semaphore:
            try:
                async with aiosqlite.connect(str(self.db_path)) as conn:
                    await conn.executemany('''
                        INSERT OR IGNORE INTO vehicle_positions (
                            vehicle_id, trip_id, route_id, lat, lon, bearing, timestamp, speed, status
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', [
                        (
                            pos.get("vehicle_id"),
                            pos.get("trip_id"),
                            pos.get("route_id"),
                            pos.get("lat"),
                            pos.get("lon"),
                            pos.get("bearing"),
                            pos.get("timestamp"),
                            pos.get("speed"),
                            pos.get("status"),
                        ) for pos in positions
                    ])
                    await conn.commit()
                    logger.debug(f"Batch inserted {len(positions)} vehicle positions")
                    
            except Exception as e:
                logger.error(f"Error batch inserting vehicle positions: {e}")

    async def get_recent_vehicle_positions(self, vehicle_id: str, route_id: str, limit: int = 500, max_age_hours: int = 6) -> List[Dict[str, Any]]:
        """Retrieve recent vehicle positions from SQLite

        Args:
            vehicle_id: The vehicle ID to search for
            route_id: The route ID to filter by
            limit: Maximum number of positions to return
            max_age_hours: Only return positions within this many hours

        Returns:
            List of position dictionaries, ordered by timestamp descending (most recent first)
        """
        cutoff_timestamp = int((datetime.now() - timedelta(hours=max_age_hours)).timestamp())

        async with self._pool_semaphore:
            try:
                async with aiosqlite.connect(str(self.db_path)) as conn:
                    conn.row_factory = aiosqlite.Row
                    cursor = await conn.execute('''
                        SELECT vehicle_id, trip_id, route_id, lat, lon, bearing, timestamp, speed, status
                        FROM vehicle_positions
                        WHERE vehicle_id = ? AND route_id = ? AND timestamp >= ?
                        ORDER BY timestamp DESC
                        LIMIT ?
                    ''', (vehicle_id, route_id, cutoff_timestamp, limit))

                    rows = await cursor.fetchall()

                    # Convert rows to dictionaries
                    positions = []
                    for row in rows:
                        positions.append({
                            "vehicle_id": row["vehicle_id"],
                            "trip_id": row["trip_id"],
                            "route_id": row["route_id"],
                            "lat": row["lat"],
                            "lon": row["lon"],
                            "bearing": row["bearing"],
                            "timestamp": row["timestamp"],
                            "speed": row["speed"],
                            "status": row["status"]
                        })

                    logger.debug(f"Retrieved {len(positions)} positions from SQLite for vehicle {vehicle_id} on route {route_id}")
                    return positions

            except Exception as e:
                logger.error(f"Error retrieving vehicle positions for vehicle_id={vehicle_id}, route_id={route_id}: {e}")
                return []

    async def get_positions_by_vehicle(self, vehicle_id: str, route_id: str, limit: int = 1000, max_age_hours: int = 8) -> List[Dict[str, Any]]:
        """Retrieve vehicle positions for a specific vehicle from SQLite

        This is the preferred method for querying positions as it filters by vehicle_id rather than trip_id.
        Trip IDs are often not updated promptly when vehicles change trips.

        Args:
            vehicle_id: The vehicle ID to search for
            route_id: The route ID to filter by
            limit: Maximum number of positions to return
            max_age_hours: Only return positions within this many hours (default: 8, matching in-memory max_age)

        Returns:
            List of position dictionaries, ordered by timestamp ascending (oldest first)
        """
        cutoff_timestamp = int((datetime.now() - timedelta(hours=max_age_hours)).timestamp())

        async with self._pool_semaphore:
            try:
                async with aiosqlite.connect(str(self.db_path)) as conn:
                    conn.row_factory = aiosqlite.Row
                    cursor = await conn.execute('''
                        SELECT vehicle_id, trip_id, route_id, lat, lon, bearing, timestamp, speed, status
                        FROM vehicle_positions
                        WHERE vehicle_id = ? AND route_id = ? AND timestamp >= ?
                        ORDER BY timestamp ASC
                        LIMIT ?
                    ''', (vehicle_id, route_id, cutoff_timestamp, limit))

                    rows = await cursor.fetchall()

                    # Convert rows to dictionaries
                    positions = []
                    for row in rows:
                        positions.append({
                            "vehicle_id": row["vehicle_id"],
                            "trip_id": row["trip_id"],
                            "route_id": row["route_id"],
                            "lat": row["lat"],
                            "lon": row["lon"],
                            "bearing": row["bearing"],
                            "timestamp": row["timestamp"],
                            "speed": row["speed"],
                            "status": row["status"]
                        })

                    logger.debug(f"Retrieved {len(positions)} positions from SQLite for vehicle {vehicle_id} on route {route_id}")
                    return positions

            except Exception as e:
                logger.error(f"Error retrieving vehicle positions for vehicle_id={vehicle_id}, route_id={route_id}: {e}")
                return []

    async def get_positions_by_trip(self, trip_id: str, route_id: str, limit: int = 1000, max_age_hours: int = 8) -> List[Dict[str, Any]]:
        """Retrieve vehicle positions for a specific trip from SQLite

        DEPRECATED: Prefer get_positions_by_vehicle() as trip IDs are not updated promptly.
        This method is kept for backward compatibility.

        Args:
            trip_id: The trip ID to search for
            route_id: The route ID to filter by
            limit: Maximum number of positions to return
            max_age_hours: Only return positions within this many hours (default: 8, matching in-memory max_age)

        Returns:
            List of position dictionaries, ordered by timestamp ascending (oldest first)
        """
        cutoff_timestamp = int((datetime.now() - timedelta(hours=max_age_hours)).timestamp())

        async with self._pool_semaphore:
            try:
                async with aiosqlite.connect(str(self.db_path)) as conn:
                    conn.row_factory = aiosqlite.Row
                    cursor = await conn.execute('''
                        SELECT vehicle_id, trip_id, route_id, lat, lon, bearing, timestamp, speed, status
                        FROM vehicle_positions
                        WHERE trip_id = ? AND route_id = ? AND timestamp >= ?
                        ORDER BY timestamp ASC
                        LIMIT ?
                    ''', (trip_id, route_id, cutoff_timestamp, limit))

                    rows = await cursor.fetchall()

                    # Convert rows to dictionaries
                    positions = []
                    for row in rows:
                        positions.append({
                            "vehicle_id": row["vehicle_id"],
                            "trip_id": row["trip_id"],
                            "route_id": row["route_id"],
                            "lat": row["lat"],
                            "lon": row["lon"],
                            "bearing": row["bearing"],
                            "timestamp": row["timestamp"],
                            "speed": row["speed"],
                            "status": row["status"]
                        })

                    logger.debug(f"Retrieved {len(positions)} positions from SQLite for trip {trip_id} on route {route_id}")
                    return positions

            except Exception as e:
                logger.error(f"Error retrieving vehicle positions for trip_id={trip_id}, route_id={route_id}: {e}")
                return []

    async def cleanup_old_data(self, retention_days: int = 365):
        """Cleanup old vehicle positions and trip data"""
        cutoff_date = (datetime.now() - timedelta(days=retention_days)).strftime('%Y-%m-%d')
        cutoff_timestamp = int((datetime.now() - timedelta(days=retention_days)).timestamp())

        async with self._pool_semaphore:
            try:
                async with aiosqlite.connect(str(self.db_path)) as conn:
                    # Clean old completed stop times
                    result1 = await conn.execute('''
                        DELETE FROM completed_stop_times
                        WHERE date < ?
                    ''', (cutoff_date,))

                    # Clean old vehicle positions
                    result2 = await conn.execute('''
                        DELETE FROM vehicle_positions
                        WHERE timestamp < ?
                    ''', (cutoff_timestamp,))

                    await conn.commit()

                    logger.info(f"Cleaned up old data: {result1.rowcount} completed stops, {result2.rowcount} vehicle positions")

            except Exception as e:
                logger.error(f"Error cleaning up old data: {e}")
