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
        self.connection_pool_size = 10  # Increased from 5 to 10 for better concurrency
        self._initialized = False

        # True connection pooling
        self._connection_pool: List[aiosqlite.Connection] = []
        self._available_connections = asyncio.Queue()
        self._pool_lock = asyncio.Lock()
        
    async def initialize(self):
        """Initialize database schema and connection pool"""
        if self._initialized:
            return

        # Ensure the directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Create initial connection for schema setup
        async with aiosqlite.connect(str(self.db_path)) as conn:
            # Enable WAL mode for concurrent read/write access (CRITICAL FIX)
            await conn.execute("PRAGMA journal_mode=WAL")
            await conn.execute("PRAGMA synchronous=NORMAL")  # Faster, still safe with WAL
            await conn.execute("PRAGMA cache_size=-4000")   # 64MB cache (restored)
            await conn.execute("PRAGMA busy_timeout=500")   # .5 second timeout - fail fast to prevent cascading delays
            await conn.execute("PRAGMA temp_store=MEMORY")   # Keep temp tables in memory

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

        # Create connection pool with persistent connections
        async with self._pool_lock:
            for i in range(self.connection_pool_size):
                conn = await aiosqlite.connect(str(self.db_path))
                # Configure each pooled connection
                await conn.execute("PRAGMA journal_mode=WAL")
                await conn.execute("PRAGMA synchronous=NORMAL")
                await conn.execute("PRAGMA busy_timeout=500")  # Match schema connection - fail fast
                await conn.execute("PRAGMA temp_store=MEMORY")
                self._connection_pool.append(conn)
                await self._available_connections.put(conn)

        self._initialized = True
        logger.info(f"Database initialized at {self.db_path} with {self.connection_pool_size} pooled connections (WAL mode enabled)")

    async def _get_connection(self, timeout: float = 3.0) -> aiosqlite.Connection:
        """
        Get a connection from the pool with timeout to prevent cascading delays.

        Args:
            timeout: Maximum time to wait for a connection (default: 3 seconds)

        Returns:
            A database connection from the pool

        Raises:
            RuntimeError: If connection pool is exhausted after timeout
        """
        try:
            return await asyncio.wait_for(
                self._available_connections.get(),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            logger.error(f"Connection pool exhausted - timeout after {timeout}s waiting for connection")
            raise RuntimeError("Database connection pool exhausted - all connections busy")

    async def _return_connection(self, conn: aiosqlite.Connection):
        """Return a connection to the pool"""
        await self._available_connections.put(conn)

    async def stop(self):
        """Stop method for application lifecycle (alias for close)"""
        await self.close()

    async def close(self):
        """Close all pooled connections"""
        async with self._pool_lock:
            for conn in self._connection_pool:
                await conn.close()
            self._connection_pool.clear()
            logger.info("Database connection pool closed")

    async def checkpoint_wal(self):
        """Checkpoint WAL file to main database (periodic maintenance)"""
        conn = await self._get_connection()
        try:
            await conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
            logger.debug("WAL checkpoint completed")
        except Exception as e:
            logger.error(f"Error during WAL checkpoint: {e}")
        finally:
            await self._return_connection(conn)

    async def log_completed_trip_stop(self, stop_data: Dict[str, Any]):
        """Log actual arrival/departure times for completed stops"""
        conn = await self._get_connection()
        try:
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
        finally:
            await self._return_connection(conn)

    async def log_vehicle_position(self, position_data: Dict[str, Any]):
        """Log vehicle position for ongoing trips"""
        conn = await self._get_connection()
        try:
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
        finally:
            await self._return_connection(conn)

    async def batch_insert_positions(self, positions: List[Dict[str, Any]]):
        """
        Batch insert vehicle positions - optimized for performance.

        Single commit strategy with WAL mode allows concurrent reads during insert.
        No artificial delays - WAL mode handles concurrency naturally.
        """
        if not positions:
            return

        conn = await self._get_connection()
        try:
            # Insert all positions in one transaction (WAL mode allows concurrent reads)
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

            # Single commit at the end
            await conn.commit()

            logger.debug(f"Batch inserted {len(positions)} vehicle positions")

        except Exception as e:
            logger.error(f"Error batch inserting vehicle positions: {e}")
        finally:
            await self._return_connection(conn)

    async def get_recent_vehicle_positions(self, vehicle_id: str, route_id: str, limit: int = 500, max_age_hours: int = 6) -> List[Dict[str, Any]]:
        """Retrieve recent vehicle positions from SQLite (read-only operation)

        Args:
            vehicle_id: The vehicle ID to search for
            route_id: The route ID to filter by
            limit: Maximum number of positions to return
            max_age_hours: Only return positions within this many hours

        Returns:
            List of position dictionaries, ordered by timestamp descending (most recent first)
        """
        cutoff_timestamp = int((datetime.now() - timedelta(hours=max_age_hours)).timestamp())

        conn = await self._get_connection()
        try:
            conn.row_factory = aiosqlite.Row
            cursor = await conn.execute('''
                SELECT vehicle_id, trip_id, route_id, lat, lon, bearing, timestamp, speed, status
                FROM vehicle_positions
                WHERE vehicle_id = ? AND route_id = ? AND timestamp >= ?
                ORDER BY timestamp DESC
                LIMIT ?
            ''', (vehicle_id, route_id, cutoff_timestamp, limit))

            rows = await cursor.fetchall()

            # Optimized: Convert rows to dictionaries using list comprehension
            positions = [dict(row) for row in rows]

            logger.debug(f"Retrieved {len(positions)} positions from SQLite for vehicle {vehicle_id} on route {route_id}")
            return positions

        except Exception as e:
            logger.error(f"Error retrieving vehicle positions for vehicle_id={vehicle_id}, route_id={route_id}: {e}")
            return []
        finally:
            await self._return_connection(conn)

    async def get_positions_by_vehicle(self, vehicle_id: str, route_id: str, limit: int = 1000, max_age_hours: int = 8) -> List[Dict[str, Any]]:
        """Retrieve vehicle positions for a specific vehicle from SQLite (read-only operation)

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

        conn = await self._get_connection()
        try:
            conn.row_factory = aiosqlite.Row
            cursor = await conn.execute('''
                SELECT vehicle_id, trip_id, route_id, lat, lon, bearing, timestamp, speed, status
                FROM vehicle_positions
                WHERE vehicle_id = ? AND route_id = ? AND timestamp >= ?
                ORDER BY timestamp ASC
                LIMIT ?
            ''', (vehicle_id, route_id, cutoff_timestamp, limit))

            rows = await cursor.fetchall()

            # Optimized: Convert rows to dictionaries using list comprehension
            positions = [dict(row) for row in rows]

            logger.debug(f"Retrieved {len(positions)} positions from SQLite for vehicle {vehicle_id} on route {route_id}")
            return positions

        except Exception as e:
            logger.error(f"Error retrieving vehicle positions for vehicle_id={vehicle_id}, route_id={route_id}: {e}")
            return []
        finally:
            await self._return_connection(conn)

    async def get_positions_by_trip(self, trip_id: str, route_id: str, limit: int = 1000, max_age_hours: int = 8) -> List[Dict[str, Any]]:
        """Retrieve vehicle positions for a specific trip from SQLite (read-only operation)

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

        conn = await self._get_connection()
        try:
            conn.row_factory = aiosqlite.Row
            cursor = await conn.execute('''
                SELECT vehicle_id, trip_id, route_id, lat, lon, bearing, timestamp, speed, status
                FROM vehicle_positions
                WHERE trip_id = ? AND route_id = ? AND timestamp >= ?
                ORDER BY timestamp ASC
                LIMIT ?
            ''', (trip_id, route_id, cutoff_timestamp, limit))

            rows = await cursor.fetchall()

            # Optimized: Convert rows to dictionaries using list comprehension
            positions = [dict(row) for row in rows]

            logger.debug(f"Retrieved {len(positions)} positions from SQLite for trip {trip_id} on route {route_id}")
            return positions

        except Exception as e:
            logger.error(f"Error retrieving vehicle positions for trip_id={trip_id}, route_id={route_id}: {e}")
            return []
        finally:
            await self._return_connection(conn)

    async def cleanup_old_data(self, retention_days: int = 365):
        """Cleanup old vehicle positions and trip data"""
        cutoff_date = (datetime.now() - timedelta(days=retention_days)).strftime('%Y-%m-%d')
        cutoff_timestamp = int((datetime.now() - timedelta(days=retention_days)).timestamp())

        conn = await self._get_connection()
        try:
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
        finally:
            await self._return_connection(conn)
