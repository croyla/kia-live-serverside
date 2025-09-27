"""
Live data repository for the new architecture.
Manages live vehicle and trip data with bounded memory usage and TTL-based cleanup.
"""

import asyncio
import gc
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Any
from collections import defaultdict
import weakref
import time

from ..models.vehicle import Vehicle
from ..models.trip import Trip, TripSchedule
from ...utils.memory_utils import BoundedDict, BoundedCache
from ...core.resource_manager import ResourceManager

logger = logging.getLogger(__name__)


class LiveDataRepository:
    """Repository for live data with bounded memory usage and automatic cleanup"""
    
    def __init__(self, resource_manager: ResourceManager, database_service, max_memory_mb: int = 50):
        self.resource_manager = resource_manager
        self.database_service = database_service
        self.max_memory_mb = max_memory_mb
        
        # Bounded storage with TTL and memory limits
        vehicle_memory_mb = int(max_memory_mb * 0.4)  # 40% for vehicles
        trip_memory_mb = int(max_memory_mb * 0.5)     # 50% for trips
        cache_memory_mb = int(max_memory_mb * 0.1)    # 10% for general cache
        
        self._vehicles = BoundedDict(
            max_memory_mb=vehicle_memory_mb,
            ttl_seconds=900,  # 15 minutes
            name="vehicles"
        )
        
        self._trips = BoundedDict(
            max_memory_mb=trip_memory_mb,
            ttl_seconds=3600,  # 1 hour
            name="trips"
        )
        
        self._vehicle_trip_mapping = BoundedDict(
            max_memory_mb=cache_memory_mb,
            ttl_seconds=1800,  # 30 minutes
            name="vehicle_trip_mapping"
        )
        
        # Trip schedules (static data, longer TTL)
        self._trip_schedules: Dict[str, TripSchedule] = {}
        
        # Historical data storage
        self._historical_vehicle_locations = BoundedDict(
            max_memory_mb=int(max_memory_mb * 0.3),  # 30% for historical vehicle data
            ttl_seconds=86400,  # 24 hours
            name="historical_vehicle_locations"
        )
        
        self._historical_stop_times = BoundedDict(
            max_memory_mb=int(max_memory_mb * 0.2),  # 20% for historical stop times
            ttl_seconds=86400,  # 24 hours
            name="historical_stop_times"
        )
        
        # Indexes for efficient querying
        self._vehicles_by_route: Dict[str, Set[str]] = defaultdict(set)
        self._trips_by_route: Dict[str, Set[str]] = defaultdict(set)
        self._active_trips: Set[str] = set()
        
        # Cleanup task
        self._cleanup_task: Optional[asyncio.Task] = None
        self._cleanup_interval = 300  # 5 minutes
        
        # Batch processing for database operations
        self._pending_vehicle_positions: List[Dict[str, Any]] = []
        self._batch_size = 100
        self._batch_timeout = 30  # seconds
        self._last_batch_time = 0
        
    async def start(self):
        """Start the repository and background cleanup tasks"""
        if self._cleanup_task is None or self._cleanup_task.done():
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())
    
    async def stop(self):
        """Stop background tasks and cleanup"""
        # Flush any remaining vehicle positions before stopping
        try:
            await self._flush_vehicle_positions_batch()
        except Exception as e:
            logger.error(f"Error flushing final vehicle positions: {e}")
            
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
    
    async def store_vehicle(self, vehicle: Vehicle):
        """Store vehicle data with automatic indexing"""
        vehicle_key = f"{vehicle.route_id}:{vehicle.id}"
        
        # Store vehicle
        self._vehicles[vehicle_key] = vehicle
        
        # Update indexes
        self._vehicles_by_route[vehicle.route_id].add(vehicle.id)
        
        # Store vehicle-trip mapping if trip is active
        if vehicle.is_trip_active():
            trip_key = self._get_trip_key(vehicle.route_id, vehicle.scheduled_trip_start_time)
            self._vehicle_trip_mapping[vehicle.id] = trip_key
        
        # Save unique vehicle position to database (batched for efficiency)
        if self.database_service:
            try:
                # Try to find the actual GTFS trip ID for this vehicle
                trip_id = None
                if vehicle.is_trip_active() and vehicle.scheduled_trip_start_time:
                    # Look for an existing trip that matches this vehicle
                    trip = await self.get_trip_by_route_and_start_time(
                        vehicle.route_id, 
                        vehicle.scheduled_trip_start_time
                    )
                    if trip:
                        trip_id = trip.id
                    else:
                        # Fallback to a generated trip ID format
                        trip_id = f"{vehicle.route_id}_{vehicle.scheduled_trip_start_time.replace(':', '')}"
                
                position_data = {
                    "vehicle_id": vehicle.id,
                    "trip_id": trip_id,  # Use actual GTFS trip ID or None
                    "route_id": vehicle.route_id,
                    "lat": vehicle.position.latitude,
                    "lon": vehicle.position.longitude,
                    "bearing": vehicle.position.bearing,
                    "timestamp": int(vehicle.position.timestamp.timestamp()),
                    "speed": None,  # Not available in current data model
                    "status": "ACTIVE" if vehicle.is_trip_active() else "INACTIVE"
                }
                
                # Add to batch for efficient processing
                self._pending_vehicle_positions.append(position_data)
                
                # Process batch if it's full or enough time has passed
                current_time = time.time()
                if (len(self._pending_vehicle_positions) >= self._batch_size or 
                    current_time - self._last_batch_time >= self._batch_timeout):
                    await self._flush_vehicle_positions_batch()
                    
            except Exception as e:
                logger.error(f"Failed to batch vehicle position for database: {e}")
    
    async def get_vehicle(self, vehicle_id: str, route_id: str) -> Optional[Vehicle]:
        """Get vehicle by ID and route"""
        vehicle_key = f"{route_id}:{vehicle_id}"
        return self._vehicles.get(vehicle_key)
    
    async def get_vehicles_for_route(self, route_id: str) -> List[Vehicle]:
        """Get all vehicles for a specific route"""
        vehicle_ids = self._vehicles_by_route.get(route_id, set())
        vehicles = []
        
        for vehicle_id in list(vehicle_ids):  # Copy to avoid iteration issues
            vehicle_key = f"{route_id}:{vehicle_id}"
            vehicle = self._vehicles.get(vehicle_key)
            if vehicle:
                vehicles.append(vehicle)
            else:
                # Clean up stale index entry
                self._vehicles_by_route[route_id].discard(vehicle_id)
        
        return vehicles
    
    async def get_active_vehicles(self) -> List[Vehicle]:
        """Get all vehicles that are currently on active trips"""
        active_vehicles = []
        
        for vehicle_key, vehicle in self._vehicles.items():
            if vehicle.is_trip_active():
                active_vehicles.append(vehicle)
        
        return active_vehicles
    
    async def store_trip(self, trip: Trip):
        """Store trip data with automatic indexing"""
        trip_key = self._get_trip_key(trip.route_id, trip.start_time.strftime("%H:%M"))
        
        # Store trip
        self._trips[trip_key] = trip
        
        # Update indexes
        self._trips_by_route[trip.route_id].add(trip.id)
        
        if trip.is_active and not trip.is_completed:
            self._active_trips.add(trip.id)
        else:
            self._active_trips.discard(trip.id)
    
    async def get_trip(self, trip_id: str) -> Optional[Trip]:
        """Get trip by ID"""
        for trip_key, trip in self._trips.items():
            if trip.id == trip_id:
                return trip
        return None
    
    async def get_trip_by_route_and_start_time(self, route_id: str, start_time: str) -> Optional[Trip]:
        """Get trip by route and start time"""
        # print(f"VEHICLE TRIP RUNNING {start_time}")
        trip_key = self._get_trip_key(route_id, start_time)
        # print("SELF TRIP KEYS ", self._trips.__dict__["_data"].keys())
        return self._trips.get(trip_key)
    
    async def get_trips_for_route(self, route_id: str) -> List[Trip]:
        """Get all trips for a specific route"""
        trip_ids = self._trips_by_route.get(route_id, set())
        trips = []
        
        for trip_id in list(trip_ids):  # Copy to avoid iteration issues
            trip = await self.get_trip(trip_id)
            if trip:
                trips.append(trip)
            else:
                # Clean up stale index entry
                self._trips_by_route[route_id].discard(trip_id)
        
        return trips
    
    async def get_active_trips(self) -> List[Trip]:
        """Get all currently active trips"""
        active_trips = []
        # print("SENDING ACTIVE TRIPS!!")
        for trip_id in list(self._active_trips):  # Copy to avoid iteration issues
            trip = await self.get_trip(trip_id)
            if trip and trip.is_active and not trip.is_completed:
                active_trips.append(trip)
            else:
                # Clean up stale index entry
                self._active_trips.discard(trip_id)
        # print("GOT LENGTH OF ACTIVE TRIPS", len(active_trips))
        return active_trips
    
    async def match_vehicle_to_trip(self, vehicle: Vehicle) -> Optional[Trip]:
        """Match a vehicle to an appropriate trip based on schedule"""
        if not vehicle.scheduled_trip_start_time:
            return None
        
        # Try direct trip lookup first
        trip = await self.get_trip_by_route_and_start_time(
            vehicle.route_id, 
            vehicle.scheduled_trip_start_time
        )
        
        if trip:
            # print(f"FOUND TRIP FOR VEHICLE {trip.id}")
            utrip = trip.add_vehicle(vehicle.id)
            await self.store_trip(utrip)
            return utrip
        
        # Try to find trip from static schedule
        route_key = self._get_route_key_from_route_id(vehicle.route_id)
        if route_key and route_key in self._trip_schedules:
            schedule = self._trip_schedules[route_key]
            
            # Parse vehicle start time
            try:
                from datetime import datetime
                import pytz
                local_tz = pytz.timezone("Asia/Kolkata")
                
                hh, mm = map(int, vehicle.scheduled_trip_start_time.split(":"))
                now = datetime.now(local_tz)
                start_datetime = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
                
                # Handle midnight crossings
                if start_datetime < now - timedelta(hours=6):
                    start_datetime += timedelta(days=1)
                elif start_datetime > now + timedelta(hours=18):
                    start_datetime -= timedelta(days=1)
                
                # Find matching trip in schedule
                matched_trip = schedule.get_trip_by_start_time(start_datetime, tolerance_minutes=2)
                if matched_trip:
                    # print(f"MATCHED TRIP FOR VEHICLE {matched_trip.id}")
                    # Add vehicle to trip and store
                    updated_trip = matched_trip.add_vehicle(vehicle.id)
                    await self.store_trip(updated_trip)
                    return updated_trip
                    
            except (ValueError, TypeError):
                pass
        # print(f"NOT MATCHED FOR VEHICLE {vehicle.id} {vehicle.route_id}")
        return None
    
    async def update_trip_with_vehicle_data(self, trip: Trip, vehicle: Vehicle) -> Trip:
        """Update trip data with vehicle information"""
        if not vehicle.current_station_id:
            print(f"RETURNING TRIP AS NO CURRENT_STATION_ID {trip.id} {vehicle.id}")
            return trip
        
        # Update trip stop status
        updated_trip = trip.update_stop_status(vehicle.current_station_id, vehicle)
        
        # Check if this update completed any stops and log to database
        if self.database_service and vehicle.actual_arrival_time and vehicle.actual_departure_time:
            try:
                # Find the corresponding stop that was just completed
                for stop in updated_trip.stops:
                    if stop.stop_id == vehicle.current_station_id and stop.is_completed():
                        stop_data = {
                            "stop_id": stop.stop_id,
                            "trip_id": updated_trip.id,
                            "route_id": updated_trip.route_id,
                            "date": updated_trip.start_time.strftime("%Y-%m-%d"),
                            "actual_arrival": vehicle.actual_arrival_time,
                            "actual_departure": vehicle.actual_departure_time,
                            "scheduled_arrival": stop.scheduled_arrival.strftime("%H:%M") if stop.scheduled_arrival else None,
                            "scheduled_departure": stop.scheduled_departure.strftime("%H:%M") if stop.scheduled_departure else None
                        }
                        await self.database_service.log_completed_trip_stop(stop_data)
                        break
            except Exception as e:
                logger.error(f"Failed to log completed trip stop to database: {e}")
        
        # Store updated trip
        await self.store_trip(updated_trip)
        
        return updated_trip
    
    def set_trip_schedules(self, trip_schedules: Dict[str, TripSchedule]):
        """Set static trip schedules for trip matching"""
        self._trip_schedules = trip_schedules
    
    def _get_trip_key(self, route_id: str, start_time: str) -> str:
        """Generate consistent trip key"""
        return f"{route_id}:{start_time}"
    
    def _get_route_key_from_route_id(self, route_id: str) -> Optional[str]:
        """Get route key from route ID (reverse lookup)"""
        # This would typically use a mapping from route_id to route_key
        # For now, we'll search through trip schedules
        for route_key, schedule in self._trip_schedules.items():
            if schedule.route_id == route_id:
                return route_key
        return None
    
    async def _flush_vehicle_positions_batch(self):
        """Flush pending vehicle positions to database"""
        if not self._pending_vehicle_positions or not self.database_service:
            return
            
        try:
            positions_to_flush = self._pending_vehicle_positions.copy()
            self._pending_vehicle_positions.clear()
            self._last_batch_time = time.time()
            
            # Use batch insert for efficiency
            await self.database_service.batch_insert_positions(positions_to_flush)
            logger.debug(f"Flushed {len(positions_to_flush)} vehicle positions to database")
            
        except Exception as e:
            logger.error(f"Failed to flush vehicle positions batch to database: {e}")
            # Re-add positions to retry later if they're not too old
            current_time = time.time()
            recent_positions = [
                pos for pos in positions_to_flush 
                if current_time - pos.get("timestamp", 0) < 300  # Keep positions less than 5 minutes old
            ]
            self._pending_vehicle_positions.extend(recent_positions)

    async def _cleanup_loop(self):
        """Background cleanup task"""
        while True:
            try:
                await asyncio.sleep(self._cleanup_interval)
                await self._cleanup_expired_data()
                await self._cleanup_indexes()
                
                # Flush any pending vehicle positions
                await self._flush_vehicle_positions_batch()
                
                # Force garbage collection periodically
                gc.collect()
                
            except asyncio.CancelledError:
                # Final flush before exiting
                try:
                    await self._flush_vehicle_positions_batch()
                except:
                    pass
                break
            except Exception as e:
                print(f"[LiveDataRepository] Cleanup error: {e}")
    
    async def _cleanup_expired_data(self):
        """Clean up expired data from bounded dictionaries"""
        # BoundedDict handles its own TTL cleanup, but we can trigger it
        self._vehicles.cleanup_expired()
        self._trips.cleanup_expired()
        self._vehicle_trip_mapping.cleanup_expired()
    
    async def _cleanup_indexes(self):
        """Clean up stale index entries"""
        # Clean vehicle indexes
        for route_id, vehicle_ids in list(self._vehicles_by_route.items()):
            valid_ids = set()
            for vehicle_id in list(vehicle_ids):
                vehicle_key = f"{route_id}:{vehicle_id}"
                if vehicle_key in self._vehicles:
                    valid_ids.add(vehicle_id)
            
            if valid_ids:
                self._vehicles_by_route[route_id] = valid_ids
            else:
                del self._vehicles_by_route[route_id]
        
        # Clean trip indexes
        for route_id, trip_ids in list(self._trips_by_route.items()):
            valid_ids = set()
            for trip_id in list(trip_ids):
                # Check if trip still exists
                trip_found = False
                for trip_key, trip in self._trips.items():
                    if trip.id == trip_id:
                        valid_ids.add(trip_id)
                        trip_found = True
                        break
                
                if not trip_found:
                    self._active_trips.discard(trip_id)
            
            if valid_ids:
                self._trips_by_route[route_id] = valid_ids
            else:
                del self._trips_by_route[route_id]
    
    def get_memory_stats(self) -> Dict[str, any]:
        """Get memory usage statistics"""
        return {
            "vehicles": self._vehicles.get_stats(),
            "trips": self._trips.get_stats(),
            "vehicle_trip_mapping": self._vehicle_trip_mapping.get_stats(),
            "indexes": {
                "vehicles_by_route": len(self._vehicles_by_route),
                "trips_by_route": len(self._trips_by_route),
                "active_trips": len(self._active_trips)
            },
            "trip_schedules_loaded": len(self._trip_schedules)
        }
    
    async def get_repository_stats(self) -> Dict[str, any]:
        """Get comprehensive repository statistics"""
        memory_stats = self.get_memory_stats()
        
        # Count active vs inactive items
        active_vehicles = len(await self.get_active_vehicles())
        active_trips = len(await self.get_active_trips())
        
        return {
            "memory": memory_stats,
            "counts": {
                "total_vehicles": len(self._vehicles),
                "active_vehicles": active_vehicles,
                "total_trips": len(self._trips),
                "active_trips": active_trips,
                "routes_with_vehicles": len(self._vehicles_by_route),
                "routes_with_trips": len(self._trips_by_route)
            },
            "health": {
                "cleanup_task_running": self._cleanup_task and not self._cleanup_task.done(),
                "memory_pressure": sum(
                    stats.get("memory_usage_mb", 0) 
                    for stats in memory_stats.values() 
                    if isinstance(stats, dict)
                ) / self.max_memory_mb
            }
        }


class CacheRepository:
    """Simple cache repository for frequently accessed data"""
    
    def __init__(self, max_size_mb: int = 10, default_ttl_seconds: int = 300):
        self._cache = BoundedCache(max_size_mb=max_size_mb, ttl_seconds=default_ttl_seconds)
    
    async def get(self, key: str) -> Optional[any]:
        """Get cached value"""
        return self._cache.get(key)
    
    async def set(self, key: str, value: any, ttl_seconds: Optional[int] = None):
        """Set cached value with optional TTL override"""
        self._cache.set(key, value, ttl_seconds)
    
    async def delete(self, key: str):
        """Delete cached value"""
        self._cache.delete(key)
    
    async def clear(self):
        """Clear all cached values"""
        self._cache.clear()
    
    def get_stats(self) -> Dict[str, any]:
        """Get cache statistics"""
        return self._cache.get_stats()
