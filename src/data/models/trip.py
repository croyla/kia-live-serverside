"""
Trip and TripStop data models for the new architecture.
Immutable data structures representing trip scheduling and stop information.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Set
import pytz
from .vehicle import Vehicle, StationInfo

local_tz = pytz.timezone("Asia/Kolkata")


@dataclass(frozen=True)
class TripStop:
    """Immutable trip stop data model"""
    stop_id: str
    sequence: int  # Stop sequence in trip
    station_info: Optional[StationInfo] = None
    
    # Scheduled times (from static schedule)
    scheduled_arrival: Optional[datetime] = None
    scheduled_departure: Optional[datetime] = None
    
    # Actual times (from live data)
    actual_arrival: Optional[datetime] = None
    actual_departure: Optional[datetime] = None
    
    # Prediction times (calculated)
    predicted_arrival: Optional[datetime] = None
    predicted_departure: Optional[datetime] = None
    
    # Stop status
    is_passed: bool = False
    is_skipped: bool = False
    confidence: Optional[float] = None
    
    def get_effective_arrival(self) -> Optional[datetime]:
        """Get the most appropriate arrival time (actual > predicted > scheduled)"""
        return self.actual_arrival or self.predicted_arrival or self.scheduled_arrival
    
    def get_effective_departure(self) -> Optional[datetime]:
        """Get the most appropriate departure time (actual > predicted > scheduled)"""
        return self.actual_departure or self.predicted_departure or self.scheduled_departure
    
    def get_delay_seconds(self) -> Optional[int]:
        """Calculate delay in seconds compared to scheduled time"""
        scheduled = self.scheduled_arrival
        effective = self.get_effective_arrival()
        
        if not scheduled or not effective:
            return None
            
        return int((effective - scheduled).total_seconds())
    
    def is_completed(self) -> bool:
        """Check if this stop has been completed (vehicle has departed)"""
        return bool(self.actual_departure)
    
    @classmethod
    def from_static_data(cls, stop_data: dict, sequence: int, station_info: Optional[StationInfo] = None) -> 'TripStop':
        """Create TripStop from static times.json data"""
        # stop_time is in minutes from midnight (int_time format)
        stop_time_minutes = stop_data.get("stop_time", 0)
        
        # Convert to datetime
        scheduled_time = None
        if stop_time_minutes:
            try:
                # Convert minutes to hours:minutes
                hours = stop_time_minutes // 60
                minutes = stop_time_minutes % 60
                
                # Create datetime for today
                now = datetime.now(local_tz)
                scheduled_time = now.replace(hour=hours, minute=minutes, second=0, microsecond=0)
                
                # Handle midnight crossings
                if scheduled_time < now - timedelta(hours=6):
                    scheduled_time += timedelta(days=1)
                elif scheduled_time > now + timedelta(hours=18):
                    scheduled_time -= timedelta(days=1)
                    
            except (ValueError, TypeError):
                scheduled_time = None
        
        return cls(
            stop_id=str(stop_data.get("stop_id", "")),
            sequence=sequence,
            station_info=station_info,
            scheduled_arrival=scheduled_time,
            scheduled_departure=scheduled_time,  # Same time for arrival and departure in static data
            confidence=stop_data.get("confidence")
        )


@dataclass(frozen=True)
class Trip:
    """Immutable trip data model"""
    id: str  # Unique trip identifier (e.g., "2985_1")
    route_id: str
    route_key: str  # Route name key (e.g., "KIA-4 UP")
    
    # Trip timing
    start_time: datetime
    duration_minutes: float
    
    # Stops on this trip
    stops: List[TripStop] = field(default_factory=list)
    
    # Associated vehicles
    vehicles: Set[str] = field(default_factory=set)  # Set of vehicle IDs
    
    # Trip status
    is_active: bool = False
    is_completed: bool = False
    
    # Service information
    service_id: str = "ALL"  # GTFS service ID
    shape_id: Optional[str] = None  # GTFS shape ID
    
    def get_current_stop(self) -> Optional[TripStop]:
        """Get the current/next stop for this trip"""
        for stop in self.stops:
            if not stop.is_passed and not stop.is_skipped:
                return stop
        return None
    
    def get_passed_stops(self) -> List[TripStop]:
        """Get all stops that have been passed"""
        return [stop for stop in self.stops if stop.is_passed]
    
    def get_remaining_stops(self) -> List[TripStop]:
        """Get all stops that haven't been passed yet"""
        return [stop for stop in self.stops if not stop.is_passed and not stop.is_skipped]
    
    def get_completed_stops(self) -> List[TripStop]:
        """Get all stops that have been completed (vehicle departed)"""
        return [stop for stop in self.stops if stop.is_completed()]
    
    def get_overall_delay_seconds(self) -> Optional[int]:
        """Calculate overall trip delay based on completed stops"""
        completed_stops = self.get_completed_stops()
        if not completed_stops:
            return None
            
        # Average delay across completed stops
        delays = [stop.get_delay_seconds() for stop in completed_stops]
        valid_delays = [d for d in delays if d is not None]
        
        if not valid_delays:
            return None
            
        return int(sum(valid_delays) / len(valid_delays))
    
    def is_trip_started(self) -> bool:
        """Check if trip has started (at least one stop passed)"""
        return len(self.get_passed_stops()) > 0
    
    def is_trip_finished(self) -> bool:
        """Check if trip is finished (all stops completed)"""
        return len(self.stops) > 0 and len(self.get_completed_stops()) == len(self.stops)
    
    def get_progress_percentage(self) -> float:
        """Get trip progress as percentage (0-100)"""
        if not self.stops:
            return 0.0
            
        completed = len(self.get_completed_stops())
        return (completed / len(self.stops)) * 100.0
    
    def add_vehicle(self, vehicle_id: str) -> 'Trip':
        """Add a vehicle to this trip (returns new Trip instance)"""
        new_vehicles = self.vehicles.copy()
        new_vehicles.add(vehicle_id)
        
        return Trip(
            id=self.id,
            route_id=self.route_id,
            route_key=self.route_key,
            start_time=self.start_time,
            duration_minutes=self.duration_minutes,
            stops=self.stops,
            vehicles=new_vehicles,
            is_active=self.is_active,
            is_completed=self.is_completed,
            service_id=self.service_id,
            shape_id=self.shape_id
        )
    
    def update_stop_status(self, stop_id: str, vehicle: Vehicle) -> 'Trip':
        """Update stop status based on vehicle data (returns new Trip instance)"""
        updated_stops = []
        
        for stop in self.stops:
            if stop.stop_id == stop_id:
                # Update this stop with vehicle data
                updated_stop = TripStop(
                    stop_id=stop.stop_id,
                    sequence=stop.sequence,
                    station_info=stop.station_info,
                    scheduled_arrival=stop.scheduled_arrival,
                    scheduled_departure=stop.scheduled_departure,
                    actual_arrival=self._parse_time(vehicle.actual_arrival_time) if vehicle.actual_arrival_time else stop.actual_arrival,
                    actual_departure=self._parse_time(vehicle.actual_departure_time) if vehicle.actual_departure_time else stop.actual_departure,
                    predicted_arrival=stop.predicted_arrival,
                    predicted_departure=stop.predicted_departure,
                    is_passed=bool(vehicle.actual_departure_time),
                    is_skipped=stop.is_skipped,
                    confidence=stop.confidence
                )
                updated_stops.append(updated_stop)
            else:
                updated_stops.append(stop)
        
        return Trip(
            id=self.id,
            route_id=self.route_id,
            route_key=self.route_key,
            start_time=self.start_time,
            duration_minutes=self.duration_minutes,
            stops=updated_stops,
            vehicles=self.vehicles,
            is_active=self.is_active,
            is_completed=self.is_trip_finished(),
            service_id=self.service_id,
            shape_id=self.shape_id
        )
    
    def _parse_time(self, time_str: str) -> Optional[datetime]:
        """Parse HH:MM time string to datetime"""
        if not time_str or ":" not in time_str:
            return None
            
        try:
            hh, mm = map(int, time_str.split(":"))
            now = datetime.now(local_tz)
            parsed_time = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
            
            # Handle midnight crossings
            if parsed_time < now - timedelta(hours=6):
                parsed_time += timedelta(days=1)
            elif parsed_time > now + timedelta(hours=18):
                parsed_time -= timedelta(days=1)
                
            return parsed_time
        except (ValueError, TypeError):
            return None
    
    @classmethod
    def from_static_data(cls, route_key: str, trip_data: dict, stations_data: Optional[Dict[str, StationInfo]] = None) -> 'Trip':
        """Create Trip from static times.json data"""
        route_id = str(trip_data.get("route_id", ""))
        start_minutes = trip_data.get("start", 0)
        duration = trip_data.get("duration", 0.0)
        
        # Convert start time to datetime
        hours = start_minutes // 60 if start_minutes >= 100 else 0
        minutes = start_minutes % 60 if start_minutes >= 100 else start_minutes
        
        now = datetime.now(local_tz)
        start_time = now.replace(hour=hours, minute=minutes, second=0, microsecond=0)
        
        # Handle midnight crossings
        if start_time < now - timedelta(hours=6):
            start_time += timedelta(days=1)
        elif start_time > now + timedelta(hours=18):
            start_time -= timedelta(days=1)
        
        # Create trip stops
        stops_data = trip_data.get("stops", [])
        trip_stops = []
        
        for i, stop_data in enumerate(stops_data):
            stop_id = str(stop_data.get("stop_id", ""))
            station_info = stations_data.get(stop_id) if stations_data else None
            
            trip_stop = TripStop.from_static_data(stop_data, i + 1, station_info)
            trip_stops.append(trip_stop)
        
        # Generate trip ID
        trip_id = f"{route_id}_{hash(start_time.isoformat()) % 1000}"
        
        return cls(
            id=trip_id,
            route_id=route_id,
            route_key=route_key,
            start_time=start_time,
            duration_minutes=duration,
            stops=trip_stops,
            is_active=True,  # Assume active if loaded from static data
            shape_id=f"sh_{route_id}"
        )


@dataclass(frozen=True)
class TripSchedule:
    """Immutable trip schedule data model for managing multiple trips on a route"""
    route_key: str
    route_id: str
    trips: List[Trip] = field(default_factory=list)
    total_distance: float = 0.0
    
    def get_active_trips(self) -> List[Trip]:
        """Get all currently active trips"""
        return [trip for trip in self.trips if trip.is_active and not trip.is_completed]
    
    def get_trip_by_start_time(self, start_time: datetime, tolerance_minutes: int = 2) -> Optional[Trip]:
        """Find trip by start time with tolerance"""
        tolerance = timedelta(minutes=tolerance_minutes)
        
        for trip in self.trips:
            if abs(trip.start_time - start_time) <= tolerance:
                return trip
        return None
    
    def get_trip_by_id(self, trip_id: str) -> Optional[Trip]:
        """Find trip by ID"""
        for trip in self.trips:
            if trip.id == trip_id:
                return trip
        return None
    
    @classmethod
    def from_static_data(cls, route_key: str, route_data: dict, stations_data: Optional[Dict[str, StationInfo]] = None) -> 'TripSchedule':
        """Create TripSchedule from static data"""
        trips_data = route_data if isinstance(route_data, list) else []
        
        # Extract route_id from first trip
        route_id = ""
        if trips_data:
            route_id = str(trips_data[0].get("route_id", ""))
        
        # Create trips
        trips = []
        for trip_data in trips_data:
            trip = Trip.from_static_data(route_key, trip_data, stations_data)
            trips.append(trip)
        
        return cls(
            route_key=route_key,
            route_id=route_id,
            trips=trips
        )
