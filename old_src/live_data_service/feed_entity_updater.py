from google.transit import gtfs_realtime_pb2
from datetime import datetime
import time
import gc
import weakref
import io
from collections import deque
from typing import Dict, Any
from old_src.shared import feed_message, feed_message_lock, ThreadSafeDict
from old_src.web_service import queue_gtfs_rt_broadcast


class ProtobufMemoryManager:
    """Manages protobuf serialization with aggressive memory cleanup"""
    
    def __init__(self):
        self._serialization_count = 0
        self._gc_frequency = 3  # More frequent cleanup - every 3 serializations instead of 10
        
    def serialize_with_cleanup(self, proto_message):
        """Serialize protobuf with aggressive memory cleanup"""
        try:
            # REMOVED: serialized_size = proto_message.ByteSize()  # This was causing 211MB accumulation
            # ByteSize() call removed as it was retaining references and causing memory leaks
            
            # Serialize the message directly without size calculation
            binary_data = proto_message.SerializeToString()
            
            self._serialization_count += 1
            
            # Periodic garbage collection to free protobuf memory pools
            if self._serialization_count % self._gc_frequency == 0:
                gc.collect()  # Force cleanup of protobuf internal buffers
                print(f"[Protobuf] Forced GC after {self._serialization_count} serializations")
            
            return binary_data
            
        except Exception as e:
            print(f"[Protobuf] Serialization error: {e}")
            raise
        finally:
            # Explicit cleanup of local references
            proto_message = None
            del proto_message

# Create global protobuf manager instance
protobuf_manager = ProtobufMemoryManager()

class MemoryEfficientEntityManager:
    """Memory-optimized entity manager with bounded storage and deduplication"""
    __slots__ = ['_entities', '_entity_keys', '_max_entities', '_max_age_seconds', 
                 '_message_pool', '_last_cleanup', '_cleanup_interval']
    
    def __init__(self, max_entities=25, max_age_seconds=300):  # Further reduced max entities for better memory efficiency
        self._entities = deque(maxlen=max_entities)  # Bounded entity storage
        self._entity_keys = set()  # Track unique entities for deduplication
        self._max_entities = max_entities
        self._max_age_seconds = max_age_seconds
        self._message_pool = deque(maxlen=5)  # Reuse protobuf messages
        self._last_cleanup = time.time()
        self._cleanup_interval = 15  # More aggressive cleanup every 15 seconds instead of 60
        
    def _cleanup_stale_entities(self):
        """Remove stale entities aggressively with bounded storage"""
        current_time = time.time()
        
        # Create new bounded storage for active entities
        active_entities = deque(maxlen=self._max_entities)
        active_keys = set()
        
        # Filter entities by age and validity
        for entity in list(self._entities):
            try:
                if self._is_entity_valid(entity, current_time):
                    entity_key = entity.vehicle.trip.trip_id
                    # Skip duplicates
                    if entity_key not in active_keys:
                        active_entities.append(entity)
                        active_keys.add(entity_key)
            except Exception:
                # Skip invalid entities
                continue
                
        # Replace with cleaned storage
        self._entities = active_entities
        self._entity_keys = active_keys
                
        # Force garbage collection after cleanup
        gc.collect()
                
    def _get_reusable_message(self) -> gtfs_realtime_pb2.FeedMessage:
        """Get a reusable protobuf message to avoid constant allocation"""
        if self._message_pool:
            msg = self._message_pool.popleft()
            msg.Clear()
            return msg
        else:
            return gtfs_realtime_pb2.FeedMessage()
            
    def _return_message(self, msg: gtfs_realtime_pb2.FeedMessage):
        """Return message to pool for reuse"""
        if len(self._message_pool) < 5:
            msg.Clear()
            self._message_pool.append(msg)
    
    def _is_entity_valid(self, entity, current_time: float) -> bool:
        """Validate entity to prevent stale data"""
        try:
            entity_timestamp = entity.vehicle.timestamp
            # More aggressive staleness check: 10 minutes max vs 20 minutes before
            return (current_time - entity_timestamp) <= 600  
        except Exception:
            return False
    
    def update_entities(self, new_entities: list) -> bytes:
        """Update entities and return serialized feed with bounded storage"""
        current_time = time.time()
        
        # Cleanup if needed
        if current_time - self._last_cleanup >= self._cleanup_interval:
            self._cleanup_stale_entities()
            self._last_cleanup = current_time
        
        # Add new entities with deduplication and size limits
        added_count = 0
        for entity in new_entities:
            try:
                if not self._is_entity_valid(entity, current_time):
                    continue
                    
                entity_key = entity.vehicle.trip.trip_id
                
                # Skip duplicates
                if entity_key in self._entity_keys:
                    continue
                    
                # Add to bounded storage (automatically removes oldest if full)
                if len(self._entities) >= self._max_entities:
                    # Remove oldest entity
                    oldest = self._entities[0] if self._entities else None
                    if oldest:
                        try:
                            old_key = oldest.vehicle.trip.trip_id
                            self._entity_keys.discard(old_key)
                        except Exception:
                            pass
                            
                self._entities.append(entity)
                self._entity_keys.add(entity_key)
                added_count += 1
                
            except Exception as e:
                print(f"[EntityManager] Error processing entity: {e}")
                continue
        
        # Build feed message with reusable objects
        feed_msg = self._get_reusable_message()
        
        try:
            # Build header
            feed_msg.header.gtfs_realtime_version = "2.0"
            feed_msg.header.timestamp = int(current_time)
            
            # Add current entities (bounded to max size) - use extend() but clear first
            feed_msg.entity.extend(list(self._entities)[:self._max_entities])  # Hard limit
            
            # No need for temporary reference cleanup since we're using direct extend
            
            # Use protobuf manager for memory-optimized serialization
            binary_data = protobuf_manager.serialize_with_cleanup(feed_msg)
            
            return binary_data
            
        finally:
            # Return message to pool for reuse
            self._return_message(feed_msg)

# Create global entity manager instance with bounded storage - reduced limits for better memory efficiency      
entity_manager = MemoryEfficientEntityManager(max_entities=25, max_age_seconds=300)

# Keep backward compatibility
entities_by_trip = ThreadSafeDict(max_age_seconds=5*60)  # Reduced from 10 to 5 minutes

def update_feed_message(entities: list):
    """
    Memory-optimized GTFS-RT FeedMessage update using entity manager.
    Maintains same API for backward compatibility.
    """
    # Use the memory-efficient entity manager
    binary = entity_manager.update_entities(entities)
    
    # Update the shared feed message for compatibility with existing code
    with feed_message_lock:
        feed_message.Clear()
        feed_message.header.gtfs_realtime_version = "2.0"
        feed_message.header.timestamp = int(datetime.now().timestamp())
        
        # Add current valid entities (filtered by entity manager)
        current_time = time.time()
        ids = set()
        
        for entity in entities:
            try:
                trip_id = entity.vehicle.trip.trip_id
                if trip_id in ids:  # Prevent duplicates
                    print('Duplicate entity', trip_id)
                    continue
                    
                # More aggressive stale data prevention - 10 minutes vs 20 minutes
                entity_age = current_time - entity.vehicle.timestamp
                if entity_age > 600:  # 10 minutes
                    continue
                    
                ids.add(trip_id)
                
                # Use cleaner logic for entity management
                if trip_id in entities_by_trip:
                    entities_by_trip.pop(trip_id, None)  # More efficient removal
                    
                entities_by_trip[trip_id] = entity
                
            except Exception as e:
                print(f"[FeedUpdater] Error processing entity: {e}")
                continue
        
        # Only add recent entities to feed message (reduce memory footprint)
        recent_entities = [
            entity for entity in entities_by_trip.values()
            if (current_time - entity.vehicle.timestamp) < 300  # 5 minutes max
        ]
        
        # Clear and add entities (protobuf repeated fields don't support slice assignment)
        feed_message.entity.extend(recent_entities)
        
        # Explicit cleanup of temporary references
        recent_entities = None
        del recent_entities
    
    # Use protobuf manager for memory-optimized serialization in broadcast
    binary_optimized = protobuf_manager.serialize_with_cleanup(feed_message)
    
    # Broadcast outside the lock
    queue_gtfs_rt_broadcast(binary_optimized)
    
    # Periodic cleanup to prevent memory buildup
    if len(entities_by_trip) > 100:  # More aggressive cleanup threshold
        print(f"[FeedUpdater] Cleaning up entities_by_trip cache (size: {len(entities_by_trip)})")
        # Force cleanup of old entries
        current_time = time.time()
        stale_keys = []
        
        for trip_id, entity in list(entities_by_trip.items()):
            try:
                if (current_time - entity.vehicle.timestamp) > 300:  # 5 minutes
                    stale_keys.append(trip_id)
            except:
                stale_keys.append(trip_id)  # Remove invalid entities
                
        for key in stale_keys:
            entities_by_trip.pop(key, None)
            
        # Force garbage collection after cleanup
        if stale_keys:
            gc.collect()
