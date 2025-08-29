from google.transit import gtfs_realtime_pb2
from datetime import datetime
from src.shared import feed_message, feed_message_lock
from src.web_service import queue_gtfs_rt_broadcast


def update_feed_message(entities: list):
    """
    Overwrites the shared GTFS-RT FeedMessage with new data.
    """
    print('UPDATING FEED MESSAGE')
    with feed_message_lock:
        feed_message.Clear()

        # Build new header
        feed_message.header.gtfs_realtime_version = "2.0"
        feed_message.header.timestamp = int(datetime.now().timestamp())
        ids = set()
        # Add entities
        for entity in entities:
            if entity.id in ids:  # Prevent duplicates
                continue
            if (int(datetime.now().timestamp()) - entity.vehicle.timestamp) > 900:  # Prevent stale data
                continue
            ids.add(entity.id)
            feed_message.entity.append(entity)
        # Serialize once under lock
        binary = feed_message.SerializeToString()
    # Broadcast outside the lock
    queue_gtfs_rt_broadcast(binary)
    # Server Sent Event / Websocket broadcast
