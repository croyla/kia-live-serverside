# Recreating the web_service since execution state was reset
import gc
import os
import asyncio
from aiohttp import web
from old_src.shared import feed_message, feed_message_lock
from contextlib import contextmanager
import weakref

from old_src.shared.config import OUT_DIR

# Keep track of open file handles
open_files = weakref.WeakSet()
MAX_OPEN_FILES = 100

# CORS settings
CORS_SETTINGS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, X-Requested-With, Authorization',
    'Access-Control-Max-Age': '3600',
}

@contextmanager
def managed_file_handle(file_path):
    """Context manager to ensure proper cleanup of file handles"""
    file = None
    try:
        if len(open_files) >= MAX_OPEN_FILES:
            # Close some old files if we've hit the limit
            for old_file in list(open_files)[:MAX_OPEN_FILES//2]:
                try:
                    old_file.close()
                except:
                    pass
                open_files.discard(old_file)
        
        file = open(file_path, 'rb')
        open_files.add(file)
        yield file
    finally:
        if file:
            try:
                file.close()
            finally:
                open_files.discard(file)

def add_cors_headers(response):
    """Add CORS headers to response"""
    for header, value in CORS_SETTINGS.items():
        response.headers[header] = value
    return response

@web.middleware
async def cors_middleware(request, handler):
    """Middleware to handle CORS preflight requests and add CORS headers to all responses"""
    # Handle preflight requests
    if request.method == 'OPTIONS':
        response = web.Response()
        return add_cors_headers(response)
    
    # Handle actual request
    try:
        response = await handler(request)
        return add_cors_headers(response)
    except web.HTTPException as ex:
        # Add CORS headers to HTTP exceptions
        await add_cors_headers(ex)
        raise
    except Exception as e:
        # Add CORS headers to error responses
        response = web.Response(status=500, text=str(e))
        return add_cors_headers(response)

# === Serve GTFS Static zip ===
async def handle_gtfs_zip(request):
    zip_path = f"{OUT_DIR}/gtfs.zip"
    if not os.path.exists(zip_path):
        return web.Response(status=404, text="GTFS ZIP not found.")
    
    return web.FileResponse(
        zip_path,
        headers={'Content-Disposition': 'attachment; filename=gtfs.zip'}
    )

# === Serve GTFS Realtime Feed ===
async def handle_gtfs_realtime(request):
    with feed_message_lock:
        binary = feed_message.SerializeToString()
    
    return web.Response(
        body=binary,
        content_type='application/x-protobuf',
        headers={
            'Cache-Control': 'no-store, no-cache, must-revalidate',
            'Pragma': 'no-cache',
            'Expires': '0'
        }
    )

# === WebSocket broadcasting for GTFS-RT updates ===
# Track websocket clients
ws_clients = weakref.WeakSet()

# Async queue for broadcast messages; initialized on startup with a max size
broadcast_queue = None  # type: asyncio.Queue | None

# Event loop reference for thread-safe publishes
_ws_loop = None  # type: asyncio.AbstractEventLoop | None

async def _broadcast_consumer(app: web.Application):
    """Background task to broadcast queued GTFS-RT binaries to all websocket clients."""
    print("BROADCASTING TO CONSUMER")
    global broadcast_queue
    while True:
        data = await broadcast_queue.get()
        stale = []
        for ws in list(ws_clients):
            try:
                if ws.closed:
                    stale.append(ws)
                    continue
                await ws.send_bytes(data)
            except Exception:
                stale.append(ws)
        for ws in stale:
            try:
                await ws.close()
            except Exception:
                pass
            ws_clients.discard(ws)
        del data
        gc.collect()

async def handle_ws_gtfs_realtime(request: web.Request):
    """WebSocket endpoint that streams GTFS-RT binary frames when updates occur."""
    ws = web.WebSocketResponse(compress=True)
    await ws.prepare(request)
    ws_clients.add(ws)

    # Send current snapshot immediately
    try:
        with feed_message_lock:
            snapshot = feed_message.SerializeToString()
        await ws.send_bytes(snapshot)
    except Exception:
        pass

    async for msg in ws:
        # We don't expect client messages; close on error/close
        if msg.type in (web.WSMsgType.CLOSE, web.WSMsgType.CLOSING, web.WSMsgType.ERROR):
            break

    ws_clients.discard(ws)
    await ws.close()
    gc.collect()
    return ws

def queue_gtfs_rt_broadcast(binary: bytes) -> None:
    """Thread-safe: enqueue a GTFS-RT binary to broadcast to websocket clients.
    Drops oldest messages if queue is full to keep memory bounded.
    """
    global _ws_loop, broadcast_queue
    if not (_ws_loop and broadcast_queue):
        return

    def _enqueue(data: bytes):
        try:
            broadcast_queue.put_nowait(data)
        except asyncio.QueueFull:
            try:
                # Drop oldest and enqueue latest
                broadcast_queue.get_nowait()
                broadcast_queue.put_nowait(data)
            except Exception:
                pass
        finally:
            gc.collect()

    try:
        _ws_loop.call_soon_threadsafe(_enqueue, binary)
    except Exception:
        pass

async def _on_startup(app: web.Application):
    """Initialize broadcaster state and start consumer task."""
    global _ws_loop, broadcast_queue
    _ws_loop = asyncio.get_running_loop()
    broadcast_queue = asyncio.Queue(maxsize=5)
    app['broadcast_task'] = asyncio.create_task(_broadcast_consumer(app))

async def _on_cleanup(app: web.Application):
    """Tear down broadcaster and close websockets."""
    task = app.get('broadcast_task')
    if task:
        task.cancel()
        try:
            await task
        except Exception:
            pass
    for ws in list(ws_clients):
        try:
            await ws.close()
        except Exception:
            pass
        ws_clients.discard(ws)

# === Serve GTFS Version Info ===
async def handle_gtfs_version(request):
    version_file = f"{OUT_DIR}/feed_info.txt"
    if not os.path.exists(version_file):
        return web.json_response({"error": "version file not found"}, status=404)

    with open(version_file, "r") as f:
        version = f.read().strip()
    response = web.Response(text=version)
    response.headers["Cache-Control"] = "no-store"
    return response

app = web.Application(middlewares=[cors_middleware])
app.on_startup.append(_on_startup)
app.on_cleanup.append(_on_cleanup)

# === Routes ===
app.router.add_get("/gtfs.zip", handle_gtfs_zip)
app.router.add_get("/gtfs-rt.proto", handle_gtfs_realtime)
app.router.add_get("/ws/gtfs-rt", handle_ws_gtfs_realtime)
app.router.add_get("/gtfs-version", handle_gtfs_version)
app.router.add_route('OPTIONS', '/{tail:.*}', lambda r: web.Response())

def run_web_service(host="0.0.0.0", port=59966):
    print(f"[web_service] Serving on http://{host}:{port}")
    web.run_app(app, host=host, port=port)
