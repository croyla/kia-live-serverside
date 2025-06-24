# Recreating the web_service since execution state was reset

import os
from aiohttp import web
from src.shared import feed_message, feed_message_lock
from contextlib import contextmanager
import weakref

from src.shared.config import OUT_DIR

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
        add_cors_headers(ex)
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

# === Routes ===
app.router.add_get("/gtfs.zip", handle_gtfs_zip)
app.router.add_get("/gtfs-rt.proto", handle_gtfs_realtime)
app.router.add_get("/gtfs-version", handle_gtfs_version)
app.router.add_route('OPTIONS', '/{tail:.*}', lambda r: web.Response())

def run_web_service(host="0.0.0.0", port=59966):
    print(f"[web_service] Serving on http://{host}:{port}")
    web.run_app(app, host=host, port=port)
