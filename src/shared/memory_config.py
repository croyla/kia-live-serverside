"""
Memory management configuration for different hardware profiles
"""
import os

# Hardware profiles for memory management - Optimized for cheap VPS deployment
HARDWARE_PROFILES = {
    "ultra_low": {
        "max_memory_mb": 200,  # Ultra-low memory for 512MB VPS
        "max_cache_size": 20,  # Minimal cache
        "max_concurrent_tasks": 2,  # Very limited concurrency
        "max_parallel_workers": 1,  # Single worker to avoid memory pressure
        "chunk_size": 250,  # Small chunks
        "cleanup_interval": 300,  # Frequent cleanup (5 minutes)
        "memory_threshold": 0.6,  # Aggressive cleanup at 60%
        "stream_threshold_mb": 2,  # Stream files > 2MB
        "max_file_load_mb": 10,  # Max file size to load at once
    },
    "low_end": {
        "max_memory_mb": 300,  # Reduced for 500MB-1GB VPS  
        "max_cache_size": 30,  # Smaller cache
        "max_concurrent_tasks": 3,  # Limited concurrent tasks
        "max_parallel_workers": 2,  # Fewer parallel workers
        "chunk_size": 500,  # Smaller chunk size
        "cleanup_interval": 600,  # More frequent cleanup (10 minutes)
        "memory_threshold": 0.7,  # 70%
        "stream_threshold_mb": 3,  # Stream files > 3MB
        "max_file_load_mb": 15,  # Max file size to load
    },
    "medium": {
        "max_memory_mb": 600,  # Reduced from 800MB
        "max_cache_size": 60,  # Reduced cache size
        "max_concurrent_tasks": 6,  # Reduced concurrent tasks
        "max_parallel_workers": 4,  # Reduced parallel workers
        "chunk_size": 1000,  # Smaller chunk size
        "cleanup_interval": 900,  # 15 minutes
        "memory_threshold": 0.75,  # 75%
        "stream_threshold_mb": 5,  # Stream files > 5MB
        "max_file_load_mb": 25,  # Max file size to load
    },
    "high_end": {
        "max_memory_mb": 1200,  # Reduced from 1500MB
        "max_cache_size": 120,  # Reduced cache size
        "max_concurrent_tasks": 12,  # Reduced concurrent tasks
        "max_parallel_workers": 8,  # Reduced parallel workers
        "chunk_size": 2000,  # Moderate chunk size
        "cleanup_interval": 1200,  # 20 minutes
        "memory_threshold": 0.8,  # 80%
        "stream_threshold_mb": 8,  # Stream files > 8MB
        "max_file_load_mb": 50,  # Max file size to load
    }
}

def get_hardware_profile() -> str:
    """Detect hardware profile based on available memory - optimized for cheap VPS"""
    try:
        import psutil
        memory_gb = psutil.virtual_memory().total / (1024**3)
        
        # Enhanced detection for ultra-low-memory systems
        if memory_gb < 0.7:  # 700MB or less - ultra-low memory VPS
            return "ultra_low"
        elif memory_gb < 1.2:  # 1.2GB or less - cheap VPS
            return "low_end"
        elif memory_gb < 3.0:  # 3GB or less - budget VPS
            return "medium"
        else:
            return "high_end"
    except Exception:
        return "low_end"  # Default to low_end for safety on unknown systems

def get_memory_config(profile: str = None) -> dict:
    """Get memory configuration for specified or detected hardware profile"""
    if profile is None:
        profile = get_hardware_profile()
    
    # Environment variable overrides
    config = HARDWARE_PROFILES[profile].copy()
    
    # Allow environment variable overrides
    config["max_memory_mb"] = int(os.getenv("KIA_MAX_MEMORY_MB", config["max_memory_mb"]))
    config["max_cache_size"] = int(os.getenv("KIA_MAX_CACHE_SIZE", config["max_cache_size"]))
    config["max_concurrent_tasks"] = int(os.getenv("KIA_MAX_CONCURRENT_TASKS", config["max_concurrent_tasks"]))
    config["chunk_size"] = int(os.getenv("KIA_CHUNK_SIZE", config["chunk_size"]))
    config["cleanup_interval"] = int(os.getenv("KIA_CLEANUP_INTERVAL", config["cleanup_interval"]))
    config["memory_threshold"] = float(os.getenv("KIA_MEMORY_THRESHOLD", config["memory_threshold"]))
    
    return config

# Default configuration
DEFAULT_CONFIG = get_memory_config()

# Memory limits for specific components
COMPONENT_LIMITS = {
    "routes_children": {"max_size": 64, "max_memory_mb": 10},
    "routes_parent": {"max_size": 64, "max_memory_mb": 10},
    "start_times": {"max_size": 1000, "max_memory_mb": 20},
    "times": {"max_size": 1000, "max_memory_mb": 20},
    "stop_times": {"max_size": 1000, "max_memory_mb": 30},
    "route_stops": {"max_size": 64, "max_memory_mb": 15},
    "prediction_cache": {"max_size": 200, "max_memory_mb": 25},
    "trip_map_cache": {"max_size": 50, "max_memory_mb": 25},
}

def get_component_config(component: str) -> dict:
    """Get configuration for a specific component"""
    base_config = COMPONENT_LIMITS.get(component, {"max_size": 100, "max_memory_mb": 20})
    
    # Scale based on hardware profile
    profile_config = get_memory_config()
    scale_factor = profile_config["max_memory_mb"] / 200  # Scale relative to medium profile
    
    return {
        "max_size": int(base_config["max_size"] * scale_factor),
        "max_memory_mb": int(base_config["max_memory_mb"] * scale_factor),
        "max_age_seconds": 24 * 3600  # 24 hours default
    }
