"""
Memory management configuration for different hardware profiles
"""
import os

# Hardware profiles for memory management
HARDWARE_PROFILES = {
    "low_end": {
        "max_memory_mb": 100,
        "max_cache_size": 25,
        "max_concurrent_tasks": 3,
        "max_parallel_workers": 2,
        "chunk_size": 500,
        "cleanup_interval": 300,  # 5 minutes
        "memory_threshold": 0.7,  # 70%
    },
    "medium": {
        "max_memory_mb": 200,
        "max_cache_size": 50,
        "max_concurrent_tasks": 5,
        "max_parallel_workers": 4,
        "chunk_size": 1000,
        "cleanup_interval": 600,  # 10 minutes
        "memory_threshold": 0.8,  # 80%
    },
    "high_end": {
        "max_memory_mb": 500,
        "max_cache_size": 100,
        "max_concurrent_tasks": 10,
        "max_parallel_workers": 8,
        "chunk_size": 2000,
        "cleanup_interval": 1200,  # 20 minutes
        "memory_threshold": 0.85,  # 85%
    }
}

def get_hardware_profile() -> str:
    """Detect hardware profile based on available memory"""
    try:
        import psutil
        memory_gb = psutil.virtual_memory().total / (1024**3)
        
        if memory_gb < 2:
            return "low_end"
        elif memory_gb < 8:
            return "medium"
        else:
            return "high_end"
    except Exception:
        return "medium"  # Default to medium

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
