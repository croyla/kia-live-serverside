"""
Memory management configuration for different hardware profiles
"""
import os

# Hardware profiles for memory management - Updated for better performance
HARDWARE_PROFILES = {
    "low_end": {
        "max_memory_mb": 400,  # Increased to allow full functionality
        "max_cache_size": 50,  # Doubled cache size
        "max_concurrent_tasks": 6,  # Doubled concurrent tasks
        "max_parallel_workers": 4,  # Doubled parallel workers
        "chunk_size": 1000,  # Doubled chunk size for faster processing
        "cleanup_interval": 900,  # Less frequent cleanup (15 minutes)
        "memory_threshold": 0.75,  # 75%
    },
    "medium": {
        "max_memory_mb": 800,  # Increased significantly
        "max_cache_size": 100,  # Doubled cache size
        "max_concurrent_tasks": 10,  # Doubled concurrent tasks
        "max_parallel_workers": 8,  # Doubled parallel workers
        "chunk_size": 2000,  # Doubled chunk size
        "cleanup_interval": 1200,  # 20 minutes
        "memory_threshold": 0.8,  # 80%
    },
    "high_end": {
        "max_memory_mb": 1500,  # Tripled memory allowance
        "max_cache_size": 200,  # Doubled cache size
        "max_concurrent_tasks": 20,  # Doubled concurrent tasks
        "max_parallel_workers": 16,  # Doubled parallel workers
        "chunk_size": 4000,  # Doubled chunk size
        "cleanup_interval": 1800,  # 30 minutes
        "memory_threshold": 0.85,  # 85%
    }
}

def get_hardware_profile() -> str:
    """Detect hardware profile based on available memory"""
    try:
        import psutil
        memory_gb = psutil.virtual_memory().total / (1024**3)
        
        # Adjust thresholds to be less restrictive and prioritize functionality
        if memory_gb < 1.5:
            return "low_end"
        elif memory_gb < 4:
            return "medium"
        else:
            return "high_end"
    except Exception:
        return "high_end"  # Default to high_end to ensure full functionality

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
