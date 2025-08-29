# Memory Optimization Guide

This document describes the memory optimization features implemented for running the transit system on lower-end hardware like VPS environments.

## Overview

The system has been optimized to:
- Automatically detect hardware capabilities
- Limit memory usage based on available resources
- Use streaming and chunked processing for large files
- Implement intelligent caching with memory bounds
- Monitor performance and provide optimization recommendations

## Hardware Profiles

The system automatically detects your hardware and applies appropriate memory limits:

### Low-End (≤2GB RAM)
- **Max Memory**: 100MB
- **Cache Size**: 25 entries
- **Concurrent Tasks**: 3
- **Chunk Size**: 500 records
- **Cleanup Interval**: 5 minutes

### Medium (2-8GB RAM)
- **Max Memory**: 200MB
- **Cache Size**: 50 entries
- **Concurrent Tasks**: 5
- **Chunk Size**: 1000 records
- **Cleanup Interval**: 10 minutes

### High-End (>8GB RAM)
- **Max Memory**: 500MB
- **Cache Size**: 100 entries
- **Concurrent Tasks**: 10
- **Chunk Size**: 2000 records
- **Cleanup Interval**: 20 minutes

## Environment Variables

You can override automatic detection with environment variables:

```bash
# Memory limits
export KIA_MAX_MEMORY_MB=150          # Set max memory to 150MB
export KIA_MAX_CACHE_SIZE=40          # Set max cache size to 40 entries
export KIA_MAX_CONCURRENT_TASKS=4     # Set max concurrent tasks to 4
export KIA_MAX_PARALLEL_WORKERS=6     # Set max parallel workers to 6

# Processing settings
export KIA_CHUNK_SIZE=800             # Set chunk size to 800 records
export KIA_CLEANUP_INTERVAL=900       # Set cleanup interval to 15 minutes
export KIA_MEMORY_THRESHOLD=0.75      # Set memory threshold to 75%

# Network settings
export KIA_FETCH_TIMEOUT=180          # Set fetch timeout to 3 minutes
export KIA_MAX_CONNECTIONS=6          # Set max connections to 6
export KIA_MAX_PROCESSING_CONCURRENT=4 # Set max processing tasks to 4
```

## Memory Management Features

### 1. Streaming JSON Loading
- Large JSON files (>10MB) are loaded in chunks
- Reduces memory spikes during data loading
- Uses `ijson` library for efficient streaming

### 2. Chunked GTFS Processing
- GTFS data is processed in configurable chunks
- Memory usage is monitored during processing
- Automatic cleanup when memory limits are reached

### 3. Intelligent Caching
- `ThreadSafeDict` with memory and size limits
- Automatic cleanup based on age and memory pressure
- System memory monitoring triggers cleanup

### 4. Trip Map Cache
- Dedicated cache for route trip mappings
- Memory-bounded with automatic cleanup
- Performance monitoring and statistics

### 5. Parallel Processing
- **Entity Building**: Multiple vehicles processed simultaneously using ThreadPoolExecutor
- **Hardware-Aware**: Worker count automatically scaled based on available CPU cores
- **Memory Efficient**: Parallel processing with bounded memory usage
- **Error Handling**: Individual vehicle failures don't affect others

## Performance Monitoring

The system includes built-in performance monitoring:

### Automatic Monitoring
- Memory usage tracking
- CPU usage monitoring
- Performance issue detection
- Optimization recommendations

### Manual Monitoring
```python
from src.shared.monitor import get_performance_summary

# Get current performance status
summary = get_performance_summary()
print(f"Health: {summary['health']}")
print(f"Memory: {summary['averages']['memory_mb']}MB")
print(f"CPU: {summary['averages']['cpu_percent']}%")
```

## Optimization Recommendations

### For VPS Environments (1-2GB RAM)
1. **Use low-end profile**: Automatically detected for ≤2GB RAM
2. **Reduce concurrent tasks**: Set `KIA_MAX_CONCURRENT_TASKS=2`
3. **Optimize parallel workers**: Set `KIA_MAX_PARALLEL_WORKERS=2`
4. **Smaller chunks**: Set `KIA_CHUNK_SIZE=300`
5. **Frequent cleanup**: Set `KIA_CLEANUP_INTERVAL=300`

### For Medium VPS (2-4GB RAM)
1. **Medium profile**: Automatically detected for 2-8GB RAM
2. **Balanced settings**: Use default medium profile settings
3. **Parallel processing**: `KIA_MAX_PARALLEL_WORKERS=4` for optimal performance
4. **Monitor performance**: Check performance summary regularly

### For High-End VPS (4GB+ RAM)
1. **High-end profile**: Automatically detected for >8GB RAM
2. **Increase limits**: Can handle larger caches and more concurrent tasks
3. **Maximize parallelization**: `KIA_MAX_PARALLEL_WORKERS=8` for best throughput
4. **Optimize for throughput**: Larger chunk sizes and longer cleanup intervals

## Parallel Processing Benefits

### Performance Improvements
- **Entity Building**: 2-8x faster processing of multiple vehicles
- **CPU Utilization**: Better use of multi-core processors
- **Scalability**: Performance scales with available CPU cores
- **Responsiveness**: Faster GTFS-RT feed updates

### Memory Efficiency
- **Bounded Memory**: Parallel processing with memory limits
- **No Memory Leaks**: Proper cleanup after parallel operations
- **Hardware Scaling**: Worker count adapts to available resources
- **Resource Monitoring**: Memory usage tracked during parallel operations

### Configuration Examples

#### Low-End VPS (1GB RAM)
```bash
export KIA_MAX_PARALLEL_WORKERS=2     # Conservative parallelization
export KIA_MAX_MEMORY_MB=80           # Low memory limit
export KIA_MAX_CONCURRENT_TASKS=2     # Few concurrent tasks
```

#### Standard VPS (2GB RAM)
```bash
export KIA_MAX_PARALLEL_WORKERS=4     # Balanced parallelization
export KIA_MAX_MEMORY_MB=150          # Medium memory limit
export KIA_MAX_CONCURRENT_TASKS=3     # Moderate concurrent tasks
```

#### Performance VPS (4GB+ RAM)
```bash
export KIA_MAX_PARALLEL_WORKERS=8     # Maximum parallelization
export KIA_MAX_MEMORY_MB=300          # High memory limit
export KIA_MAX_CONCURRENT_TASKS=6     # High concurrent tasks
```

## Troubleshooting

### High Memory Usage
1. Check current memory usage: `get_performance_summary()`
2. Reduce cache sizes: `export KIA_MAX_CACHE_SIZE=20`
3. Reduce concurrent tasks: `export KIA_MAX_CONCURRENT_TASKS=2`
4. Force cleanup: Restart the service

### Performance Issues
1. Monitor CPU usage in performance summary
2. Reduce concurrent processing: `export KIA_MAX_PROCESSING_CONCURRENT=2`
3. Increase timeouts for slower hardware: `export KIA_FETCH_TIMEOUT=300`

### Memory Leaks
1. Check for growing cache sizes in logs
2. Monitor memory trends in performance summary
3. Restart service if memory usage grows continuously

## Best Practices

### 1. Start Conservative
- Begin with automatic hardware detection
- Monitor performance for first few hours
- Adjust settings based on actual usage patterns

### 2. Monitor Regularly
- Check performance summary every few hours
- Watch for memory usage trends
- Address performance issues promptly

### 3. Test Changes
- Test configuration changes in staging first
- Monitor impact of changes on performance
- Revert if performance degrades

### 4. Resource Planning
- Plan for peak usage times
- Consider seasonal variations in transit data
- Monitor disk space for GTFS files

## Example Configurations

### Minimal VPS (1GB RAM)
```bash
export KIA_MAX_MEMORY_MB=80
export KIA_MAX_CACHE_SIZE=20
export KIA_MAX_CONCURRENT_TASKS=2
export KIA_CHUNK_SIZE=300
export KIA_CLEANUP_INTERVAL=300
export KIA_MEMORY_THRESHOLD=0.6
```

### Standard VPS (2GB RAM)
```bash
export KIA_MAX_MEMORY_MB=150
export KIA_MAX_CACHE_SIZE=35
export KIA_MAX_CONCURRENT_TASKS=3
export KIA_CHUNK_SIZE=600
export KIA_CLEANUP_INTERVAL=600
export KIA_MEMORY_THRESHOLD=0.7
```

### Performance VPS (4GB RAM)
```bash
export KIA_MAX_MEMORY_MB=300
export KIA_MAX_CACHE_SIZE=60
export KIA_MAX_CONCURRENT_TASKS=6
export KIA_CHUNK_SIZE=1200
export KIA_CLEANUP_INTERVAL=900
export KIA_MEMORY_THRESHOLD=0.8
```

## Monitoring Commands

### Check Current Status
```bash
# View performance summary
python -c "from src.shared.monitor import get_performance_summary; import json; print(json.dumps(get_performance_summary(), indent=2, default=str))"

# Check memory usage
python -c "import psutil; print(f'Memory: {psutil.virtual_memory().percent:.1f}%')"
```

### Log Analysis
```bash
# Monitor memory cleanup logs
grep "Memory limit reached" logs/app.log

# Check cache statistics
grep "Cache.*Size.*Memory" logs/app.log

# Monitor performance issues
grep "Performance issues detected" logs/app.log
```

## Support

If you encounter memory or performance issues:

1. Check the performance summary first
2. Review the optimization recommendations
3. Adjust environment variables based on your hardware
4. Monitor the impact of changes
5. Check logs for specific error messages

The system is designed to be self-healing and will automatically adjust to your hardware capabilities while maintaining optimal performance.
