# Individual Task Runner Documentation

## Overview

The ETL pipeline now supports running individual tasks independently, allowing you to:

- **🚚 Load to SDE only** - Load processed data from staging.gdb to SDE database
- **🔄 Geoprocess only** - Apply clipping and projection to staging.gdb in-place  
- **📦 Stage only** - Download sources and build staging.gdb

This is useful for debugging, re-running failed steps, or working with existing processed data.

## Quick Start

### Windows Batch Files (Recommended)

```cmd
# Load existing staging.gdb to SDE
load_sde.bat

# Run geoprocessing on existing staging.gdb
geoprocess.bat

# Download and stage data only
stage.bat
```

### Python Script (Advanced)

```cmd
# Load to SDE with custom parameters
python scripts\run_task.py sde --source-gdb "path\to\custom.gdb" --sde-connection "path\to\custom.sde"

# Geoprocess with custom AOI and SRID
python scripts\run_task.py geoprocess --aoi-boundary "path\to\boundary.shp" --target-srid 4326

# Stage with forced download
python scripts\run_task.py stage --force-download --no-reset-gdb
```

## Task Details

### 🚚 SDE Loading Task

**Purpose**: Load processed feature classes from a staging GDB to an SDE database.

**Requirements**:
- Source GDB must exist (usually `data\staging.gdb`)
- SDE connection file must exist and be accessible
- SDE datasets must already exist (created separately)

**Usage**:
```cmd
# Using batch file (simplest)
load_sde.bat

# Using Python script with defaults
python scripts\run_task.py sde

# With custom parameters
python scripts\run_task.py sde --source-gdb "data\custom.gdb" --sde-connection "data\connections\test.sde"
```

**Configuration**: Uses `sde_load_strategy`, `parallel_sde_loading`, and mapping settings from `config\config.yaml`.

---

### 🔄 Geoprocessing Task

**Purpose**: Apply spatial operations (clip + project) to feature classes in staging.gdb.

**Requirements**:
- Source GDB must exist with feature classes to process
- AOI boundary file must exist for clipping
- Target SRID must be valid

**Usage**:
```cmd
# Using batch file (simplest)
geoprocess.bat

# Using Python script with defaults
python scripts\run_task.py geoprocess

# With custom parameters
python scripts\run_task.py geoprocess --aoi-boundary "data\custom_boundary.shp" --target-srid 4326
```

**Configuration**: Uses `geoprocessing` settings from `config\config.yaml`.

---

### 📦 Staging Task

**Purpose**: Download source data and build staging.gdb from configured sources.

**Requirements**:
- `config\sources.yaml` must exist with enabled sources
- Network access to download URLs
- Write permissions to data directories

**Usage**:
```cmd
# Using batch file (simplest)
stage.bat

# With forced download (re-download everything)
stage.bat --force-download

# Without resetting GDB (append mode)
stage.bat --no-reset-gdb

# Using Python script
python scripts\run_task.py stage --force-download
```

**Configuration**: Uses handler settings and source definitions from YAML files.

## Common Workflows

### 1. Debug SDE Loading Issues
```cmd
# Run full pipeline once to get staging.gdb
python run_etl.py

# Then iterate on SDE loading only
load_sde.bat
# Fix issues, then run again
load_sde.bat
```

### 2. Test Different Geoprocessing Parameters
```cmd
# Stage data once
stage.bat

# Try different target projections
python scripts\run_task.py geoprocess --target-srid 4326
python scripts\run_task.py geoprocess --target-srid 3006
python scripts\run_task.py geoprocess --target-srid 2154
```

### 3. Refresh Data Without Full Pipeline
```cmd
# Re-download and stage new data
stage.bat --force-download

# Apply same geoprocessing
geoprocess.bat

# Load to SDE
load_sde.bat
```

### 4. Work with Custom Data Sources
```cmd
# Create custom staging.gdb from external sources
# (manual process)

# Then use task runner to geoprocess and load
python scripts\run_task.py geoprocess --source-gdb "data\custom.gdb"
python scripts\run_task.py sde --source-gdb "data\custom.gdb"
```

## Error Handling

All tasks include comprehensive error handling and logging:

- **Success**: Returns exit code 0
- **Failure**: Returns non-zero exit code and logs detailed error information
- **Summary**: Always generates a summary report at completion

Use `--log-level DEBUG` for detailed troubleshooting:
```cmd
python scripts\run_task.py sde --log-level DEBUG
```

## Integration with Existing Code

The task runner reuses all existing pipeline components:
- Same configuration files (`config\config.yaml`, `config\mappings.yaml`)
- Same handlers and loaders
- Same monitoring and logging systems
- Same parallel processing capabilities

This ensures consistency and reduces code duplication.

## Performance Considerations

### Parallel Processing
- SDE loading uses parallel processing by default (configurable)
- Geoprocessing respects existing parallelization settings
- Staging downloads sources sequentially (as in main pipeline)

### Resource Usage
- Each task can be configured independently
- Memory usage scales with GDB size and parallel worker count
- Network usage only occurs during staging task

### Monitoring
- All tasks include performance monitoring
- Metrics are collected and logged
- Summary reports show timing and success rates

## Troubleshooting

### Common Issues

**"GDB not found"**
- Ensure the source GDB exists at the specified path
- Run staging task first if needed

**"SDE connection failed"**  
- Verify SDE connection file exists and is accessible
- Test connection independently using ArcGIS tools
- Check network connectivity and permissions

**"AOI boundary not found"**
- Ensure boundary file exists at specified path
- Verify file format is supported (shapefile, feature class)

**"Permission denied"**
- Ensure write permissions to output directories
- Close any applications that might lock the GDB
- Run as administrator if necessary

### Getting Help

```cmd
# Show all available tasks
python scripts\run_task.py --help

# Show task-specific options
python scripts\run_task.py sde --help
python scripts\run_task.py geoprocess --help
python scripts\run_task.py stage --help
```
