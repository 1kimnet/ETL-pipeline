# Individual Task Runner Documentation

## Overview

The ETL pipeline provides Windows batch files for running specific phases independently:

- **🚚 load_sde.bat** - Load processed data from staging.gdb to SDE database
- **🔄 geoprocess.bat** - Apply clipping and projection to staging.gdb in-place  
- **📦 stage.bat** - Download sources and build staging.gdb

This is useful for debugging, re-running failed steps, or working with existing processed data.

## Quick Start

### Windows Batch Files

```cmd
# Load existing staging.gdb to SDE
load_sde.bat

# Run geoprocessing on existing staging.gdb
geoprocess.bat

# Download and stage data only
stage.bat

# Stage with force download option
stage.bat --force-download

# Stage without resetting GDB
stage.bat --no-reset-gdb
```

## Task Details

### 🚚 SDE Loading Task (`load_sde.bat`)

**Purpose**: Load processed feature classes from staging.gdb to an SDE database.

**Requirements**:
- `data\staging.gdb` must exist with feature classes
- SDE connection file must exist and be accessible
- SDE datasets must already exist (run `python scripts\create_sde_datasets.py`)

**Usage**:
```cmd
load_sde.bat
```

**Configuration**: Uses settings from `config\config.yaml`:
- `sde_connection_file`
- `sde_load_strategy` 
- `sde_dataset_pattern`
- Mapping configuration from `config\mappings.yaml` (if exists)

---

### 🔄 Geoprocessing Task (`geoprocess.bat`)

**Purpose**: Apply spatial operations (clip + project) to feature classes in staging.gdb.

**Requirements**:
- `data\staging.gdb` must exist with feature classes
- AOI boundary file must exist (configured in config.yaml)

**Usage**:
```cmd
geoprocess.bat
```

**Configuration**: Uses `geoprocessing` settings from `config\config.yaml`:
- `aoi_boundary` path
- `target_srid`
- `parallel_processing_factor`

---

### 📦 Staging Task (`stage.bat`)

**Purpose**: Download source data and build staging.gdb from configured sources.

**Requirements**:
- `config\sources.yaml` must exist with enabled sources
- Network access to download URLs
- Write permissions to data directories

**Usage**:
```cmd
# Standard staging
stage.bat

# Force re-download all sources
stage.bat --force-download

# Don't reset staging.gdb (append mode)
stage.bat --no-reset-gdb
```

**Configuration**: Uses source definitions from `config\sources.yaml` and handler settings.

## Common Workflows

### 1. Debug SDE Loading Issues
```cmd
# Run full pipeline once to get staging.gdb
python run_etl.py

# Then iterate on SDE loading only
load_sde.bat
# Fix configuration issues, then run again
load_sde.bat
```

### 2. Test Different Geoprocessing Parameters
```cmd
# Stage data once
stage.bat

# Modify geoprocessing settings in config\config.yaml
# Then apply geoprocessing
geoprocess.bat

# Load to SDE
load_sde.bat
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

### 4. Incremental Updates
```cmd
# Add new data to existing staging.gdb
stage.bat --no-reset-gdb

# Process the updated GDB
geoprocess.bat
load_sde.bat
```

## Implementation Details

The batch files are simple wrappers that call the main ETL pipeline with specific phases:

### How It Works
- Each batch file sets environment variables or flags
- Calls the main `run_etl.py` with specific parameters
- Uses the same configuration files and logging system
- Maintains full compatibility with existing pipeline code

### Limitations
- Batch files use fixed paths and default configuration
- Advanced parameter customization requires modifying config files
- No command-line parameter passing for custom GDB paths or connections

## Error Handling

All batch files include error handling and return appropriate exit codes:

- **Success**: Returns exit code 0
- **Failure**: Returns non-zero exit code and displays error message
- **Logging**: All output is logged to standard ETL log files

Example error output:
```cmd
C:\Git\ETL-pipeline> load_sde.bat
🚚 Running SDE loading task...
❌ SDE loading failed with error code 1
```

## Troubleshooting

### Common Issues

**"staging.gdb not found"**
- Run `stage.bat` first to create the staging database
- Verify `data\staging.gdb` exists

**"SDE connection failed"**  
- Check `config\config.yaml` for correct `sde_connection_file` path
- Verify SDE connection file exists and is accessible
- Test connection using ArcGIS Catalog

**"AOI boundary not found"**
- Check `config\config.yaml` for correct `aoi_boundary` path
- Verify boundary file exists and is a valid shapefile

**"Permission denied"**
- Ensure write permissions to data directories
- Close ArcGIS applications that might lock the GDB
- Run Command Prompt as administrator if necessary

### Configuration Check

Before running tasks, verify your configuration:
```cmd
# Check if config files exist
dir config\*.yaml

# Check if required data exists
dir data\connections\*.sde
dir data\connections\*.shp
```
