# ETL Pipeline - Swedish Geographic Data Processing System

## 🚀 Project Overview

This is a Python-based ETL (Extract, Transform, Load) pipeline designed for automated geographic data processing from Swedish national and regional authorities. The system downloads geospatial data from various sources, processes it through ArcGIS, and loads it into an SDE database for municipal use.

## ✨ Key Features

### 🏗️ **Core Architecture**
- **Modular Design**: Clearly separated handlers for different data source types
- **Configuration-Driven**: Fully configurable via YAML files 
- **ArcGIS Integration**: Built on ArcPy for geospatial processing and SDE connectivity
- **Handler System**: Extensible handlers for file, REST API, OGC API, and Atom feed sources

### 🛡️ **Reliability & Resilience**
- **Robust Error Handling**: Comprehensive exception hierarchy with descriptive error messages
- **Retry Logic**: Configurable retry mechanisms for failed operations
- **Continue on Failure**: Option to process remaining sources when individual sources fail
- **Data Validation**: Built-in validation for downloads and processing steps

### ⚡ **Performance Features**
- **Parallel Processing**: Multi-threaded downloading and processing capabilities
- **Connection Pooling**: Efficient HTTP session management for API calls
- **Memory Management**: Configurable memory limits and chunked processing
- **Performance Monitoring**: Built-in timing and metrics collection

### 🔧 **Configuration Management**
- **YAML Configuration**: Easy-to-maintain configuration files for sources and settings
- **Environment Support**: Development, staging, and production environment configurations
- **Environment Variables**: Override configuration via environment variables
- **Flexible Source Definitions**: Support for various data source types and formats

### 📊 **Monitoring & Logging**
- **Structured Logging**: Comprehensive logging with summary and debug outputs
- **Run Summaries**: Human-readable emoji-based summaries of pipeline runs
- **Performance Metrics**: Built-in metrics collection and timing analysis
- **Health Monitoring**: System health checks and monitoring capabilities

### ✅ **Data Processing**
- **Multi-format Support**: Handles ZIP files, shapefiles, GeoPackages, and API responses
- **Geoprocessing**: In-place clipping and projection to municipality boundaries
- **SDE Loading**: Automated loading to enterprise geodatabase with dataset organization
- **Data Validation**: Geometry and attribute validation during processing

## 📁 Project Structure

```
ETL-pipeline/
├── etl/                     # Core ETL framework
│   ├── handlers/            # Data source handlers
│   │   ├── rest_api.py     # REST API data extraction
│   │   ├── file.py         # File download handling
│   │   ├── ogc_api.py      # OGC API support
│   │   ├── atom_feed.py    # Atom feed processing
│   │   └── geoprocess.py   # Geoprocessing operations
│   ├── loaders/             # Data output loaders
│   │   ├── filegdb.py      # File Geodatabase loader
│   │   ├── geojson_loader.py    # GeoJSON export
│   │   ├── gpkg_loader.py       # GeoPackage support
│   │   └── shapefile_loader.py  # Shapefile export
│   ├── utils/               # Utility modules
│   │   ├── cleanup.py      # Cleanup operations
│   │   ├── http.py         # HTTP utilities
│   │   ├── logging_cfg.py  # Logging configuration
│   │   ├── naming.py       # Name sanitization
│   │   ├── paths.py        # Path management
│   │   ├── performance.py  # Performance monitoring
│   │   ├── retry.py        # Retry mechanisms
│   │   └── run_summary.py  # Run summary generation
│   ├── config.py           # Configuration management
│   ├── exceptions.py       # Exception hierarchy
│   ├── monitoring.py       # Monitoring and metrics
│   ├── models.py           # Data models
│   └── pipeline.py         # Main pipeline orchestrator
├── config/                  # Configuration files
│   ├── config.yaml         # Main configuration
│   ├── sources.yaml        # Data source definitions
│   ├── mappings.yaml       # Field mappings (optional)
│   └── environment.yaml    # Environment-specific settings
├── data/                    # Data directories
│   ├── connections/        # SDE and boundary files
│   ├── downloads/          # Downloaded source data
│   ├── staging/            # Temporary processing files
│   └── staging.gdb/        # Staging geodatabase
├── tests/                   # Test suite
│   ├── unit/               # Unit tests
│   ├── integration/        # Integration tests
│   ├── e2e/                # End-to-end tests
│   ├── fixtures/           # Test data and configurations
│   └── test_runner.py      # Test runner script
├── scripts/                 # Utility scripts
│   ├── cleanup_downloads.py    # Download cleanup
│   ├── create_sde_datasets.py  # SDE dataset creation
│   └── list_tree.py            # Project structure listing
├── docs/                    # Documentation
├── logs/                    # Log files
└── run_etl.py              # Main entry point
```

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- ArcGIS Pro 3.3+ or ArcGIS Server 11.3+
- Access to production SDE database
- Municipality boundary shapefile

### Running the Pipeline
```bash
# Run with default configuration
python run_etl.py

# Run with custom sources and config files
python run_etl.py custom_sources.yaml custom_config.yaml

# Run with custom sources, config, and mappings
python run_etl.py sources.yaml config.yaml mappings.yaml
```

### Environment Configuration
Set environment variables to override configuration:
```bash
# Development environment
set ETL_ENVIRONMENT=development
set ETL_LOG_LEVEL=DEBUG

# Production environment
set ETL_ENVIRONMENT=production
set ETL_LOG_LEVEL=INFO
```

### Basic Pipeline Workflow
1. **Download Phase**: Downloads data from configured sources to `data/downloads/`
2. **Staging Phase**: Processes downloaded data into staging.gdb
3. **Geoprocessing Phase**: Clips and projects data to municipality boundary
4. **SDE Loading Phase**: Loads processed data to production SDE database

## 🧪 Testing

### Run All Tests
```bash
# Using the test runner
python tests/test_runner.py

# Using pytest directly
python -m pytest tests/
```

### Specific Test Types
```bash
python tests/test_runner.py unit         # Unit tests only
python tests/test_runner.py integration  # Integration tests only  
python tests/test_runner.py e2e          # End-to-end tests only
```

### Test Configuration
The test runner automatically detects and includes coverage reporting when available. Test structure includes:
- **Unit tests**: Individual component testing
- **Integration tests**: Handler and loader integration testing
- **End-to-end tests**: Complete pipeline workflow testing
- **Fixtures**: Shared test data and mock configurations

## 🔧 Configuration

### Main Configuration (`config/config.yaml`)
```yaml
# Environment settings
environment: "development"  # Options: development, staging, production
debug: false

# Cleanup settings for scheduled runs
cleanup_downloads_before_run: true
cleanup_staging_before_run: true

# Logging configuration
logging:
  level: "INFO"
  console_level: "INFO"
  summary_file: "etl_summary.log"
  debug_file: "etl_debug.log"

# Processing configuration  
processing:
  parallel_workers: 2
  memory_limit_mb: 1024
  chunk_size: 1000

# SDE loading settings
sde_connection_file: "data/connections/prod.sde"
sde_schema: "GNG"
sde_dataset_pattern: "Underlag_{authority}"
sde_load_strategy: "truncate_and_load"

# Geoprocessing settings
geoprocessing:
  enabled: true
  aoi_boundary: "data/connections/municipality_boundary.shp"
  target_srid: 3010
  parallel_processing_factor: "100"
```

### Source Configuration (`config/sources.yaml`)
```yaml
sources:
  - name: "Naturvårdsverket - Naturvårdsregistret"
    authority: "NVV"
    type: "file"
    url: "https://geodata.naturvardsverket.se/nedladdning/naturvardsregistret/"
    enabled: true
    download_format: "zip"
    staged_data_type: "shapefile_collection"
    include:
      - "naturvardsregistret.shp"
  
  - name: "Länsstyrelsen REST API"
    authority: "LST"
    type: "rest_api"
    url: "https://ext-geodata.lansstyrelsen.se/arcgis/rest/services/"
    enabled: true
    raw:
      layer_ids: [0, 1, 2]
      bbox_coords: "16.5,59.0,17.6,59.6"
```

## 📊 Monitoring & Logging

### Run Summaries
The pipeline generates emoji-based summaries for easy monitoring:
```
🏁 ETL Pipeline Summary
📥 Downloads: 12 done, 1 error, 0 skipped
📦 Staging: 11 loaded to staging.gdb
🚚 SDE: 11 loaded to production database
⏱️  Total runtime: 5.2 minutes
```

### Structured Logging
The system provides comprehensive logging with multiple levels:
- **Summary logs**: High-level progress with emojis for readability
- **Debug logs**: Detailed technical information for troubleshooting
- **Console output**: Real-time progress indicators

### Log Files
- `etl_summary.log`: Human-readable pipeline progress
- `etl_debug.log`: Detailed technical logs
- Console output shows real-time progress with emoji indicators

### Performance Monitoring
Built-in monitoring includes:
- Download timing and success rates
- Processing performance metrics  
- Memory usage tracking
- Error rate monitoring

## 🔌 Handler System

The pipeline uses a modular handler system for different data source types:

### File Handler (`file.py`)
Downloads and extracts files from URLs:
```python
# Handles sources of type "file"
# Supports ZIP, GeoPackage, Shapefile downloads
# Automatic extraction and staging
```

### REST API Handler (`rest_api.py`)
Processes ArcGIS REST services:
```python
# Handles sources of type "rest_api"
# Layer-by-layer processing
# BBOX filtering support
# Multiple output geometry types (point, polyline, polygon)
```

### OGC API Handler (`ogc_api.py`)
Connects to OGC API-Features services:
```python
# Handles sources of type "ogc_api"
# Standards-compliant API access
# Collection-based data retrieval
```

### Atom Feed Handler (`atom_feed.py`)
Processes XML Atom feeds:
```python
# Handles sources of type "atom_feed"
# Feed parsing and link extraction
# Automatic file download from feed entries
```

## 🛡️ Error Handling

The system includes comprehensive error handling:

### Exception Hierarchy
```python
from etl.exceptions import SourceUnavailableError, NetworkError

try:
    # ETL operation
    handler.fetch_data(source)
except SourceUnavailableError as e:
    # Handle temporary service issues
    logger.warning(f"Source temporarily unavailable: {e}")
except NetworkError as e:
    # Handle network connectivity issues
    logger.error(f"Network error: {e}")
```

### Error Recovery
- Configurable retry attempts for failed downloads
- Continue-on-failure option to process remaining sources
- Detailed error logging with context information
- Graceful degradation when optional services are unavailable

### Common Error Scenarios
- **Source Unavailable**: Temporary service outages
- **Network Timeouts**: Connection issues
- **File Format Errors**: Unsupported or corrupted data
- **SDE Connection Issues**: Database connectivity problems
- **Geoprocessing Failures**: Spatial operation errors

## 🏗️ Data Processing Workflow

### 1. Download Phase
- Downloads data from configured sources to `data/downloads/`
- Supports multiple formats: ZIP, GeoPackage, Shapefile, API responses
- Validates downloads and extracts compressed files
- Handles authentication and custom headers

### 2. Staging Phase  
- Loads downloaded data into `staging.gdb`
- Standardizes feature class naming using authority prefixes
- Validates geometry and attributes
- Organizes data by source and geometry type

### 3. Geoprocessing Phase
- Clips features to municipality boundary
- Projects to target coordinate system (SWEREF99 TM)
- Performs in-place processing to optimize performance
- Validates spatial operations

### 4. SDE Loading Phase
- Organizes feature classes into authority-based datasets
- Maps staging names to production SDE structure
- Supports truncate-and-load, replace, and append strategies
- Validates SDE connections and permissions

### SDE Dataset Organization
Feature classes are organized into datasets by authority:
```
GNG.Underlag_NVV/     # Naturvårdsverket data
  ├── naturvardsregistret_point
  ├── naturvardsregistret_polygon
  └── skyddade_omraden_polygon

GNG.Underlag_LST/     # Länsstyrelsen data  
  ├── riksintressen_polygon
  ├── biotopskydd_point
  └── miljoriskomraden_polygon
```

## 🛠️ Development

### Adding New Sources
1. Add source configuration to `config/sources.yaml`
2. Ensure the handler type matches an existing handler
3. Test with a small dataset first
4. Verify SDE loading and dataset organization

### Adding New Handlers
1. Create handler class in `etl/handlers/`
2. Implement required methods: `fetch()` and any validation
3. Register handler in `etl/handlers/__init__.py` 
4. Add configuration support if needed
5. Write unit tests for the new handler

### Code Structure
- **Pipeline orchestration**: `etl/pipeline.py`
- **Data models**: `etl/models.py` 
- **Configuration**: `etl/config.py`
- **Utilities**: `etl/utils/` directory
- **Exception handling**: `etl/exceptions.py`

### Development Setup
```bash
# Install development dependencies
pip install pytest coverage

# Run tests during development
python tests/test_runner.py unit

# Check configuration validation
python -c "from etl.config import ConfigManager; ConfigManager().load_global_config()"
```

## 📋 Requirements

### System Requirements
- **Python**: 3.8 or higher
- **ArcGIS**: ArcGIS Pro 3.3+ or ArcGIS Server 11.3+
- **Operating System**: Windows (for ArcGIS compatibility)
- **Memory**: Minimum 4GB RAM (8GB+ recommended for large datasets)
- **Storage**: 10GB+ free space for downloads and staging

### Python Dependencies
The pipeline uses only standard library and ArcGIS dependencies:
- `arcpy` (included with ArcGIS)
- `yaml` for configuration management
- `requests` for HTTP operations
- `pathlib` for path handling
- Standard library modules: `logging`, `json`, `time`, `threading`

### ArcGIS License Requirements
- ArcGIS Pro Standard or Advanced license
- Spatial Analyst extension (for some geoprocessing operations)
- Network connectivity to SDE database

### Data Requirements  
- Municipality boundary shapefile in `data/connections/`
- SDE connection file (`.sde`) with write permissions
- Network access to configured data sources

## 🤝 Contributing

### Development Process
1. Fork the repository
2. Create a feature branch from `main`
3. Make changes with appropriate error handling
4. Add or update tests as needed
5. Update documentation if required
6. Test changes thoroughly
7. Submit a pull request

### Code Standards
- Follow Python PEP 8 style guidelines
- Include docstrings for public methods
- Use type hints where appropriate
- Handle errors gracefully with informative messages
- Log significant operations with appropriate levels

### Testing Guidelines
- Write unit tests for new functions and classes
- Include integration tests for new handlers
- Test error conditions and edge cases
- Ensure tests can run without ArcGIS when possible
- Update test documentation

## 📄 License

This project is designed for Swedish municipal and governmental use in accordance with local data processing requirements and regulations. Usage should comply with data provider terms of service and applicable privacy laws.

---

## 🎯 Current Status

This ETL pipeline is a **production-ready system** currently used for Swedish municipal geographic data processing with the following capabilities:

### ✅ **Implemented Features**
- **Multiple data source support**: File downloads, REST APIs, OGC APIs, Atom feeds
- **Robust processing workflow**: Download → Stage → Geoprocess → SDE loading  
- **Comprehensive configuration**: YAML-based configuration with environment support
- **Error handling and recovery**: Continue-on-failure with detailed error reporting
- **Monitoring and logging**: Structured logging with run summaries
- **ArcGIS integration**: Full ArcPy integration for spatial operations and SDE connectivity
- **Modular architecture**: Extensible handler system for different data sources

### 🚀 **Key Benefits**
- **Automated data processing**: Reduces manual data management overhead
- **Reliable data updates**: Consistent processing with error recovery
- **Scalable architecture**: Handles multiple data sources and large datasets
- **Municipal focus**: Designed specifically for Swedish municipal data needs
- **Production stability**: Battle-tested with real-world data sources

The system successfully processes geographic data from major Swedish authorities including Naturvårdsverket, Länsstyrelser, Riksantikvarieämbetet, and others, providing municipalities with up-to-date spatial data for planning and decision-making.