# ETL Pipeline Dependencies

## ArcGIS Pro & Windows Compatibility Requirements

This document outlines the external dependencies required for the ETL pipeline and their compatibility with ArcGIS Pro environments running on Windows Server/ArcGIS Server.

## Windows Compatibility Status

✅ **WINDOWS COMPATIBLE** - All Linux-specific code has been removed or made platform-aware:
- Fixed disk usage operations (no longer uses Unix-specific `/` path)
- Made `resource` module import conditional (Unix-specific module)
- Made configuration paths platform-aware (removed hardcoded `/etc/etl`)
- Removed redundant fallback modules that could mask real issues

## Critical Dependencies (Not Bundled with ArcGIS Pro)

### 1. Scientific Computing Libraries

#### NumPy
- **Package**: `numpy`
- **Version**: `>=1.21.0`
- **Used in**: `etl/utils/regression_detector.py`
- **Purpose**: Statistical analysis for performance regression detection
- **Installation**: `pip install numpy`
- **ArcGIS Pro Status**: ❌ **NOT BUNDLED** - Requires separate installation
- **Functionality**: 
  - Linear regression calculations
  - Array operations for statistical analysis
  - Mathematical operations for performance metrics

#### SciPy
- **Package**: `scipy`
- **Version**: `>=1.7.0`
- **Used in**: `etl/utils/regression_detector.py`
- **Purpose**: Advanced statistical functions for performance analysis
- **Installation**: `pip install scipy`
- **ArcGIS Pro Status**: ❌ **NOT BUNDLED** - Requires separate installation
- **Functionality**:
  - Confidence interval calculations
  - T-tests and statistical significance testing
  - Change point detection
  - Advanced statistical distributions

### 2. System Monitoring Library

#### psutil
- **Package**: `psutil`
- **Version**: `>=5.8.0`
- **Used in**: 
  - `etl/utils/performance_monitor.py`
  - `etl/utils/performance_optimizer.py`
  - `etl/utils/adaptive_tuning.py`
- **Purpose**: System resource monitoring and optimization
- **Installation**: `pip install psutil`
- **ArcGIS Pro Status**: ❌ **NOT BUNDLED** - Requires separate installation
- **Windows Compatibility**: ✅ **COMPATIBLE** - Works on Windows
- **Functionality**:
  - CPU usage monitoring
  - Memory usage tracking
  - Disk space monitoring
  - Network connection counting
  - Process resource monitoring

## Standard Dependencies (Likely Available)

### 3. HTTP Client Library

#### requests
- **Package**: `requests`
- **Version**: `>=2.25.0`
- **Used in**: Multiple modules for data downloading
- **Purpose**: HTTP requests for API interactions and data downloads
- **Installation**: `pip install requests`
- **ArcGIS Pro Status**: ✅ **LIKELY BUNDLED** - Usually available
- **Functionality**:
  - REST API data retrieval
  - File downloads
  - HTTP session management

### 4. YAML Processing

#### PyYAML
- **Package**: `pyyaml`
- **Version**: `>=5.4.0`
- **Used in**: Configuration file parsing
- **Purpose**: Reading pipeline configuration files
- **Installation**: `pip install pyyaml`
- **ArcGIS Pro Status**: ✅ **LIKELY BUNDLED** - Usually available
- **Functionality**:
  - Configuration file parsing
  - Data serialization/deserialization

## Installation Instructions

### For ArcGIS Pro Environment

1. **Open ArcGIS Pro Python Command Prompt** (as Administrator)
2. **Install required packages**:
   ```cmd
   pip install numpy scipy psutil
   ```

### Alternative Installation via Conda

```cmd
conda install numpy scipy psutil -c conda-forge
```

### Verification Script

```python
# Test script to verify dependencies
import sys

def test_dependencies():
    """Test if all required dependencies are available."""
    dependencies = {
        'numpy': 'Statistical analysis for performance monitoring',
        'scipy': 'Advanced statistical functions',
        'psutil': 'System resource monitoring',
        'requests': 'HTTP client for data downloads',
        'yaml': 'Configuration file parsing'
    }
    
    results = {}
    for package, description in dependencies.items():
        try:
            __import__(package)
            results[package] = "✅ Available"
        except ImportError:
            results[package] = "❌ Missing"
    
    print("Dependency Check Results:")
    print("-" * 50)
    for package, status in results.items():
        print(f"{package:12} {status}")
    
    missing = [pkg for pkg, status in results.items() if "Missing" in status]
    if missing:
        print(f"\nMissing packages: {', '.join(missing)}")
        print("Install with: pip install " + " ".join(missing))
        return False
    else:
        print("\n✅ All dependencies are available!")
        return True

if __name__ == "__main__":
    test_dependencies()
```

## Impact on Pipeline Functionality

### Core ETL Operations
- **Downloads**: ✅ Works without optional dependencies
- **Staging**: ✅ Works without optional dependencies  
- **Geoprocessing**: ✅ Works without optional dependencies
- **SDE Loading**: ✅ Works without optional dependencies

### Advanced Features (Require Dependencies)
- **Performance Monitoring**: ❌ Requires `psutil`
- **Regression Detection**: ❌ Requires `numpy` and `scipy`
- **Adaptive Tuning**: ❌ Requires `psutil`
- **Statistical Analysis**: ❌ Requires `scipy`

## Fallback Behavior

The pipeline is designed with **fail-fast** behavior for production reliability:

1. **Missing `psutil`**: Pipeline will fail immediately (required for monitoring)
2. **Missing `numpy/scipy`**: Pipeline will fail immediately (required for regression detection)
3. **Missing `requests`**: Pipeline will fail immediately (critical dependency)
4. **Missing `yaml`**: Pipeline will fail immediately (critical dependency)
5. **Missing `arcpy`**: Pipeline will fail immediately (required for ArcGIS operations)

**Note**: Redundant fallback modules have been removed to prevent silent failures that could mask real issues in production.

## Recommendations

### For Production Deployment

1. **Install all dependencies** for full functionality
2. **Test in target environment** before production deployment
3. **Monitor ArcGIS Pro updates** that might affect dependency availability
4. **Consider conda environment** for better dependency management

### For Development

1. **Use virtual environment** to isolate dependencies
2. **Test with and without optional dependencies** to ensure fallback behavior
3. **Document any new dependencies** added to the pipeline

## Troubleshooting

### Common Issues

1. **Permission Errors**: Run installation as Administrator
2. **Version Conflicts**: Use conda environment for better management
3. **Missing C++ Compiler**: Install Visual Studio Build Tools for Windows
4. **ArcGIS Pro Python Environment**: Ensure using correct Python environment

### Support

For dependency-related issues:
1. Check ArcGIS Pro Python environment: `where python`
2. Verify pip installation: `pip --version`
3. Test import in Python console
4. Check for conflicting packages: `pip list`