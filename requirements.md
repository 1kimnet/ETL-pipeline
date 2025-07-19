# ETL Pipeline Dependencies

## ArcGIS Pro & Windows Compatibility Requirements

This document outlines the Python library dependencies for the ETL pipeline and their availability in ArcGIS Pro environments.

## Windows Compatibility Status

✅ **WINDOWS COMPATIBLE** - All Linux-specific code has been removed or made platform-aware:
- Fixed disk usage operations (no longer uses Unix-specific `/` path)
- Made `resource` module import conditional (Unix-specific module)
- Made configuration paths platform-aware (removed hardcoded `/etc/etl`)
- Removed redundant fallback modules that could mask real issues

## ✅ All Dependencies Are Bundled with ArcGIS Pro 3.3

**Great news!** All required libraries are included in the default ArcGIS Pro 3.3 Python environment. No additional installations required.

### Bundled Libraries (Available in ArcGIS Pro 3.3)

#### NumPy 1.24.3
- **Package**: `numpy`
- **Used in**: `etl/utils/regression_detector.py`
- **Purpose**: Statistical analysis for performance regression detection
- **ArcGIS Pro Status**: ✅ **BUNDLED** - Available by default
- **Functionality**: 
  - Linear regression calculations
  - Array operations for statistical analysis
  - Mathematical operations for performance metrics

#### SciPy 1.9.3
- **Package**: `scipy`
- **Used in**: `etl/utils/regression_detector.py`
- **Purpose**: Advanced statistical functions for performance analysis
- **ArcGIS Pro Status**: ✅ **BUNDLED** - Available by default
- **Functionality**:
  - Confidence interval calculations
  - T-tests and statistical significance testing
  - Change point detection
  - Advanced statistical distributions

#### psutil 5.9.0
- **Package**: `psutil`
- **Used in**: 
  - `etl/utils/performance_monitor.py`
  - `etl/utils/performance_optimizer.py`
  - `etl/utils/adaptive_tuning.py`
- **Purpose**: System resource monitoring and optimization
- **ArcGIS Pro Status**: ✅ **BUNDLED** - Available by default
- **Windows Compatibility**: ✅ **COMPATIBLE** - Works on Windows
- **Functionality**:
  - CPU usage monitoring
  - Memory usage tracking
  - Disk space monitoring
  - Network connection counting
  - Process resource monitoring

#### requests 2.31.0
- **Package**: `requests`
- **Used in**: Multiple modules for data downloading
- **Purpose**: HTTP requests for API interactions and data downloads
- **ArcGIS Pro Status**: ✅ **BUNDLED** - Available by default
- **Functionality**:
  - REST API data retrieval
  - File downloads
  - HTTP session management

#### PyYAML 6.0.1
- **Package**: `pyyaml`
- **Used in**: Configuration file parsing
- **Purpose**: Reading pipeline configuration files
- **ArcGIS Pro Status**: ✅ **BUNDLED** - Available by default
- **Functionality**:
  - Configuration file parsing
  - Data serialization/deserialization

## No Installation Required! 

### ✅ Ready to Use with ArcGIS Pro 3.3

**All dependencies are already included in the default ArcGIS Pro 3.3 Python environment.**

No additional installations, pip commands, or environment modifications needed.

### Verification Script

You can verify all dependencies are available by running this simple test:

```python
# Test script to verify bundled dependencies
import sys

def test_bundled_dependencies():
    """Test if all required bundled dependencies are available."""
    dependencies = {
        'numpy': 'Statistical analysis for performance monitoring',
        'scipy': 'Advanced statistical functions', 
        'psutil': 'System resource monitoring',
        'requests': 'HTTP client for data downloads',
        'yaml': 'Configuration file parsing',
        'arcpy': 'ArcGIS geoprocessing functions'
    }
    
    results = {}
    for package, description in dependencies.items():
        try:
            module = __import__(package)
            version = getattr(module, '__version__', 'N/A')
            results[package] = f"✅ Available (v{version})"
        except ImportError:
            results[package] = "❌ Missing"
    
    print("ArcGIS Pro 3.3 Bundled Dependencies Check:")
    print("-" * 50)
    for package, status in results.items():
        print(f"{package:12} {status}")
    
    missing = [pkg for pkg, status in results.items() if "Missing" in status]
    if missing:
        print(f"\n❌ Missing packages: {', '.join(missing)}")
        print("This may indicate an ArcGIS Pro installation issue.")
        return False
    else:
        print("\n✅ All bundled dependencies are available!")
        print("ETL pipeline is ready to use!")
        return True

if __name__ == "__main__":
    test_bundled_dependencies()
```

## ✅ Complete Pipeline Functionality Available

### Core ETL Operations
- **Downloads**: ✅ Fully functional with bundled `requests`
- **Staging**: ✅ Fully functional with bundled libraries
- **Geoprocessing**: ✅ Fully functional with bundled `arcpy`
- **SDE Loading**: ✅ Fully functional with bundled `arcpy`

### Advanced Features (All Available)
- **Performance Monitoring**: ✅ Available with bundled `psutil`
- **Regression Detection**: ✅ Available with bundled `numpy` and `scipy`
- **Adaptive Tuning**: ✅ Available with bundled `psutil`
- **Statistical Analysis**: ✅ Available with bundled `scipy`
- **System Health Monitoring**: ✅ Available with bundled `psutil`
- **Intelligent Caching**: ✅ Available with bundled libraries

## Fail-Fast Behavior

The pipeline is designed with **fail-fast** behavior for production reliability:

1. **Missing `psutil`**: Pipeline will fail immediately (should be bundled)
2. **Missing `numpy/scipy`**: Pipeline will fail immediately (should be bundled)
3. **Missing `requests`**: Pipeline will fail immediately (should be bundled)
4. **Missing `yaml`**: Pipeline will fail immediately (should be bundled)
5. **Missing `arcpy`**: Pipeline will fail immediately (should be bundled)

**Note**: All libraries should be available in ArcGIS Pro 3.3. If any are missing, this indicates an ArcGIS Pro installation issue.

## Recommendations

### For Production Deployment

1. **No installation required** - All dependencies are bundled with ArcGIS Pro 3.3
2. **Test in target environment** before production deployment
3. **Use the verification script** to confirm all dependencies are available
4. **Monitor ArcGIS Pro updates** for any changes to bundled libraries

### For Development

1. **Use ArcGIS Pro Python environment** - No need for virtual environments
2. **Test with bundled dependencies** - Ensures consistency with production
3. **Document any additional dependencies** if added to the pipeline

## Troubleshooting

### Common Issues

1. **Import Errors**: Verify ArcGIS Pro installation is complete
2. **Missing Libraries**: Run the verification script to identify issues
3. **Path Issues**: Ensure using the correct ArcGIS Pro Python environment
4. **Version Mismatches**: Check ArcGIS Pro version (requires 3.3+)

### Support

For dependency-related issues:
1. **Check ArcGIS Pro Python environment**: Verify you're using the correct Python
2. **Run verification script**: Use the provided script to test all dependencies
3. **Check ArcGIS Pro version**: Ensure you're using version 3.3 or higher
4. **ArcGIS Pro installation**: Verify complete installation with all components

### ArcGIS Pro Python Environment

The pipeline is designed to work with the **default ArcGIS Pro Python environment**:
- **Location**: Usually `C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\`
- **Python Version**: 3.9+ (varies by ArcGIS Pro version)
- **All required libraries**: Bundled and ready to use

**No environment cloning or modification required!**