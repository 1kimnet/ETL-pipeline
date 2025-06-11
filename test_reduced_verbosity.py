#!/usr/bin/env python3
"""Test script to verify reduced logging verbosity."""

import logging
from pathlib import Path
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from etl.utils.logging_cfg import configure_logging

if __name__ == "__main__":
    # Configure logging like the main ETL pipeline
    configure_logging(level_on_console="INFO")
    
    # Get a logger like the handlers would use
    log = logging.getLogger("etl.test")
    
    print("Testing reduced verbosity logging patterns:")
    print()
    
    # Test download-style logging
    log.info("📥 large_dataset.gpkg (15.2 MB)")
    log.debug("📊 large_dataset.gpkg: 25.0% (3.8 MB / 15.2 MB)")
    log.debug("📊 large_dataset.gpkg: 50.0% (7.6 MB / 15.2 MB)")  
    log.debug("📊 large_dataset.gpkg: 75.0% (11.4 MB / 15.2 MB)")
    log.info("✅ large_dataset.gpkg")
    
    print()
    
    # Test REST API-style logging
    log.info("🚚 layer_name_sanitized")
    log.debug("Fetching page 1 for layer layer_name_sanitized (offset 0, limit 2000)")
    log.debug("🏁 All features likely retrieved for layer layer_name_sanitized")
    log.info("✅ layer_name_sanitized: 156 features")
    
    print()
    log.info("🚚 empty_layer")
    log.info("ℹ️ empty_layer: no features")
    
    print()
    print("✅ Reduced verbosity test completed!")
    print("💡 Console now shows clean progress summary")
    print("🔍 Detailed progress is available in debug logs")
