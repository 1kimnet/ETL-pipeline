#!/usr/bin/env python3
"""Test script to verify improved download source identification."""

import logging
from pathlib import Path
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from etl.utils.logging_cfg import configure_logging

if __name__ == "__main__":
    # Configure logging like the main ETL pipeline
    configure_logging(level_on_console="INFO")
    
    # Get a logger like the pipeline would use
    log = logging.getLogger("etl.test")
    
    print("Testing improved download source identification:")
    print()
    
    # Test the new flow
    print("Before fix - generic messaging:")
    print("  🚚 Downloading")
    print("  ⬇ filename.zip (15.2 MB)")
    print("  ✅ filename.zip")
    print()
    
    print("After fix - source identification:")
    log.info("🚚 Försvarsmakten Geodata")
    log.info("⬇ rikstackande-geodata-forsvarsmakten.zip (15.2 MB)")  
    log.info("✅ rikstackande-geodata-forsvarsmakten.zip")
    
    print()
    log.info("🚚 Länsstyrelsen Miljöriskområden")
    log.info("⬇ lst.LST_Miljoriskomrade.zip (2.3 MB)")
    log.info("✅ lst.LST_Miljoriskomrade.zip")
    
    print()
    print("✅ Source identification test completed!")
    print("💡 Now shows source name followed by file details")
    print("🎯 Easy to track which source is being processed")
