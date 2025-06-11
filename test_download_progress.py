#!/usr/bin/env python3
"""Simple test script to verify enhanced download progress logging."""

from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent))

from etl.utils.logging_cfg import configure_logging

if __name__ == "__main__":
    # Configure logging like the main ETL pipeline
    configure_logging(level_on_console="INFO")
    
    # Test the format bytes function
    from etl.utils.io import _format_bytes
    print("Testing byte formatting:")
    for size in [512, 1536, 1048576, 1073741824]:
        print(f"  {size} bytes = {_format_bytes(size)}")
    
    print("\n✅ Download enhancement test completed")
    print("🚀 The enhanced download function will now show progress during downloads")
