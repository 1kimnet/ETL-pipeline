from __future__ import annotations

import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


def debug_shapefile_directory(base_path: str = "data/staging/LST/vindkraftskollen_vindkraftverk") -> None:
    """🔍 Debug what files exist in the shapefile directory."""
    staging_dir = Path(base_path)
    
    log.info("🔍 Debugging shapefile directory: %s", staging_dir.absolute())
    
    if not staging_dir.exists():
        log.error("❌ Directory does not exist: %s", staging_dir)
        return
        
    log.info("✅ Directory exists at: %s", staging_dir.absolute())
    
    # List all files
    all_files = list(staging_dir.iterdir())
    log.info("📁 Found %d items in directory", len(all_files))
    
    for item in sorted(all_files):
        if item.is_file():
            log.info("  📄 File: %s (size: %d bytes)", item.name, item.stat().st_size)
        elif item.is_dir():
            log.info("  📁 Dir: %s/", item.name)
            # List files in subdirectory
            sub_files = list(item.iterdir())
            for sub_item in sorted(sub_files)[:10]:  # Limit to first 10
                if sub_item.is_file():
                    log.info("    📄 %s (size: %d bytes)", sub_item.name, sub_item.stat().st_size)
    
    # Look specifically for shapefiles
    shp_files = list(staging_dir.rglob("*.shp"))
    log.info("\n🔍 Shapefile search results:")
    if shp_files:
        for shp in shp_files:
            log.info("  ✅ Found: %s", shp.relative_to(staging_dir))
            
            # Check for required components
            shp_stem = shp.stem
            shp_dir = shp.parent
            
            for ext in ['.shx', '.dbf', '.prj']:
                component = shp_dir / f"{shp_stem}{ext}"
                if component.exists():
                    log.info("    ✅ %s exists", component.name)
                else:
                    log.warning("    ⚠️ %s is MISSING", component.name)
    else:
        log.warning("⚠️ No .shp files found in %s", staging_dir)


if __name__ == "__main__":
    # Run the debug function
    debug_shapefile_directory()
    
    # Also check the downloads directory
    log.info("\n" + "="*60 + "\n")
    log.info("🔍 Checking downloads directory...")
    
    downloads_dir = Path("data/downloads")
    if downloads_dir.exists():
        zip_files = list(downloads_dir.glob("*.zip"))
        log.info("Found %d ZIP files in downloads:", len(zip_files))
        for zf in zip_files:
            log.info("  📦 %s (size: %d bytes)", zf.name, zf.stat().st_size)