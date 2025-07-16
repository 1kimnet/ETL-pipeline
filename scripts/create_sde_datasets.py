# scripts/create_sde_datasets.py
"""
Script to create feature datasets in production SDE for the ETL pipeline.
Creates datasets with SWEREF99 12 00 (WKID: 3010) spatial reference.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Final, List

import arcpy

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log: Final = logging.getLogger(__name__)

# Configuration
SDE_CONNECTION = "data/connections/prod.sde"
TARGET_SRID = 3010  # SWEREF99 12 00

# Datasets to create based on your authorities
DATASETS_TO_CREATE = [
    "FM",  # Försvarsmakten
    "NVV",  # Naturvårdsverket
    "LST",  # Länsstyrelsen
    "LSTD",  # Länsstyrelsen Södermanland
    "MSB",  # Myndigheten för samhällsskydd och beredskap
    "RAA",  # Riksantikvarieämbetet
    "SGI",  # Statens geotekniska institut
    "SGU",  # Sveriges geologiska undersökning
    "SJV",  # Jordbruksverket
    "SKS",  # Skogsstyrelsen
    "SVK",  # Svenska kraftnät
    "TRV",  # Trafikverket
]


def create_sde_datasets(
    sde_connection: str | Path, datasets: List[str], target_srid: int
) -> None:
    """🔄 Create feature datasets in SDE geodatabase.

    Args:
        sde_connection: Path to SDE connection file
        datasets: List of dataset names to create
        target_srid: Spatial reference system ID (3010 = SWEREF99 12 00)
    """
    sde_path = Path(sde_connection)

    if not sde_path.exists():
        raise FileNotFoundError(f"SDE connection file not found: {sde_path}")

    log.info("🔄 Creating %d feature datasets in SDE", len(datasets))
    log.info("📍 Target spatial reference: EPSG:%d", target_srid)

    # Create spatial reference object
    try:
        spatial_ref = arcpy.SpatialReference(target_srid)
        log.info("✅ Spatial reference: %s", spatial_ref.name)
    except Exception as exc:
        log.error(
            "❌ Failed to create spatial reference for EPSG:%d: %s", target_srid, exc
        )
        raise

    created_count = 0
    skipped_count = 0
    error_count = 0

    for dataset_name in datasets:
        try:
            dataset_path = str(Path(sde_connection) / dataset_name)

            # Check if dataset already exists
            if arcpy.Exists(dataset_path):
                log.info("⏭️ Dataset already exists: %s", dataset_name)
                skipped_count += 1
                continue

            # Create the feature dataset
            log.info("🆕 Creating dataset: %s", dataset_name)
            arcpy.management.CreateFeatureDataset(
                out_dataset_path=str(sde_connection),
                out_name=dataset_name,
                spatial_reference=spatial_ref,
            )

            log.info("✅ Created dataset: %s", dataset_name)
            created_count += 1

        except arcpy.ExecuteError:
            log.error(
                "❌ Failed to create dataset %s: %s", dataset_name, arcpy.GetMessages(2)
            )
            error_count += 1
        except Exception as exc:
            log.error("❌ Unexpected error creating dataset %s: %s", dataset_name, exc)
            error_count += 1

    # Summary
    log.info("📊 Dataset creation complete:")
    log.info("   ✅ Created: %d", created_count)
    log.info("   ⏭️ Skipped: %d", skipped_count)
    log.info("   ❌ Errors:  %d", error_count)

    if error_count > 0:
        log.warning(
            "⚠️ Some datasets failed to create. Check SDE permissions and connection."
        )
    else:
        log.info("🎉 All datasets processed successfully!")


def list_existing_datasets(sde_connection: str | Path) -> None:
    """📋 List existing feature datasets in SDE for verification."""
    log.info("📋 Listing existing feature datasets in SDE...")

    try:
        with arcpy.EnvManager(workspace=str(sde_connection)):
            datasets = arcpy.ListDatasets(feature_type="Feature")

        if datasets:
            log.info("📁 Found %d existing feature datasets:", len(datasets))
            for dataset in sorted(datasets):
                log.info("   📁 %s", dataset)
        else:
            log.info("📁 No existing feature datasets found")

    except Exception as exc:
        log.error("❌ Failed to list datasets: %s", exc)


def main() -> None:
    """Main function to create SDE datasets."""
    log.info("🚀 Starting SDE dataset creation script")

    sde_connection_path = Path(SDE_CONNECTION)

    # Validate SDE connection
    if not sde_connection_path.exists():
        log.error("❌ SDE connection file not found: %s", sde_connection_path)
        log.error("   Create an SDE connection file first using ArcGIS Pro")
        return

    try:
        # List existing datasets first
        list_existing_datasets(sde_connection_path)

        # Create new datasets
        create_sde_datasets(
            sde_connection=sde_connection_path,
            datasets=DATASETS_TO_CREATE,
            target_srid=TARGET_SRID,
        )

        # List datasets again to verify
        log.info("")
        list_existing_datasets(sde_connection_path)

    except Exception as exc:
        log.error("❌ Script failed: %s", exc, exc_info=True)


if __name__ == "__main__":
    main()
