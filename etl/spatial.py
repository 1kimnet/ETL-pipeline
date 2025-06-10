"""Spatial operations abstraction layer for ETL pipeline.

This module provides an abstraction layer over ArcPy and other spatial libraries,
reducing vendor lock-in and improving testability. It includes interfaces for
common spatial operations and pluggable backend implementations.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol, Tuple, Union

from .exceptions import GeospatialError, LicenseError, WorkspaceError

log = logging.getLogger(__name__)


@dataclass
class SpatialReference:
    """Spatial reference system information."""
    code: int
    name: str
    wkt: Optional[str] = None
    proj4: Optional[str] = None
    
    def __str__(self) -> str:
        return f"EPSG:{self.code} ({self.name})"


@dataclass
class GeometryInfo:
    """Geometry information and statistics."""
    geometry_type: str  # Point, LineString, Polygon, etc.
    feature_count: int
    spatial_reference: SpatialReference
    extent: Tuple[float, float, float, float]  # minx, miny, maxx, maxy
    has_z: bool = False
    has_m: bool = False


@dataclass
class FieldInfo:
    """Field/attribute information."""
    name: str
    type: str  # String, Integer, Double, Date, etc.
    length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    nullable: bool = True
    alias: Optional[str] = None


@dataclass
class DatasetInfo:
    """Complete dataset information."""
    name: str
    path: str
    dataset_type: str  # FeatureClass, Table, Raster, etc.
    geometry_info: Optional[GeometryInfo] = None
    fields: List[FieldInfo] = None
    record_count: Optional[int] = None
    
    def __post_init__(self):
        if self.fields is None:
            self.fields = []


class SpatialBackend(Protocol):
    """Protocol defining the interface for spatial operation backends."""
    
    def is_available(self) -> bool:
        """Check if the backend is available and can be used."""
        ...
    
    def get_workspace(self) -> str:
        """Get current workspace path."""
        ...
    
    def set_workspace(self, workspace: Union[str, Path]) -> None:
        """Set workspace for spatial operations."""
        ...
    
    def describe_dataset(self, dataset_path: str) -> DatasetInfo:
        """Get information about a spatial dataset."""
        ...
    
    def project_dataset(
        self, 
        input_dataset: str, 
        output_dataset: str, 
        target_srs: Union[int, str, SpatialReference]
    ) -> str:
        """Project dataset to different spatial reference system."""
        ...
    
    def clip_dataset(
        self, 
        input_dataset: str, 
        clip_features: str, 
        output_dataset: str
    ) -> str:
        """Clip dataset using clip features."""
        ...
    
    def buffer_features(
        self, 
        input_dataset: str, 
        output_dataset: str, 
        distance: float, 
        unit: str = "meters"
    ) -> str:
        """Create buffer around features."""
        ...
    
    def copy_features(self, input_dataset: str, output_dataset: str) -> str:
        """Copy features from one dataset to another."""
        ...


class ArcPySpatialBackend:
    """ArcPy-based spatial operations backend."""
    
    def __init__(self):
        self._arcpy = None
        self._available = False
        self._initialize_arcpy()
    
    def _initialize_arcpy(self):
        """Initialize ArcPy if available."""
        try:
            import arcpy
            self._arcpy = arcpy
            self._available = True
            
            # Configure ArcPy environment
            arcpy.env.overwriteOutput = True
            
            log.info("✅ ArcPy spatial backend initialized")
        except ImportError:
            log.warning("⚠️ ArcPy not available - some spatial operations will be disabled")
        except Exception as e:
            log.error("❌ Failed to initialize ArcPy: %s", e)
    
    def is_available(self) -> bool:
        """Check if ArcPy is available."""
        return self._available
    
    def get_workspace(self) -> str:
        """Get current ArcPy workspace."""
        if not self._available:
            raise LicenseError("ArcPy not available")
        return str(self._arcpy.env.workspace or "")
    
    def set_workspace(self, workspace: Union[str, Path]) -> None:
        """Set ArcPy workspace."""
        if not self._available:
            raise LicenseError("ArcPy not available")
        
        workspace_str = str(workspace)
        self._arcpy.env.workspace = workspace_str
        log.debug("Set ArcPy workspace to: %s", workspace_str)
    
    def describe_dataset(self, dataset_path: str) -> DatasetInfo:
        """Describe spatial dataset using ArcPy."""
        if not self._available:
            raise LicenseError("ArcPy not available")
        
        try:
            desc = self._arcpy.Describe(dataset_path)
            
            # Get basic info
            info = DatasetInfo(
                name=desc.name,
                path=desc.catalogPath,
                dataset_type=desc.dataType
            )
            
            # Get record count if available
            if hasattr(desc, 'featureClass') or desc.dataType == 'FeatureClass':
                try:
                    info.record_count = int(self._arcpy.management.GetCount(dataset_path)[0])
                except Exception:
                    pass
            
            # Get geometry info for feature classes
            if hasattr(desc, 'shapeType'):
                spatial_ref = self._parse_spatial_reference(desc.spatialReference)
                extent = (desc.extent.XMin, desc.extent.YMin, desc.extent.XMax, desc.extent.YMax)
                
                info.geometry_info = GeometryInfo(
                    geometry_type=desc.shapeType,
                    feature_count=info.record_count or 0,
                    spatial_reference=spatial_ref,
                    extent=extent,
                    has_z=getattr(desc, 'hasZ', False),
                    has_m=getattr(desc, 'hasM', False)
                )
            
            # Get field information
            if hasattr(desc, 'fields'):
                info.fields = [
                    FieldInfo(
                        name=field.name,
                        type=field.type,
                        length=getattr(field, 'length', None),
                        precision=getattr(field, 'precision', None),
                        scale=getattr(field, 'scale', None),
                        nullable=not field.required,
                        alias=getattr(field, 'aliasName', None)
                    )
                    for field in desc.fields
                ]
            
            return info
            
        except Exception as e:
            raise GeospatialError(f"Failed to describe dataset {dataset_path}: {e}", operation="describe") from e
    
    def _parse_spatial_reference(self, sr) -> SpatialReference:
        """Parse ArcPy spatial reference to our format."""
        try:
            return SpatialReference(
                code=sr.factoryCode,
                name=sr.name,
                wkt=sr.exportToString() if hasattr(sr, 'exportToString') else None
            )
        except Exception:
            return SpatialReference(code=0, name="Unknown")
    
    def project_dataset(
        self, 
        input_dataset: str, 
        output_dataset: str, 
        target_srs: Union[int, str, SpatialReference]
    ) -> str:
        """Project dataset using ArcPy."""
        if not self._available:
            raise LicenseError("ArcPy not available")
        
        try:
            # Convert target SRS to ArcPy format
            if isinstance(target_srs, int):
                srs_obj = self._arcpy.SpatialReference(target_srs)
            elif isinstance(target_srs, SpatialReference):
                srs_obj = self._arcpy.SpatialReference(target_srs.code)
            else:
                srs_obj = target_srs
            
            self._arcpy.management.Project(input_dataset, output_dataset, srs_obj)
            
            log.info("✅ Projected %s to %s", input_dataset, output_dataset)
            return output_dataset
            
        except Exception as e:
            raise GeospatialError(f"Projection failed: {e}", operation="project") from e
    
    def clip_dataset(
        self, 
        input_dataset: str, 
        clip_features: str, 
        output_dataset: str
    ) -> str:
        """Clip dataset using ArcPy."""
        if not self._available:
            raise LicenseError("ArcPy not available")
        
        try:
            self._arcpy.analysis.Clip(input_dataset, clip_features, output_dataset)
            
            log.info("✅ Clipped %s with %s to %s", input_dataset, clip_features, output_dataset)
            return output_dataset
            
        except Exception as e:
            raise GeospatialError(f"Clipping failed: {e}", operation="clip") from e
    
    def buffer_features(
        self, 
        input_dataset: str, 
        output_dataset: str, 
        distance: float, 
        unit: str = "meters"
    ) -> str:
        """Buffer features using ArcPy."""
        if not self._available:
            raise LicenseError("ArcPy not available")
        
        try:
            buffer_distance = f"{distance} {unit}"
            self._arcpy.analysis.Buffer(input_dataset, output_dataset, buffer_distance)
            
            log.info("✅ Buffered %s by %s to %s", input_dataset, buffer_distance, output_dataset)
            return output_dataset
            
        except Exception as e:
            raise GeospatialError(f"Buffer operation failed: {e}", operation="buffer") from e
    
    def copy_features(self, input_dataset: str, output_dataset: str) -> str:
        """Copy features using ArcPy."""
        if not self._available:
            raise LicenseError("ArcPy not available")
        
        try:
            self._arcpy.management.CopyFeatures(input_dataset, output_dataset)
            
            log.info("✅ Copied features from %s to %s", input_dataset, output_dataset)
            return output_dataset
            
        except Exception as e:
            raise GeospatialError(f"Copy operation failed: {e}", operation="copy") from e


class SpatialOperations:
    """Main spatial operations manager with pluggable backends."""
    
    def __init__(self, backend: Optional[SpatialBackend] = None):
        if backend is None:
            backend = ArcPySpatialBackend()
        
        self.backend = backend
        self.workspace: Optional[str] = None
        
        if not self.backend.is_available():
            log.warning("⚠️ Spatial backend not available - spatial operations will be limited")
    
    def set_workspace(self, workspace: Union[str, Path]) -> None:
        """Set workspace for spatial operations."""
        self.workspace = str(workspace)
        
        if self.backend.is_available():
            self.backend.set_workspace(workspace)
            log.info("📂 Set spatial workspace: %s", workspace)
        else:
            log.warning("⚠️ Cannot set workspace - spatial backend not available")
    
    def get_workspace(self) -> Optional[str]:
        """Get current workspace."""
        if self.backend.is_available():
            return self.backend.get_workspace()
        return self.workspace
    
    def describe_dataset(self, dataset_path: str) -> DatasetInfo:
        """Get information about a spatial dataset."""
        if not self.backend.is_available():
            raise LicenseError("Spatial backend not available")
        
        return self.backend.describe_dataset(dataset_path)
    
    def project_to_target_srs(
        self, 
        input_dataset: str, 
        output_dataset: str, 
        target_srs: Union[int, str, SpatialReference],
        validate: bool = True
    ) -> str:
        """Project dataset to target spatial reference system."""
        if not self.backend.is_available():
            raise LicenseError("Spatial backend not available")
        
        # Validate inputs if requested
        if validate:
            input_info = self.describe_dataset(input_dataset)
            if input_info.geometry_info is None:
                raise GeospatialError(f"Dataset {input_dataset} has no geometry", operation="project")
        
        return self.backend.project_dataset(input_dataset, output_dataset, target_srs)
    
    def clip_to_boundary(
        self, 
        input_dataset: str, 
        clip_features: str, 
        output_dataset: str,
        validate: bool = True
    ) -> str:
        """Clip dataset to boundary features."""
        if not self.backend.is_available():
            raise LicenseError("Spatial backend not available")
        
        # Validate inputs if requested
        if validate:
            self._validate_dataset_exists(input_dataset)
            self._validate_dataset_exists(clip_features)
        
        return self.backend.clip_dataset(input_dataset, clip_features, output_dataset)
    
    def create_buffer(
        self, 
        input_dataset: str, 
        output_dataset: str, 
        distance: float, 
        unit: str = "meters"
    ) -> str:
        """Create buffer around features."""
        if not self.backend.is_available():
            raise LicenseError("Spatial backend not available")
        
        if distance <= 0:
            raise GeospatialError("Buffer distance must be positive", operation="buffer")
        
        return self.backend.buffer_features(input_dataset, output_dataset, distance, unit)
    
    def copy_dataset(self, input_dataset: str, output_dataset: str) -> str:
        """Copy spatial dataset."""
        if not self.backend.is_available():
            raise LicenseError("Spatial backend not available")
        
        return self.backend.copy_features(input_dataset, output_dataset)
    
    def _validate_dataset_exists(self, dataset_path: str) -> None:
        """Validate that a dataset exists and is accessible."""
        try:
            self.describe_dataset(dataset_path)
        except Exception as e:
            raise GeospatialError(f"Dataset validation failed for {dataset_path}: {e}", operation="validate") from e
    
    def get_backend_info(self) -> Dict[str, Any]:
        """Get information about the current spatial backend."""
        return {
            'backend_type': type(self.backend).__name__,
            'available': self.backend.is_available(),
            'workspace': self.get_workspace()
        }


# Plugin system for spatial backends
class SpatialBackendRegistry:
    """Registry for spatial backend plugins."""
    
    def __init__(self):
        self._backends: Dict[str, type] = {}
        self._register_default_backends()
    
    def _register_default_backends(self):
        """Register default spatial backends."""
        self.register('arcpy', ArcPySpatialBackend)
    
    def register(self, name: str, backend_class: type):
        """Register a spatial backend."""
        self._backends[name] = backend_class
        log.debug("Registered spatial backend: %s", name)
    
    def create_backend(self, name: str) -> SpatialBackend:
        """Create a spatial backend instance."""
        if name not in self._backends:
            raise ValueError(f"Unknown spatial backend: {name}")
        
        backend_class = self._backends[name]
        return backend_class()
    
    def list_backends(self) -> List[str]:
        """List available spatial backends."""
        return list(self._backends.keys())
    
    def get_available_backends(self) -> List[str]:
        """Get list of available (working) backends."""
        available = []
        for name in self._backends:
            try:
                backend = self.create_backend(name)
                if backend.is_available():
                    available.append(name)
            except Exception:
                pass  # Backend not available
        return available


# Global instances
_backend_registry = SpatialBackendRegistry()
_spatial_operations = None


def get_spatial_operations(backend: Optional[str] = None) -> SpatialOperations:
    """Get global spatial operations instance."""
    global _spatial_operations
    
    if _spatial_operations is None or backend is not None:
        if backend:
            backend_instance = _backend_registry.create_backend(backend)
            _spatial_operations = SpatialOperations(backend_instance)
        else:
            _spatial_operations = SpatialOperations()
    
    return _spatial_operations


def register_spatial_backend(name: str, backend_class: type):
    """Register a custom spatial backend."""
    _backend_registry.register(name, backend_class)


def list_spatial_backends() -> List[str]:
    """List all registered spatial backends."""
    return _backend_registry.list_backends()


def get_available_spatial_backends() -> List[str]:
    """Get list of available spatial backends."""
    return _backend_registry.get_available_backends()