"""Data quality validation and schema enforcement for ETL pipeline.

This module provides comprehensive data validation capabilities including
schema validation, geometry validation, attribute validation, and data quality checks.
"""
from __future__ import annotations

import json
import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field as dataclass_field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union

from .exceptions import DataQualityError, ValidationError, GeospatialError
from .spatial import SpatialOperations, DatasetInfo, FieldInfo

log = logging.getLogger(__name__)


@dataclass
class ValidationRule:
    """Definition of a validation rule."""
    name: str
    rule_type: str  # required, pattern, range, geometry, custom
    field: Optional[str] = None
    parameters: Dict[str, Any] = dataclass_field(default_factory=dict)
    severity: str = "error"  # error, warning, info
    message: Optional[str] = None


@dataclass
class ValidationResult:
    """Result of a validation check."""
    rule_name: str
    field: Optional[str]
    severity: str
    message: str
    record_id: Optional[Union[str, int]] = None
    value: Optional[Any] = None
    details: Dict[str, Any] = dataclass_field(default_factory=dict)


@dataclass
class ValidationSummary:
    """Summary of validation results."""
    total_records: int
    valid_records: int
    error_count: int
    warning_count: int
    info_count: int
    rules_applied: int
    validation_time_seconds: float
    results: List[ValidationResult] = dataclass_field(default_factory=list)
    
    @property
    def validity_rate(self) -> float:
        """Get validity rate as percentage."""
        return (self.valid_records / self.total_records * 100) if self.total_records > 0 else 0.0
    
    @property
    def has_errors(self) -> bool:
        """Check if validation found errors."""
        return self.error_count > 0


class Validator(ABC):
    """Base class for data validators."""
    
    @abstractmethod
    def validate(self, data: Any, schema: Optional[Dict] = None) -> List[ValidationResult]:
        """Validate data against rules."""
        pass


class SchemaValidator(Validator):
    """Validates data against a defined schema."""
    
    def __init__(self):
        self.validators = {
            'required': self._validate_required,
            'type': self._validate_type,
            'pattern': self._validate_pattern,
            'range': self._validate_range,
            'length': self._validate_length,
            'enum': self._validate_enum,
            'unique': self._validate_unique
        }
    
    def validate(self, data: Any, schema: Optional[Dict] = None) -> List[ValidationResult]:
        """Validate data against schema."""
        if not schema:
            return []
        
        results = []
        
        # Handle different data types
        if isinstance(data, list):
            # Validate list of records
            for i, record in enumerate(data):
                record_results = self._validate_record(record, schema, record_id=i)
                results.extend(record_results)
        elif isinstance(data, dict):
            # Validate single record
            record_results = self._validate_record(data, schema)
            results.extend(record_results)
        else:
            results.append(ValidationResult(
                rule_name="data_type",
                field=None,
                severity="error",
                message=f"Unsupported data type for validation: {type(data)}"
            ))
        
        return results
    
    def _validate_record(self, record: Dict[str, Any], schema: Dict, record_id: Optional[Union[str, int]] = None) -> List[ValidationResult]:
        """Validate a single record against schema."""
        results = []
        
        # Get field definitions from schema
        fields = schema.get('fields', {})
        
        for field_name, field_schema in fields.items():
            value = record.get(field_name)
            
            # Apply validation rules for this field
            for rule_type, rule_config in field_schema.items():
                if rule_type in self.validators:
                    validator_func = self.validators[rule_type]
                    
                    try:
                        is_valid, message = validator_func(value, rule_config, field_name)
                        
                        if not is_valid:
                            results.append(ValidationResult(
                                rule_name=rule_type,
                                field=field_name,
                                severity=field_schema.get('severity', 'error'),
                                message=message,
                                record_id=record_id,
                                value=value
                            ))
                    except Exception as e:
                        results.append(ValidationResult(
                            rule_name=rule_type,
                            field=field_name,
                            severity="error",
                            message=f"Validation error: {e}",
                            record_id=record_id,
                            value=value
                        ))
        
        return results
    
    def _validate_required(self, value: Any, config: bool, field_name: str) -> tuple[bool, str]:
        """Validate required field."""
        if not config:
            return True, ""
        
        is_valid = value is not None and value != ""
        message = f"Field '{field_name}' is required" if not is_valid else ""
        return is_valid, message
    
    def _validate_type(self, value: Any, expected_type: str, field_name: str) -> tuple[bool, str]:
        """Validate field type."""
        if value is None:
            return True, ""  # Type validation skipped for null values
        
        type_map = {
            'string': str,
            'integer': int,
            'float': float,
            'boolean': bool,
            'datetime': (str, datetime),  # Accept string or datetime
            'date': (str, datetime)
        }
        
        expected_python_type = type_map.get(expected_type.lower())
        if expected_python_type is None:
            return False, f"Unknown type '{expected_type}' for field '{field_name}'"
        
        is_valid = isinstance(value, expected_python_type)
        message = f"Field '{field_name}' must be of type {expected_type}, got {type(value).__name__}" if not is_valid else ""
        return is_valid, message
    
    def _validate_pattern(self, value: Any, pattern: str, field_name: str) -> tuple[bool, str]:
        """Validate field against regex pattern."""
        if value is None:
            return True, ""
        
        if not isinstance(value, str):
            return False, f"Pattern validation requires string value for field '{field_name}'"
        
        try:
            is_valid = bool(re.match(pattern, value))
            message = f"Field '{field_name}' does not match required pattern" if not is_valid else ""
            return is_valid, message
        except re.error as e:
            return False, f"Invalid regex pattern for field '{field_name}': {e}"
    
    def _validate_range(self, value: Any, range_config: Dict, field_name: str) -> tuple[bool, str]:
        """Validate field value within range."""
        if value is None:
            return True, ""
        
        min_val = range_config.get('min')
        max_val = range_config.get('max')
        
        try:
            if min_val is not None and value < min_val:
                return False, f"Field '{field_name}' value {value} is below minimum {min_val}"
            
            if max_val is not None and value > max_val:
                return False, f"Field '{field_name}' value {value} is above maximum {max_val}"
            
            return True, ""
        except TypeError:
            return False, f"Cannot compare value for field '{field_name}' - incompatible types"
    
    def _validate_length(self, value: Any, length_config: Union[int, Dict], field_name: str) -> tuple[bool, str]:
        """Validate field length."""
        if value is None:
            return True, ""
        
        if isinstance(length_config, int):
            max_length = length_config
            min_length = 0
        else:
            max_length = length_config.get('max')
            min_length = length_config.get('min', 0)
        
        try:
            value_length = len(value)
            
            if min_length and value_length < min_length:
                return False, f"Field '{field_name}' length {value_length} is below minimum {min_length}"
            
            if max_length and value_length > max_length:
                return False, f"Field '{field_name}' length {value_length} is above maximum {max_length}"
            
            return True, ""
        except TypeError:
            return False, f"Cannot get length of value for field '{field_name}'"
    
    def _validate_enum(self, value: Any, allowed_values: List[Any], field_name: str) -> tuple[bool, str]:
        """Validate field value is in allowed enum values."""
        if value is None:
            return True, ""
        
        is_valid = value in allowed_values
        message = f"Field '{field_name}' value '{value}' not in allowed values: {allowed_values}" if not is_valid else ""
        return is_valid, message
    
    def _validate_unique(self, value: Any, config: bool, field_name: str) -> tuple[bool, str]:
        """Validate field uniqueness (placeholder - requires dataset context)."""
        # This would need to be implemented at the dataset level
        return True, ""


class GeometryValidator(Validator):
    """Validates spatial geometry data."""
    
    def __init__(self, spatial_ops: Optional[SpatialOperations] = None):
        self.spatial_ops = spatial_ops
    
    def validate(self, data: Any, schema: Optional[Dict] = None) -> List[ValidationResult]:
        """Validate geometry data."""
        results = []
        
        # This is a simplified implementation
        # In practice, you would validate geometries using spatial libraries
        
        if isinstance(data, dict) and 'geometry' in data:
            geometry = data['geometry']
            results.extend(self._validate_geometry(geometry))
        elif isinstance(data, list):
            for i, item in enumerate(data):
                if isinstance(item, dict) and 'geometry' in item:
                    geometry_results = self._validate_geometry(item['geometry'], record_id=i)
                    results.extend(geometry_results)
        
        return results
    
    def _validate_geometry(self, geometry: Dict, record_id: Optional[int] = None) -> List[ValidationResult]:
        """Validate a single geometry object."""
        results = []
        
        # Check geometry type
        geom_type = geometry.get('type')
        valid_types = ['Point', 'LineString', 'Polygon', 'MultiPoint', 'MultiLineString', 'MultiPolygon', 'GeometryCollection']
        
        if geom_type not in valid_types:
            results.append(ValidationResult(
                rule_name="geometry_type",
                field="geometry.type",
                severity="error",
                message=f"Invalid geometry type: {geom_type}",
                record_id=record_id,
                value=geom_type
            ))
        
        # Check coordinates
        coordinates = geometry.get('coordinates')
        if coordinates is None and geom_type != 'GeometryCollection':
            results.append(ValidationResult(
                rule_name="geometry_coordinates",
                field="geometry.coordinates",
                severity="error",
                message="Geometry missing coordinates",
                record_id=record_id
            ))
        elif coordinates is not None and geom_type is not None:
            coord_results = self._validate_coordinates(coordinates, geom_type, record_id)
            results.extend(coord_results)
        
        return results
    
    def _validate_coordinates(self, coordinates: Any, geom_type: str, record_id: Optional[int] = None) -> List[ValidationResult]:
        """Validate geometry coordinates."""
        results = []
        
        try:
            if geom_type == 'Point':
                if not isinstance(coordinates, list) or len(coordinates) < 2:
                    results.append(ValidationResult(
                        rule_name="point_coordinates",
                        field="geometry.coordinates",
                        severity="error",
                        message="Point coordinates must be [x, y] or [x, y, z]",
                        record_id=record_id,
                        value=coordinates
                    ))
            elif geom_type == 'LineString':
                if not isinstance(coordinates, list) or len(coordinates) < 2:
                    results.append(ValidationResult(
                        rule_name="linestring_coordinates",
                        field="geometry.coordinates",
                        severity="error",
                        message="LineString must have at least 2 coordinate pairs",
                        record_id=record_id,
                        value=coordinates
                    ))
            elif geom_type == 'Polygon':
                if not isinstance(coordinates, list) or not coordinates:
                    results.append(ValidationResult(
                        rule_name="polygon_coordinates",
                        field="geometry.coordinates",
                        severity="error",
                        message="Polygon coordinates must be array of linear rings",
                        record_id=record_id,
                        value=coordinates
                    ))
                else:
                    # Check that first and last points are the same for each ring
                    for i, ring in enumerate(coordinates):
                        if isinstance(ring, list) and len(ring) >= 4:
                            if ring[0] != ring[-1]:
                                results.append(ValidationResult(
                                    rule_name="polygon_closure",
                                    field=f"geometry.coordinates[{i}]",
                                    severity="error",
                                    message=f"Polygon ring {i} is not closed",
                                    record_id=record_id
                                ))
        except Exception as e:
            results.append(ValidationResult(
                rule_name="coordinate_validation",
                field="geometry.coordinates",
                severity="error",
                message=f"Error validating coordinates: {e}",
                record_id=record_id,
                value=coordinates
            ))
        
        return results


class DataQualityValidator:
    """Comprehensive data quality validation manager."""
    
    def __init__(self, spatial_ops: Optional[SpatialOperations] = None):
        self.validators: Dict[str, Validator] = {
            'schema': SchemaValidator(),
            'geometry': GeometryValidator(spatial_ops)
        }
        self.custom_rules: List[ValidationRule] = []
    
    def add_validator(self, name: str, validator: Validator):
        """Add a custom validator."""
        self.validators[name] = validator
        log.debug("Added validator: %s", name)
    
    def add_custom_rule(self, rule: ValidationRule):
        """Add a custom validation rule."""
        self.custom_rules.append(rule)
        log.debug("Added custom rule: %s", rule.name)
    
    def validate_dataset(
        self,
        data: Any,
        schema: Optional[Dict] = None,
        rules: Optional[List[ValidationRule]] = None
    ) -> ValidationSummary:
        """Validate entire dataset."""
        import time
        start_time = time.time()
        
        all_results = []
        
        # Apply schema validation
        if schema and 'schema' in self.validators:
            schema_results = self.validators['schema'].validate(data, schema)
            all_results.extend(schema_results)
        
        # Apply geometry validation
        if 'geometry' in self.validators:
            geometry_results = self.validators['geometry'].validate(data, schema)
            all_results.extend(geometry_results)
        
        # Apply custom rules
        custom_rules = rules or self.custom_rules
        for rule in custom_rules:
            rule_results = self._apply_custom_rule(data, rule)
            all_results.extend(rule_results)
        
        # Apply other validators
        for name, validator in self.validators.items():
            if name not in ['schema', 'geometry']:
                validator_results = validator.validate(data, schema)
                all_results.extend(validator_results)
        
        # Calculate summary
        total_records = len(data) if isinstance(data, list) else 1
        error_count = len([r for r in all_results if r.severity == 'error'])
        warning_count = len([r for r in all_results if r.severity == 'warning'])
        info_count = len([r for r in all_results if r.severity == 'info'])
        
        # Count records with errors
        error_record_ids = set()
        for result in all_results:
            if result.severity == 'error' and result.record_id is not None:
                error_record_ids.add(result.record_id)
        
        valid_records = total_records - len(error_record_ids)
        
        validation_time = time.time() - start_time
        
        summary = ValidationSummary(
            total_records=total_records,
            valid_records=valid_records,
            error_count=error_count,
            warning_count=warning_count,
            info_count=info_count,
            rules_applied=len(custom_rules) + len(self.validators),
            validation_time_seconds=validation_time,
            results=all_results
        )
        
        log.info("🔍 Validation completed: %d/%d records valid (%.1f%%) in %.2fs",
                valid_records, total_records, summary.validity_rate, validation_time)
        
        return summary
    
    def _apply_custom_rule(self, data: Any, rule: ValidationRule) -> List[ValidationResult]:
        """Apply a custom validation rule."""
        results = []
        
        # This is a simplified implementation
        # In practice, you would implement custom rule logic based on rule.rule_type
        
        return results
    
    def validate_spatial_dataset(self, dataset_path: str, schema: Optional[Dict] = None) -> ValidationSummary:
        """Validate a spatial dataset file."""
        try:
            # This would integrate with the spatial abstraction layer
            # to read and validate spatial datasets
            
            # For now, return a placeholder summary
            return ValidationSummary(
                total_records=0,
                valid_records=0,
                error_count=0,
                warning_count=0,
                info_count=0,
                rules_applied=0,
                validation_time_seconds=0.0,
                results=[ValidationResult(
                    rule_name="spatial_validation",
                    field=None,
                    severity="info",
                    message=f"Spatial validation not yet implemented for {dataset_path}"
                )]
            )
            
        except Exception as e:
            raise DataQualityError(f"Failed to validate spatial dataset {dataset_path}: {e}") from e


def load_schema_from_file(schema_path: Union[str, Path]) -> Dict[str, Any]:
    """Load validation schema from JSON file."""
    schema_path = Path(schema_path)
    
    if not schema_path.exists():
        raise ValidationError(f"Schema file not found: {schema_path}")
    
    try:
        with schema_path.open('r', encoding='utf-8') as f:
            schema = json.load(f)
        
        log.debug("Loaded validation schema from: %s", schema_path)
        return schema
        
    except json.JSONDecodeError as e:
        raise ValidationError(f"Invalid JSON in schema file {schema_path}: {e}") from e
    except Exception as e:
        raise ValidationError(f"Failed to load schema from {schema_path}: {e}") from e


def create_schema_from_dataset_info(dataset_info: DatasetInfo) -> Dict[str, Any]:
    """Create validation schema from dataset information."""
    schema = {
        "type": "object",
        "fields": {}
    }
    
    for field in dataset_info.fields:
        field_schema = {
            "type": _map_field_type_to_schema_type(field.type),
            "required": not field.nullable
        }
        
        if field.length:
            field_schema["length"] = {"max": field.length}
        
        schema["fields"][field.name] = field_schema
    
    return schema


def _map_field_type_to_schema_type(field_type: str) -> str:
    """Map ArcGIS field type to schema type."""
    type_mapping = {
        "String": "string",
        "Integer": "integer",
        "SmallInteger": "integer",
        "Double": "float",
        "Single": "float",
        "Date": "datetime",
        "Blob": "string",
        "Raster": "string",
        "GUID": "string",
        "GlobalID": "string",
        "XML": "string"
    }
    
    return type_mapping.get(field_type, "string")