# ETL Pipeline Mapping System

## Overview

The ETL pipeline includes a flexible mapping system that allows custom naming and organization of output feature classes and datasets in the production SDE geodatabase. This system provides granular control over where and how data is stored while maintaining backward compatibility with existing naming logic.

## Key Benefits

✅ **Flexible Naming**: Override default naming patterns for specific sources  
✅ **Dataset Organization**: Group related feature classes into logical SDE datasets  
✅ **Swedish Naming Support**: Handle Swedish characters and naming conventions  
✅ **Backward Compatibility**: Unmapped sources continue to use existing logic  
✅ **Optional Configuration**: System works with or without mapping files  
✅ **Validation**: Built-in validation for SDE naming constraints  

## Configuration Files

### mappings.yaml Structure

The mapping system uses a separate `config/mappings.yaml` file:

```yaml
# Global mapping settings
settings:
  default_schema: "GNG"
  default_dataset_pattern: "Underlag_{authority}"
  default_fc_pattern: "{authority}_{source_name}"
  validate_datasets: true
  create_missing_datasets: true
  skip_unmappable_sources: false

# Individual mappings
mappings:
  - staging_fc: "MSB_Stamnat"
    sde_fc: "Stamnat_sodermanland"
    sde_dataset: "Underlag_MSB"
    description: "MSB national infrastructure data for Södermanland"
  
  - staging_fc: "NVV_Naturreservat"
    sde_fc: "Naturskydd_Reservat"
    sde_dataset: "Underlag_Naturvard"
    description: "Nature reserves from Naturvårdsverket"
    enabled: true
    schema: "GNG"
```

### Field Descriptions

#### Settings Section
- `default_schema`: Default SDE schema prefix (default: "GNG")
- `default_dataset_pattern`: Pattern for dataset names when no mapping exists
- `default_fc_pattern`: Pattern for feature class names when no mapping exists
- `validate_datasets`: Whether to validate dataset existence before loading
- `create_missing_datasets`: Whether to create missing datasets automatically
- `skip_unmappable_sources`: Whether to skip sources with invalid mappings

#### Mappings Section
- `staging_fc`: Name of feature class in staging.gdb (required)
- `sde_fc`: Target feature class name in SDE (required)
- `sde_dataset`: Target SDE dataset name (required)
- `description`: Human-readable description (optional)
- `enabled`: Whether this mapping is active (optional, default: true)
- `schema`: SDE schema override (optional, uses default_schema if not specified)

## Usage Examples

### Basic Usage

The mapping system is automatically loaded when the `config/mappings.yaml` file exists:

```bash
# Uses default mappings.yaml if it exists
python run_etl.py

# Specify custom mappings file
python run_etl.py sources.yaml config.yaml custom_mappings.yaml
```

### Integration in Code

```python
from etl.mapping import get_mapping_manager, OutputMapping
from etl.models import Source

# Get mapping manager
mapping_manager = get_mapping_manager()

# Get mapping for a source
source = Source(name="Test Source", authority="MSB")
mapping = mapping_manager.get_output_mapping(source, "MSB_Stamnat")

print(f"Target: {mapping.sde_dataset}.{mapping.sde_fc}")
# Output: Target: Underlag_MSB.Stamnat_sodermanland
```

### SDE Loader Integration

```python
from etl.loaders.sde_loader import SDELoader

# Initialize SDE loader with mappings
loader = SDELoader(
    sde_connection="connection.sde",
    mapping_manager=mapping_manager,
    global_config=config
)

# Load feature class using mappings
result_path = loader.load_feature_class(
    source=source,
    staging_fc_path="staging.gdb/MSB_Stamnat",
    staging_fc_name="MSB_Stamnat"
)
```

## Naming Logic

### With Explicit Mapping
When a staging feature class has an explicit mapping:

```
staging.gdb/MSB_Stamnat → connection.sde/GNG.Underlag_MSB/Stamnat_sodermanland
```

### Without Mapping (Default Logic)
When no mapping exists, uses pattern-based naming:

```
Source: authority="MSB", name="National Infrastructure"
staging.gdb/MSB_Infrastructure → connection.sde/GNG.Underlag_MSB/MSB_national_infrastructure
```

### Pattern Variables
Available variables for default patterns:
- `{authority}`: Source authority (e.g., "MSB", "NVV")
- `{source_name}`: Sanitized source name
- `{staging_fc}`: Staging feature class name

## Real-World Examples

### Swedish Authority Mappings

```yaml
mappings:
  # Myndigheten för samhällsskydd och beredskap
  - staging_fc: "MSB_Stamnat"
    sde_fc: "Infrastruktur_Stamnat"
    sde_dataset: "Underlag_MSB"
    description: "National infrastructure from MSB"
  
  # Naturvårdsverket
  - staging_fc: "NVV_Naturreservat"
    sde_fc: "Skydd_Naturreservat"
    sde_dataset: "Underlag_Naturvard"
    description: "Nature reserves"
  
  - staging_fc: "NVV_Nationalparker"
    sde_fc: "Skydd_Nationalparker" 
    sde_dataset: "Underlag_Naturvard"
    description: "National parks"
  
  # Sveriges geologiska undersökning
  - staging_fc: "SGU_Berggrund_50k"
    sde_fc: "Geologi_Berggrund"
    sde_dataset: "Underlag_Geologi"
    description: "Bedrock geology 1:50,000"
  
  # Regional customizations
  - staging_fc: "REGION_Detaljplaner"
    sde_fc: "Plan_Detaljplan_Regional"
    sde_dataset: "Underlag_Planering"
    description: "Regional detailed plans"
```

### Dataset Organization

This mapping strategy creates a logical organization:

```
SDE Geodatabase
├── GNG.Underlag_MSB/
│   ├── Infrastruktur_Stamnat
│   └── Riskzoner_MSB
├── GNG.Underlag_Naturvard/
│   ├── Skydd_Naturreservat
│   ├── Skydd_Nationalparker
│   └── Biotopskydd_Omraden
├── GNG.Underlag_Geologi/
│   ├── Geologi_Berggrund
│   └── Geologi_Jordarter
└── GNG.Underlag_Planering/
    ├── Plan_Detaljplan_Regional
    └── Plan_Oversikt_Kommunal
```

## Validation and Error Handling

### Automatic Validation
The system validates mappings for:
- Empty required fields
- Invalid characters for SDE naming
- Name length limits (128 characters)
- Schema and dataset existence

### Error Handling Strategies
```yaml
settings:
  # Create missing datasets automatically
  create_missing_datasets: true
  
  # Skip sources with mapping issues
  skip_unmappable_sources: false
  
  # Validate before loading
  validate_datasets: true
```

### Validation Example
```python
# Validate all mappings
issues = mapping_manager.validate_all_mappings()
for staging_fc, errors in issues.items():
    print(f"Issues with {staging_fc}: {errors}")

# Validate specific mapping
mapping = OutputMapping(
    staging_fc="Test_FC",
    sde_fc="Invalid@Name",  # Invalid character
    sde_dataset="Test_Dataset"
)
issues = mapping_manager.validate_mapping(mapping)
# Returns: ["SDE FC name 'Invalid@Name' contains invalid characters"]
```

## Management Operations

### Adding Mappings Programmatically
```python
# Add new mapping
new_mapping = OutputMapping(
    staging_fc="CUSTOM_Data",
    sde_fc="Custom_Output",
    sde_dataset="Custom_Dataset",
    description="Custom data mapping"
)
mapping_manager.add_mapping(new_mapping)

# Save to file
mapping_manager.save_mappings()
```

### Statistics and Monitoring
```python
# Get mapping statistics
stats = mapping_manager.get_mapping_statistics()
print(f"Total mappings: {stats['total_mappings']}")
print(f"Enabled mappings: {stats['enabled_mappings']}")
print(f"Target datasets: {stats['datasets']}")

# Get mappings for specific dataset
dataset_mappings = mapping_manager.get_mappings_for_dataset("Underlag_MSB")
```

## Command Line Usage

### Basic Usage
```bash
# Use default configuration (mappings.yaml automatically loaded if exists)
python run_etl.py

# Specify all configuration files
python run_etl.py sources.yaml config.yaml mappings.yaml

# Run without mappings (uses default naming logic)
python run_etl.py sources.yaml config.yaml
```

### Environment Variables
```bash
# Override mapping file location
export ETL_MAPPINGS_FILE="/path/to/custom/mappings.yaml"
python run_etl.py
```

## Best Practices

### 1. Naming Conventions
- Use consistent prefixes for related datasets
- Keep feature class names descriptive but concise
- Follow Swedish naming conventions where appropriate

### 2. Dataset Organization
- Group related feature classes into logical datasets
- Use authority-based datasets (e.g., `Underlag_MSB`, `Underlag_NVV`)
- Consider data usage patterns when organizing

### 3. Maintenance
- Document mapping purposes in the description field
- Regular validation of mappings
- Version control mapping files alongside source configurations

### 4. Testing
- Test mappings in development environment first
- Validate dataset creation before production deployment
- Monitor loading performance with different strategies

## Troubleshooting

### Common Issues

**Mapping Not Applied**
- Check that `staging_fc` name matches exactly
- Ensure mapping is enabled (`enabled: true`)
- Verify mappings.yaml syntax

**Dataset Creation Fails**
- Check SDE permissions for dataset creation
- Verify spatial reference configuration
- Ensure schema exists in SDE

**Invalid Naming**
- Use validation functions to check naming rules
- Avoid special characters in SDE names
- Keep names under 128 characters

### Debug Commands
```python
# Check if mapping exists
mapping_manager = get_mapping_manager()
if "MSB_Stamnat" in mapping_manager.mappings:
    print("Mapping found")

# Validate specific mapping
issues = mapping_manager.validate_mapping(mapping)
if issues:
    print(f"Validation issues: {issues}")
```

## Migration from Legacy System

For existing installations, the mapping system provides seamless migration:

1. **Immediate Compatibility**: Existing sources work unchanged
2. **Gradual Migration**: Add mappings for specific sources as needed
3. **Validation Tools**: Check existing naming against new patterns
4. **Rollback Support**: Disable mappings.yaml to revert to original behavior

This design ensures that the mapping system enhances rather than disrupts existing ETL workflows while providing powerful customization capabilities for Swedish municipal and governmental data systems.