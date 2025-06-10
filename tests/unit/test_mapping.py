"""Unit tests for etl.mapping module."""
import pytest
import tempfile
from pathlib import Path
import yaml

from etl.mapping import (
    OutputMapping,
    MappingSettings,
    MappingManager,
    get_mapping_manager
)
from etl.models import Source
from etl.exceptions import ConfigurationError, ValidationError


class TestOutputMapping:
    """Test OutputMapping dataclass."""
    
    @pytest.mark.unit
    def test_valid_mapping(self):
        mapping = OutputMapping(
            staging_fc="MSB_Stamnat",
            sde_fc="Stamnat_sodermanland",
            sde_dataset="Underlag_MSB",
            description="Test mapping"
        )
        
        assert mapping.staging_fc == "MSB_Stamnat"
        assert mapping.sde_fc == "Stamnat_sodermanland"
        assert mapping.sde_dataset == "Underlag_MSB"
        assert mapping.description == "Test mapping"
        assert mapping.enabled is True
        assert mapping.schema is None
    
    @pytest.mark.unit
    def test_empty_staging_fc(self):
        with pytest.raises(ValidationError, match="staging_fc cannot be empty"):
            OutputMapping(
                staging_fc="",
                sde_fc="test_fc",
                sde_dataset="test_dataset"
            )
    
    @pytest.mark.unit
    def test_empty_sde_fc(self):
        with pytest.raises(ValidationError, match="sde_fc cannot be empty"):
            OutputMapping(
                staging_fc="test_fc",
                sde_fc="",
                sde_dataset="test_dataset"
            )
    
    @pytest.mark.unit
    def test_empty_sde_dataset(self):
        with pytest.raises(ValidationError, match="sde_dataset cannot be empty"):
            OutputMapping(
                staging_fc="test_fc",
                sde_fc="test_fc",
                sde_dataset=""
            )


class TestMappingSettings:
    """Test MappingSettings dataclass."""
    
    @pytest.mark.unit
    def test_default_settings(self):
        settings = MappingSettings()
        
        assert settings.default_schema == "GNG"
        assert settings.default_dataset_pattern == "Underlag_{authority}"
        assert settings.default_fc_pattern == "{authority}_{source_name}"
        assert settings.validate_datasets is True
        assert settings.create_missing_datasets is True
        assert settings.skip_unmappable_sources is False


class TestMappingManager:
    """Test MappingManager functionality."""
    
    @pytest.fixture
    def temp_mappings_file(self):
        """Create temporary mappings file."""
        mappings_content = {
            'settings': {
                'default_schema': 'GNG',
                'default_dataset_pattern': 'Underlag_{authority}',
                'default_fc_pattern': '{authority}_{source_name}',
                'validate_datasets': True,
                'create_missing_datasets': True,
                'skip_unmappable_sources': False
            },
            'mappings': [
                {
                    'staging_fc': 'MSB_Stamnat',
                    'sde_fc': 'Stamnat_sodermanland',
                    'sde_dataset': 'Underlag_MSB',
                    'description': 'MSB national infrastructure'
                },
                {
                    'staging_fc': 'NVV_Naturreservat',
                    'sde_fc': 'Naturskydd_Reservat',
                    'sde_dataset': 'Underlag_Naturvard',
                    'description': 'Nature reserves',
                    'enabled': True
                },
                {
                    'staging_fc': 'DISABLED_Source',
                    'sde_fc': 'Disabled_FC',
                    'sde_dataset': 'Test_Dataset',
                    'enabled': False
                }
            ]
        }
        
        temp_file = Path(tempfile.mktemp(suffix='.yaml'))
        with temp_file.open('w') as f:
            yaml.dump(mappings_content, f)
        
        yield temp_file
        
        # Cleanup
        if temp_file.exists():
            temp_file.unlink()
    
    @pytest.mark.unit
    def test_initialization_without_file(self):
        manager = MappingManager()
        assert len(manager.mappings) == 0
        assert isinstance(manager.settings, MappingSettings)
    
    @pytest.mark.unit
    def test_load_mappings_from_file(self, temp_mappings_file):
        manager = MappingManager(temp_mappings_file)
        
        assert len(manager.mappings) == 3
        assert 'MSB_Stamnat' in manager.mappings
        assert 'NVV_Naturreservat' in manager.mappings
        assert 'DISABLED_Source' in manager.mappings
        
        msb_mapping = manager.mappings['MSB_Stamnat']
        assert msb_mapping.sde_fc == 'Stamnat_sodermanland'
        assert msb_mapping.sde_dataset == 'Underlag_MSB'
        assert msb_mapping.enabled is True
    
    @pytest.mark.unit
    def test_load_nonexistent_file(self):
        nonexistent_file = Path("nonexistent_mappings.yaml")
        manager = MappingManager(nonexistent_file)
        
        # Should not raise error, just log warning
        assert len(manager.mappings) == 0
    
    @pytest.mark.unit
    def test_get_explicit_mapping(self, temp_mappings_file):
        manager = MappingManager(temp_mappings_file)
        source = Source(name="Test Source", authority="MSB")
        
        mapping = manager.get_output_mapping(source, "MSB_Stamnat")
        
        assert mapping.staging_fc == "MSB_Stamnat"
        assert mapping.sde_fc == "Stamnat_sodermanland"
        assert mapping.sde_dataset == "Underlag_MSB"
    
    @pytest.mark.unit
    def test_get_disabled_mapping_fallback(self, temp_mappings_file):
        manager = MappingManager(temp_mappings_file)
        source = Source(name="Disabled Source", authority="TEST")
        
        # Should fall back to default logic for disabled mapping
        mapping = manager.get_output_mapping(source, "DISABLED_Source")
        
        assert mapping.staging_fc == "DISABLED_Source"
        assert mapping.sde_dataset == "Underlag_TEST"  # Default pattern
        assert mapping.description.startswith("Auto-generated")
    
    @pytest.mark.unit
    def test_create_default_mapping(self):
        manager = MappingManager()
        source = Source(name="Test Source", authority="TEST")
        
        mapping = manager._create_default_mapping(source, "TEST_Source")
        
        assert mapping.staging_fc == "TEST_Source"
        assert mapping.sde_dataset == "Underlag_TEST"
        assert mapping.sde_fc == "TEST_test_source"
        assert mapping.schema == "GNG"
    
    @pytest.mark.unit
    def test_get_full_sde_path(self):
        manager = MappingManager()
        mapping = OutputMapping(
            staging_fc="test_fc",
            sde_fc="output_fc",
            sde_dataset="test_dataset",
            schema="GNG"
        )
        
        path = manager.get_full_sde_path(mapping, "connection.sde")
        assert path == "connection.sde\\GNG.test_dataset\\output_fc"
    
    @pytest.mark.unit
    def test_get_full_sde_path_no_schema(self):
        manager = MappingManager()
        mapping = OutputMapping(
            staging_fc="test_fc",
            sde_fc="output_fc",
            sde_dataset="test_dataset"
        )
        
        path = manager.get_full_sde_path(mapping, "connection.sde")
        assert path == "connection.sde\\test_dataset\\output_fc"
    
    @pytest.mark.unit
    def test_get_dataset_path(self):
        manager = MappingManager()
        mapping = OutputMapping(
            staging_fc="test_fc",
            sde_fc="output_fc",
            sde_dataset="test_dataset",
            schema="GNG"
        )
        
        path = manager.get_dataset_path(mapping, "connection.sde")
        assert path == "connection.sde\\GNG.test_dataset"
    
    @pytest.mark.unit
    def test_get_mappings_for_dataset(self, temp_mappings_file):
        manager = MappingManager(temp_mappings_file)
        
        mappings = manager.get_mappings_for_dataset("Underlag_MSB")
        assert len(mappings) == 1
        assert mappings[0].staging_fc == "MSB_Stamnat"
    
    @pytest.mark.unit
    def test_get_all_target_datasets(self, temp_mappings_file):
        manager = MappingManager(temp_mappings_file)
        
        datasets = manager.get_all_target_datasets()
        
        # Should include enabled mappings only
        assert "GNG.Underlag_MSB" in datasets
        assert "GNG.Underlag_Naturvard" in datasets
        # Disabled mapping should not be included
        assert "GNG.Test_Dataset" not in datasets
    
    @pytest.mark.unit
    def test_validate_mapping_valid(self):
        manager = MappingManager()
        mapping = OutputMapping(
            staging_fc="Valid_FC",
            sde_fc="Valid_Output",
            sde_dataset="Valid_Dataset"
        )
        
        issues = manager.validate_mapping(mapping)
        assert len(issues) == 0
    
    @pytest.mark.unit
    def test_validate_mapping_invalid_characters(self):
        manager = MappingManager()
        mapping = OutputMapping(
            staging_fc="Invalid@FC",
            sde_fc="Invalid#Output",
            sde_dataset="Invalid$Dataset"
        )
        
        issues = manager.validate_mapping(mapping)
        assert len(issues) == 3
        assert any("invalid characters" in issue for issue in issues)
    
    @pytest.mark.unit
    def test_validate_mapping_too_long(self):
        manager = MappingManager()
        long_name = "a" * 150  # Exceeds 128 character limit
        
        mapping = OutputMapping(
            staging_fc="Valid_FC",
            sde_fc=long_name,
            sde_dataset=long_name
        )
        
        issues = manager.validate_mapping(mapping)
        assert len(issues) == 2
        assert any("exceeds 128 character limit" in issue for issue in issues)
    
    @pytest.mark.unit
    def test_add_mapping(self):
        manager = MappingManager()
        mapping = OutputMapping(
            staging_fc="New_FC",
            sde_fc="New_Output",
            sde_dataset="New_Dataset"
        )
        
        manager.add_mapping(mapping)
        
        assert "New_FC" in manager.mappings
        assert manager.mappings["New_FC"] == mapping
    
    @pytest.mark.unit
    def test_add_invalid_mapping(self):
        manager = MappingManager()
        mapping = OutputMapping(
            staging_fc="",  # Invalid
            sde_fc="Valid_Output",
            sde_dataset="Valid_Dataset"
        )
        
        with pytest.raises(ValidationError):
            manager.add_mapping(mapping)
    
    @pytest.mark.unit
    def test_remove_mapping(self, temp_mappings_file):
        manager = MappingManager(temp_mappings_file)
        
        assert "MSB_Stamnat" in manager.mappings
        
        result = manager.remove_mapping("MSB_Stamnat")
        assert result is True
        assert "MSB_Stamnat" not in manager.mappings
        
        # Try removing non-existent mapping
        result = manager.remove_mapping("NonExistent")
        assert result is False
    
    @pytest.mark.unit
    def test_get_mapping_statistics(self, temp_mappings_file):
        manager = MappingManager(temp_mappings_file)
        
        stats = manager.get_mapping_statistics()
        
        assert stats['total_mappings'] == 3
        assert stats['enabled_mappings'] == 2  # One is disabled
        assert stats['disabled_mappings'] == 1
        assert 'Underlag_MSB' in stats['datasets']
        assert 'Underlag_Naturvard' in stats['datasets']
    
    @pytest.mark.unit
    def test_save_mappings(self, temp_mappings_file):
        manager = MappingManager(temp_mappings_file)
        
        # Add a new mapping
        new_mapping = OutputMapping(
            staging_fc="Test_FC",
            sde_fc="Test_Output",
            sde_dataset="Test_Dataset",
            description="Test mapping"
        )
        manager.add_mapping(new_mapping)
        
        # Save to new file
        output_file = Path(tempfile.mktemp(suffix='.yaml'))
        try:
            manager.save_mappings(output_file)
            
            # Verify file was created and contains expected data
            assert output_file.exists()
            
            with output_file.open('r') as f:
                saved_data = yaml.safe_load(f)
            
            assert 'settings' in saved_data
            assert 'mappings' in saved_data
            assert len(saved_data['mappings']) == 4  # 3 original + 1 new
            
        finally:
            if output_file.exists():
                output_file.unlink()
    
    @pytest.mark.unit
    def test_load_invalid_yaml(self):
        invalid_file = Path(tempfile.mktemp(suffix='.yaml'))
        invalid_file.write_text("invalid: yaml: content: [")
        
        try:
            with pytest.raises(ConfigurationError, match="Invalid YAML"):
                MappingManager(invalid_file)
        finally:
            if invalid_file.exists():
                invalid_file.unlink()


class TestGlobalMappingManager:
    """Test global mapping manager functions."""
    
    @pytest.mark.unit
    def test_get_mapping_manager_singleton(self):
        # Clear global instance
        import etl.mapping
        etl.mapping._mapping_manager = None
        
        manager1 = get_mapping_manager()
        manager2 = get_mapping_manager()
        
        assert manager1 is manager2  # Should be same instance
    
    @pytest.mark.unit
    def test_get_mapping_manager_with_file(self, temp_mappings_file):
        # Clear global instance
        import etl.mapping
        etl.mapping._mapping_manager = None
        
        manager = get_mapping_manager(temp_mappings_file)
        
        assert len(manager.mappings) > 0
        assert manager.mappings_file == temp_mappings_file