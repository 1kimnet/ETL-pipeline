"""Unit tests for etl.models module."""
import pytest
from pathlib import Path
import tempfile
import yaml

from etl.models import Source, _parse_include


class TestParseInclude:
    """Test the _parse_include helper function."""
    
    @pytest.mark.unit
    def test_parse_include_none(self):
        assert _parse_include(None) == []
    
    @pytest.mark.unit
    def test_parse_include_empty_list(self):
        assert _parse_include([]) == []
    
    @pytest.mark.unit
    def test_parse_include_string(self):
        assert _parse_include("item1") == ["item1"]
    
    @pytest.mark.unit
    def test_parse_include_semicolon_separated(self):
        result = _parse_include("item1;item2;item3")
        assert result == ["item1", "item2", "item3"]
    
    @pytest.mark.unit
    def test_parse_include_comma_separated(self):
        result = _parse_include("item1,item2,item3")
        assert result == ["item1", "item2", "item3"]
    
    @pytest.mark.unit
    def test_parse_include_mixed_separators(self):
        result = _parse_include("item1;item2,item3")
        assert result == ["item1", "item2", "item3"]
    
    @pytest.mark.unit
    def test_parse_include_with_trailing_periods(self):
        result = _parse_include("item1.;item2.,item3.")
        assert result == ["item1", "item2", "item3"]
    
    @pytest.mark.unit
    def test_parse_include_with_whitespace(self):
        result = _parse_include("  item1  ; item2  ,  item3  ")
        assert result == ["item1", "item2", "item3"]
    
    @pytest.mark.unit
    def test_parse_include_list_input(self):
        result = _parse_include(["item1", "item2;item3", "item4,item5"])
        assert result == ["item1", "item2", "item3", "item4", "item5"]


class TestSource:
    """Test the Source dataclass."""
    
    @pytest.mark.unit
    def test_source_defaults(self):
        source = Source(name="Test", authority="TEST")
        assert source.name == "Test"
        assert source.authority == "TEST"
        assert source.type == "file"
        assert source.url == ""
        assert source.enabled is True
        assert source.download_format is None
        assert source.staged_data_type is None
        assert source.include == []
        assert source.raw == {}
    
    @pytest.mark.unit
    def test_source_from_dict_basic(self):
        data = {
            "name": "Test Source",
            "authority": "TEST",
            "type": "rest_api",
            "url": "https://example.com",
            "enabled": False
        }
        source = Source.from_dict(data)
        assert source.name == "Test Source"
        assert source.authority == "TEST"
        assert source.type == "rest_api"
        assert source.url == "https://example.com"
        assert source.enabled is False
    
    @pytest.mark.unit
    def test_source_from_dict_with_include(self):
        data = {
            "name": "Test",
            "authority": "TEST",
            "include": "item1;item2,item3"
        }
        source = Source.from_dict(data)
        assert source.include == ["item1", "item2", "item3"]
    
    @pytest.mark.unit
    def test_source_from_dict_with_raw_dict(self):
        data = {
            "name": "Test",
            "authority": "TEST",
            "raw": {"custom_param": "value", "timeout": 30}
        }
        source = Source.from_dict(data)
        assert source.raw == {"custom_param": "value", "timeout": 30}
    
    @pytest.mark.unit
    def test_source_from_dict_with_unknown_fields(self):
        data = {
            "name": "Test",
            "authority": "TEST",
            "custom_field": "custom_value",
            "another_field": 42
        }
        source = Source.from_dict(data)
        assert source.raw == {"custom_field": "custom_value", "another_field": 42}
    
    @pytest.mark.unit
    def test_source_from_dict_combined_raw_and_unknown(self):
        data = {
            "name": "Test",
            "authority": "TEST",
            "raw": {"explicit_raw": "value"},
            "unknown_field": "unknown_value"
        }
        source = Source.from_dict(data)
        expected_raw = {"explicit_raw": "value", "unknown_field": "unknown_value"}
        assert source.raw == expected_raw
    
    @pytest.mark.unit
    def test_load_all_empty_file(self, temp_dir):
        """Test loading from empty YAML file."""
        yaml_file = temp_dir / "empty.yaml"
        yaml_file.write_text("")
        
        sources = Source.load_all(yaml_file)
        assert sources == []
    
    @pytest.mark.unit
    def test_load_all_missing_sources_key(self, temp_dir):
        """Test loading from YAML without 'sources' key."""
        yaml_file = temp_dir / "no_sources.yaml"
        yaml_file.write_text("other_key: value")
        
        sources = Source.load_all(yaml_file)
        assert sources == []
    
    @pytest.mark.unit
    def test_load_all_invalid_sources_type(self, temp_dir):
        """Test loading when sources is not a list."""
        yaml_file = temp_dir / "invalid_sources.yaml"
        yaml_file.write_text("sources: not_a_list")
        
        sources = Source.load_all(yaml_file)
        assert sources == []
    
    @pytest.mark.unit
    def test_load_all_valid_sources(self, temp_dir):
        """Test loading valid sources."""
        yaml_content = """
sources:
  - name: "Source 1"
    authority: "AUTH1"
    type: "rest_api"
    url: "https://example1.com"
    enabled: true
  - name: "Source 2"
    authority: "AUTH2"
    type: "file"
    url: "https://example2.com"
    enabled: false
    include: "item1;item2"
        """
        yaml_file = temp_dir / "valid_sources.yaml"
        yaml_file.write_text(yaml_content)
        
        sources = Source.load_all(yaml_file)
        assert len(sources) == 2
        
        assert sources[0].name == "Source 1"
        assert sources[0].authority == "AUTH1"
        assert sources[0].type == "rest_api"
        assert sources[0].enabled is True
        
        assert sources[1].name == "Source 2"
        assert sources[1].authority == "AUTH2"
        assert sources[1].type == "file"
        assert sources[1].enabled is False
        assert sources[1].include == ["item1", "item2"]
    
    @pytest.mark.unit
    def test_load_all_nonexistent_file(self):
        """Test loading from nonexistent file."""
        sources = Source.load_all("nonexistent.yaml")
        assert sources == []
    
    @pytest.mark.unit
    def test_load_all_invalid_yaml(self, temp_dir):
        """Test loading from invalid YAML."""
        yaml_file = temp_dir / "invalid.yaml"
        yaml_file.write_text("sources: [\ninvalid yaml")
        
        sources = Source.load_all(yaml_file)
        assert sources == []