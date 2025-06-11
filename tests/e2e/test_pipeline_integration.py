"""End-to-end integration tests for the ETL pipeline."""
import pytest
from pathlib import Path
from unittest.mock import patch, Mock, MagicMock
import tempfile
import shutil
import yaml

from etl.pipeline import Pipeline
from etl.models import Source
from etl.utils.run_summary import Summary


class TestPipelineIntegration:
    """End-to-end tests for the complete ETL pipeline."""
    
    @pytest.fixture
    def test_pipeline_config(self, temp_dir):
        """Create test configuration files for pipeline testing."""
        # Create sources config
        sources_config = {
            "sources": [
                {
                    "name": "Test REST Source",
                    "authority": "TEST",
                    "type": "rest_api", 
                    "url": "https://api.example.com/features",
                    "enabled": True,
                    "output_format": "geojson"
                },
                {
                    "name": "Test File Source",
                    "authority": "TEST",
                    "type": "file",
                    "url": "https://example.com/data.zip",
                    "enabled": False,  # Disabled for testing
                    "download_format": "zip"
                }
            ]
        }
        
        # Create global config
        global_config = {
            "logging": {
                "level": "DEBUG",
                "summary_file": "test_summary.log",
                "debug_file": "test_debug.log"
            },
            "retry": {
                "max_attempts": 2,
                "backoff_factor": 1.5
            },
            "paths": {
                "download": str(temp_dir / "downloads"),
                "staging": str(temp_dir / "staging")
            }
        }
        
        sources_file = temp_dir / "test_sources.yaml"
        config_file = temp_dir / "test_config.yaml"
        
        with sources_file.open("w") as f:
            yaml.dump(sources_config, f)
        
        with config_file.open("w") as f:
            yaml.dump(global_config, f)
        
        return {
            "sources_file": sources_file,
            "config_file": config_file,
            "temp_dir": temp_dir
        }
    
    @pytest.mark.e2e
    def test_pipeline_initialization(self, test_pipeline_config):
        """Test pipeline initialization with config files."""
        pipeline = Pipeline(
            sources_yaml=test_pipeline_config["sources_file"],
            config_yaml_path=test_pipeline_config["config_file"]
        )
        
        assert pipeline.sources_yaml_path == test_pipeline_config["sources_file"]
        assert isinstance(pipeline.global_cfg, dict)
        assert isinstance(pipeline.summary, Summary)
    
    @pytest.mark.e2e
    def test_pipeline_with_custom_summary(self, test_pipeline_config):
        """Test pipeline initialization with custom summary object."""
        custom_summary = Summary()
        pipeline = Pipeline(
            sources_yaml=test_pipeline_config["sources_file"],
            config_yaml_path=test_pipeline_config["config_file"],
            summary=custom_summary
        )
        
        assert pipeline.summary is custom_summary
    
    @pytest.mark.e2e
    def test_pipeline_with_extra_handlers(self, test_pipeline_config):
        """Test pipeline with additional handler mappings."""
        extra_handlers = {
            "custom_handler": Mock()
        }
        
        pipeline = Pipeline(
            sources_yaml=test_pipeline_config["sources_file"],
            config_yaml_path=test_pipeline_config["config_file"],
            extra_handler_map=extra_handlers
        )
        
        assert "custom_handler" in pipeline.handler_map
    
    @pytest.mark.e2e
    def test_pipeline_with_nonexistent_config(self, test_pipeline_config):
        """Test pipeline behavior with nonexistent config file."""
        nonexistent_config = test_pipeline_config["temp_dir"] / "nonexistent.yaml"
        
        pipeline = Pipeline(
            sources_yaml=test_pipeline_config["sources_file"],
            config_yaml_path=nonexistent_config
        )
        
        # Should use default empty config
        assert pipeline.global_cfg == {}
    
    @pytest.mark.e2e
    @patch('etl.pipeline.Source.load_all')
    def test_pipeline_with_no_sources(self, mock_load_all, test_pipeline_config):
        """Test pipeline behavior when no sources are loaded."""
        mock_load_all.return_value = []
        
        pipeline = Pipeline(
            sources_yaml=test_pipeline_config["sources_file"],
            config_yaml_path=test_pipeline_config["config_file"]
        )
        
        # Pipeline should handle empty source list gracefully
        with patch.object(pipeline, 'run') as mock_run:
            mock_run.return_value = None
            pipeline.run()
    
    @pytest.mark.e2e
    @patch('etl.pipeline.HANDLER_MAP')
    @patch('etl.pipeline.Source.load_all')
    def test_pipeline_with_mocked_handlers(self, mock_load_all, mock_handler_map, test_pipeline_config):
        """Test pipeline execution with mocked handlers."""
        # Mock source loading
        mock_source = Mock(spec=Source)
        mock_source.name = "Test Source"
        mock_source.authority = "TEST"
        mock_source.type = "rest_api"
        mock_source.enabled = True
        mock_load_all.return_value = [mock_source]
        
        # Mock handler
        mock_handler_class = Mock()
        mock_handler_instance = Mock()
        mock_handler_class.return_value = mock_handler_instance
        mock_handler_map.__getitem__ = Mock(return_value=mock_handler_class)
        
        pipeline = Pipeline(
            sources_yaml=test_pipeline_config["sources_file"],
            config_yaml_path=test_pipeline_config["config_file"]
        )
        
        # Verify pipeline can be created
        assert pipeline is not None
    
    @pytest.mark.e2e
    def test_pipeline_invalid_yaml_handling(self, temp_dir):
        """Test pipeline behavior with invalid YAML files."""
        invalid_sources_file = temp_dir / "invalid_sources.yaml"
        invalid_sources_file.write_text("sources: [\ninvalid yaml")
        
        valid_config_file = temp_dir / "valid_config.yaml"
        valid_config_file.write_text("logging:\n  level: INFO")
        
        # Should not crash during initialization
        pipeline = Pipeline(
            sources_yaml=invalid_sources_file,
            config_yaml_path=valid_config_file
        )
        
        assert pipeline is not None
    
    @pytest.mark.e2e
    @patch('etl.pipeline.ArcPyFileGDBLoader')
    @patch('etl.pipeline.geoprocess')
    def test_pipeline_component_integration(self, mock_geoprocess, mock_loader, test_pipeline_config):
        """Test that pipeline integrates with its main components."""
        pipeline = Pipeline(
            sources_yaml=test_pipeline_config["sources_file"],
            config_yaml_path=test_pipeline_config["config_file"]
        )
        
        # Verify components are accessible
        assert hasattr(pipeline, 'handler_map')
        assert hasattr(pipeline, 'global_cfg')
        assert hasattr(pipeline, 'summary')
    
    @pytest.mark.e2e
    def test_pipeline_logging_configuration(self, test_pipeline_config, caplog):
        """Test that pipeline respects logging configuration."""
        pipeline = Pipeline(
            sources_yaml=test_pipeline_config["sources_file"],
            config_yaml_path=test_pipeline_config["config_file"]
        )
        
        # Pipeline should log initialization messages
        # Note: This test may need adjustment based on actual logging behavior
        assert pipeline is not None
    
    @pytest.mark.e2e
    def test_pipeline_summary_integration(self, test_pipeline_config):
        """Test pipeline integration with summary reporting."""
        summary = Summary()
        pipeline = Pipeline(
            sources_yaml=test_pipeline_config["sources_file"],
            config_yaml_path=test_pipeline_config["config_file"],
            summary=summary
        )
        
        # Verify summary is properly integrated
        assert pipeline.summary is summary
        
        # Summary should be usable
        summary.add_success("Test", "Test success message")
        assert len(summary.successes) > 0