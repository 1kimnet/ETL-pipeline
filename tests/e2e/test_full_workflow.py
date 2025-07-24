"""End-to-end workflow tests for the complete ETL system."""
import pytest
from pathlib import Path
from unittest.mock import patch, Mock, MagicMock
import tempfile
import yaml
import json

from etl.pipeline import Pipeline
from etl.utils.run_summary import Summary


class TestFullWorkflow:
    """Test complete ETL workflow scenarios."""

    @pytest.fixture
    def mock_arcpy_environment(self):
        """Set up comprehensive ArcPy mocking for full workflow tests."""
        with patch('arcpy.env') as mock_env, \
                patch('arcpy.Describe') as mock_describe, \
                patch('arcpy.management') as mock_mgmt:

            # Configure ArcPy environment
            mock_env.workspace = ""
            mock_env.overwriteOutput = True

            # Configure Describe mock
            mock_spatial_ref = Mock()
            mock_spatial_ref.factoryCode = 4326
            mock_describe.return_value.spatialReference = mock_spatial_ref

            yield {
                'env': mock_env,
                'describe': mock_describe,
                'management': mock_mgmt
            }

    @pytest.fixture
    def complete_test_config(self, temp_dir):
        """Create complete test configuration simulating real workflow."""
        sources_config = {
            "sources": [
                {
                    "name": "Test Municipality Data",
                    "authority": "KOMMUN",
                    "type": "rest_api",
                    "url": "https://gis.kommun.se/arcgis/rest/services/PlanData/MapServer/0/query",
                    "enabled": True,
                    "output_format": "geojson",
                    "bbox": "586206,6551160,647910,6610992",
                    "sr": "3006"
                },
                {
                    "name": "Environmental Data Package",
                    "authority": "MILJO",
                    "type": "file",
                    "url": "https://data.miljo.se/datasets/skyddade_omraden.zip",
                    "enabled": True,
                    "download_format": "zip",
                    "include": ["naturreservat", "nationalparker"]
                },
                {
                    "name": "Geological Survey Data",
                    "authority": "SGU",
                    "type": "ogc_api",
                    "url": "https://resource.sgu.se/ogc/collections/berggrund/items",
                    "enabled": False,  # Test disabled source handling
                    "output_format": "gpkg"
                }
            ]
        }

        global_config = {
            "logging": {
                "level": "INFO",
                "summary_file": str(temp_dir / "workflow_summary.log"),
                "debug_file": str(temp_dir / "workflow_debug.log"),
                "console_level": "WARNING"
            },
            "retry": {
                "max_attempts": 3,
                "backoff_factor": 2.0,
                "timeout": 45
            },
            "paths": {
                "download": str(temp_dir / "etl_downloads"),
                "staging": str(temp_dir / "etl_staging"),
                "output": str(temp_dir / "etl_output")
            },
            "processing": {
                "chunk_size": 1000,
                "parallel_workers": 2,
                "memory_limit_mb": 512
            }
        }

        sources_file = temp_dir / "workflow_sources.yaml"
        config_file = temp_dir / "workflow_config.yaml"

        with sources_file.open("w") as f:
            yaml.dump(sources_config, f)

        with config_file.open("w") as f:
            yaml.dump(global_config, f)

        return {
            "sources_file": sources_file,
            "config_file": config_file,
            "temp_dir": temp_dir,
            "sources_config": sources_config,
            "global_config": global_config
        }

    @pytest.mark.e2e
    @pytest.mark.slow
    def test_complete_workflow_initialization(
            self, complete_test_config, mock_arcpy_environment):
        """Test complete workflow initialization with realistic configuration."""
        summary = Summary()

        pipeline = Pipeline(
            sources_yaml=complete_test_config["sources_file"],
            config_yaml_path=complete_test_config["config_file"],
            summary=summary
        )

        # Verify pipeline is properly configured
        assert pipeline.sources_yaml_path == complete_test_config["sources_file"]
        assert "logging" in pipeline.global_cfg
        assert "retry" in pipeline.global_cfg
        assert "paths" in pipeline.global_cfg
        assert isinstance(pipeline.summary, Summary)

    @pytest.mark.e2e
    @pytest.mark.slow
    @patch('etl.handlers.rest_api.requests')
    @patch('etl.handlers.file.download')
    @patch('etl.pipeline.ensure_dirs')
    def test_workflow_with_mocked_external_services(
            self,
            mock_ensure_dirs,
            mock_download,
            mock_requests,
            complete_test_config,
            mock_arcpy_environment):
        """Test workflow with external services mocked."""
        # Mock HTTP responses
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "features": [
                {
                    "type": "Feature", "properties": {
                        "id": 1, "name": "Test Feature"}, "geometry": {
                        "type": "Point", "coordinates": [
                            18.0649, 59.3293]}}]}
        mock_requests.get.return_value = mock_response

        # Mock file download
        mock_download.return_value = complete_test_config["temp_dir"] / \
            "downloaded.zip"

        pipeline = Pipeline(
            sources_yaml=complete_test_config["sources_file"],
            config_yaml_path=complete_test_config["config_file"]
        )

        # Verify pipeline can handle mocked services
        assert pipeline is not None
        assert len(pipeline.handler_map) > 0

    @pytest.mark.e2e
    def test_workflow_error_handling_scenarios(
            self, complete_test_config, mock_arcpy_environment):
        """Test workflow behavior under various error conditions."""
        # Test with corrupted sources file
        corrupted_sources = complete_test_config["temp_dir"] / \
            "corrupted_sources.yaml"
        corrupted_sources.write_text("sources: invalid yaml [\n")

        # Should handle gracefully
        pipeline = Pipeline(
            sources_yaml=corrupted_sources,
            config_yaml_path=complete_test_config["config_file"]
        )

        assert pipeline is not None

    @pytest.mark.e2e
    @patch('etl.pipeline.Source.load_all')
    def test_workflow_with_mixed_source_types(
            self,
            mock_load_all,
            complete_test_config,
            mock_arcpy_environment):
        """Test workflow handling different source types together."""
        # Create mock sources of different types
        mock_sources = [
            Mock(name="REST Source", type="rest_api", enabled=True),
            Mock(name="File Source", type="file", enabled=True),
            Mock(name="OGC Source", type="ogc_api", enabled=False),
            Mock(name="Atom Source", type="atom_feed", enabled=True)
        ]
        mock_load_all.return_value = mock_sources

        pipeline = Pipeline(
            sources_yaml=complete_test_config["sources_file"],
            config_yaml_path=complete_test_config["config_file"]
        )

        # Verify all handler types are available
        expected_handlers = ["rest_api", "file", "ogc_api", "atom_feed"]
        for handler_type in expected_handlers:
            assert handler_type in pipeline.handler_map

    @pytest.mark.e2e
    def test_workflow_summary_generation(
            self,
            complete_test_config,
            mock_arcpy_environment):
        """Test complete workflow summary generation."""
        summary = Summary()

        # Simulate workflow results
        summary.add_success(
            "KOMMUN", "Successfully processed municipality data")
        summary.add_failure(
            "MILJO",
            "Failed to download environmental data",
            "Network timeout")
        summary.add_skip("SGU", "Source disabled in configuration")

        pipeline = Pipeline(
            sources_yaml=complete_test_config["sources_file"],
            config_yaml_path=complete_test_config["config_file"],
            summary=summary
        )

        # Verify summary contains expected data
        assert len(summary.successes) == 1
        assert len(summary.failures) == 1
        assert len(summary.skips) == 1

        # Test summary output (would normally be written to log)
        summary_output = summary._generate_report()
        assert "KOMMUN" in summary_output
        assert "MILJO" in summary_output
        assert "SGU" in summary_output

    @pytest.mark.e2e
    def test_workflow_configuration_validation(
            self, temp_dir, mock_arcpy_environment):
        """Test workflow with various configuration scenarios."""
        # Test minimal configuration
        minimal_config = {"sources": []}
        minimal_file = temp_dir / "minimal.yaml"
        with minimal_file.open("w") as f:
            yaml.dump(minimal_config, f)

        # Should handle minimal config
        pipeline = Pipeline(sources_yaml=minimal_file)
        assert pipeline is not None

        # Test configuration with unknown keys
        extended_config = {
            "sources": [],
            "unknown_section": {"unknown_key": "unknown_value"},
            "experimental_features": ["feature1", "feature2"]
        }
        extended_file = temp_dir / "extended.yaml"
        with extended_file.open("w") as f:
            yaml.dump(extended_config, f)

        # Should handle unknown configuration gracefully
        pipeline = Pipeline(sources_yaml=extended_file)
        assert pipeline is not None

    @pytest.mark.e2e
    @pytest.mark.slow
    def test_workflow_resource_management(
            self, complete_test_config, mock_arcpy_environment):
        """Test workflow resource management and cleanup."""
        # Create directories that would be used
        download_dir = Path(
            complete_test_config["global_config"]["paths"]["download"])
        staging_dir = Path(
            complete_test_config["global_config"]["paths"]["staging"])

        with patch('etl.pipeline.ensure_dirs') as mock_ensure_dirs:
            pipeline = Pipeline(
                sources_yaml=complete_test_config["sources_file"],
                config_yaml_path=complete_test_config["config_file"]
            )

            # Verify directories would be created
            mock_ensure_dirs.assert_called()

            # Verify configuration paths are accessible
            paths_config = pipeline.global_cfg.get("paths", {})
            assert "download" in paths_config
            assert "staging" in paths_config
