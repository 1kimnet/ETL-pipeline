"""Integration tests for etl.handlers.rest_api module."""
import pytest
from unittest.mock import Mock, patch, MagicMock
import json
from pathlib import Path

from etl.handlers.rest_api import RestApiDownloadHandler
from etl.models import Source


class TestRestApiDownloadHandler:
    """Test RestApiDownloadHandler with mocked external dependencies."""

    @pytest.fixture
    def sample_rest_api_source(self):
        """Create a sample REST API source for testing."""
        return Source(
            name="Test REST API",
            authority="TEST",
            type="rest_api",
            url="https://services.arcgis.com/test/arcgis/rest/services/TestService/MapServer/0/query",
            enabled=True)

    @pytest.fixture
    def sample_global_config(self):
        """Sample global configuration."""
        return {
            "max_retries": 3,
            "timeout": 30,
            "paths": {
                "download": "test_downloads",
                "staging": "test_staging"
            }
        }

    @pytest.mark.integration
    @patch('etl.handlers.rest_api.ensure_dirs')
    def test_handler_initialization(
            self,
            mock_ensure_dirs,
            sample_rest_api_source,
            sample_global_config):
        """Test handler initialization."""
        handler = RestApiDownloadHandler(
            sample_rest_api_source, sample_global_config)

        assert handler.src == sample_rest_api_source
        assert handler.global_config == sample_global_config
        mock_ensure_dirs.assert_called_once()

    @pytest.mark.integration
    @patch('etl.handlers.rest_api.requests')
    @patch('etl.handlers.rest_api.ensure_dirs')
    def test_get_service_metadata_successful(
            self,
            mock_ensure_dirs,
            mock_requests,
            sample_rest_api_source,
            sample_global_config):
        """Test successful service metadata retrieval."""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "currentVersion": "10.8",
            "id": 0,
            "name": "TestLayer",
            "type": "Feature Layer"
        }
        mock_requests.get.return_value = mock_response

        handler = RestApiDownloadHandler(
            sample_rest_api_source, sample_global_config)
        result = handler._get_service_metadata("https://test.com/MapServer")

        assert result is not None
        assert result["name"] == "TestLayer"
        assert result["type"] == "Feature Layer"
        mock_requests.get.assert_called_once()

    @pytest.mark.integration
    @patch('etl.handlers.rest_api.requests')
    @patch('etl.handlers.rest_api.ensure_dirs')
    def test_get_service_metadata_with_retries(
            self,
            mock_ensure_dirs,
            mock_requests,
            sample_rest_api_source,
            sample_global_config):
        """Test service metadata retrieval with retries on failure."""
        # Mock first call to fail, second to succeed
        mock_response_fail = Mock()
        mock_response_fail.status_code = 500
        mock_response_fail.raise_for_status.side_effect = Exception(
            "Server Error")

        mock_response_success = Mock()
        mock_response_success.status_code = 200
        mock_response_success.json.return_value = {"name": "TestLayer"}

        mock_requests.get.side_effect = [
            mock_response_fail, mock_response_success]

        handler = RestApiDownloadHandler(
            sample_rest_api_source, sample_global_config)
        result = handler._get_service_metadata("https://test.com/MapServer")

        assert result is not None
        assert result["name"] == "TestLayer"
        assert mock_requests.get.call_count == 2

    @pytest.mark.integration
    @patch('etl.handlers.rest_api.requests')
    @patch('etl.handlers.rest_api.ensure_dirs')
    def test_get_service_metadata_max_retries_exceeded(
            self,
            mock_ensure_dirs,
            mock_requests,
            sample_rest_api_source,
            sample_global_config):
        """Test service metadata retrieval when max retries exceeded."""
        # Mock all calls to fail
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = Exception("Server Error")
        mock_requests.get.return_value = mock_response

        handler = RestApiDownloadHandler(
            sample_rest_api_source, sample_global_config)
        result = handler._get_service_metadata("https://test.com/MapServer")

        assert result is None
        assert mock_requests.get.call_count == 3  # max_retries from config

    @pytest.mark.integration
    @patch('etl.handlers.rest_api.requests')
    @patch('etl.handlers.rest_api.ensure_dirs')
    def test_get_service_metadata_invalid_json(
            self,
            mock_ensure_dirs,
            mock_requests,
            sample_rest_api_source,
            sample_global_config):
        """Test handling of invalid JSON response."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = json.JSONDecodeError(
            "Invalid JSON", "", 0)
        mock_requests.get.return_value = mock_response

        handler = RestApiDownloadHandler(
            sample_rest_api_source, sample_global_config)
        result = handler._get_service_metadata("https://test.com/MapServer")

        assert result is None

    @pytest.mark.integration
    @patch('etl.handlers.rest_api.ensure_dirs')
    def test_handler_with_disabled_source(self, mock_ensure_dirs):
        """Test handler behavior with disabled source."""
        disabled_source = Source(
            name="Disabled Source",
            authority="TEST",
            type="rest_api",
            url="https://test.com",
            enabled=False
        )

        handler = RestApiDownloadHandler(disabled_source)
        # This should not raise any exceptions
        assert handler.src.enabled is False

    @pytest.mark.integration
    @patch('etl.handlers.rest_api.requests')
    @patch('etl.handlers.rest_api.ensure_dirs')
    def test_handler_with_invalid_config(
            self,
            mock_ensure_dirs,
            mock_requests,
            sample_rest_api_source):
        """Test handler behavior with invalid global config values."""
        invalid_config = {
            "max_retries": "invalid",  # Should be int
            "timeout": -1  # Should be positive
        }

        handler = RestApiDownloadHandler(
            sample_rest_api_source, invalid_config)

        # Handler should handle invalid config gracefully
        assert handler.global_config == invalid_config

        # Test that metadata fetch still works with default retry logic
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"name": "TestLayer"}
        mock_requests.get.return_value = mock_response

        result = handler._get_service_metadata("https://test.com/MapServer")
        assert result is not None
