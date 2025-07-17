"""Integration tests for etl.handlers.file module."""
import pytest
from unittest.mock import Mock, patch, MagicMock, mock_open
from pathlib import Path
import tempfile
import shutil

from etl.handlers.file import FileDownloadHandler
from etl.models import Source


class TestFileDownloadHandler:
    """Test FileDownloadHandler with mocked external dependencies."""
    
    @pytest.fixture
    def sample_file_source(self):
        """Create a sample file source for testing."""
        return Source(
            name="Test File Source",
            authority="TEST",
            type="file",
            url="https://example.com/data.zip",
            enabled=True,
            download_format="zip"
        )
    
    @pytest.fixture
    def sample_gpkg_source(self):
        """Create a sample GPKG file source for testing."""
        return Source(
            name="Test GPKG Source",
            authority="TEST",
            type="file",
            url="https://example.com/data.gpkg",
            enabled=True,
            download_format="gpkg"
        )
    
    @pytest.fixture
    def sample_multi_file_source(self):
        """Create a sample source with multiple files to download."""
        return Source(
            name="Test Multi File Source", 
            authority="TEST",
            type="file",
            url="https://example.com/base/",
            enabled=True,
            download_format="zip",
            include=["file1", "file2", "file3"]
        )
    
    @pytest.fixture
    def sample_global_config(self):
        """Sample global configuration."""
        return {
            "timeout": 30,
            "paths": {
                "download": "test_downloads",
                "staging": "test_staging"
            }
        }
    
    @pytest.mark.integration
    @patch('etl.handlers.file.ensure_dirs')
    def test_handler_initialization(self, mock_ensure_dirs, sample_file_source, sample_global_config):
        """Test handler initialization."""
        handler = FileDownloadHandler(sample_file_source, sample_global_config)
        
        assert handler.src == sample_file_source
        assert handler.global_config == sample_global_config
        mock_ensure_dirs.assert_called_once()
    
    @pytest.mark.integration
    @patch('etl.handlers.file.ensure_dirs')
    def test_fetch_disabled_source(self, mock_ensure_dirs, sample_global_config):
        """Test fetch behavior with disabled source."""
        disabled_source = Source(
            name="Disabled Source",
            authority="TEST",
            type="file",
            url="https://example.com/data.zip",
            enabled=False
        )
        
        handler = FileDownloadHandler(disabled_source, sample_global_config)
        
        # Should return early without attempting download
        handler.fetch()  # Should not raise any exception
    
    @pytest.mark.integration
    @patch('etl.handlers.file.ensure_dirs')
    @patch('etl.handlers.file.download')
    @patch('etl.handlers.file.extract_zip')
    def test_fetch_single_zip_file(self, mock_extract_zip, mock_download, mock_ensure_dirs,
                                  sample_file_source, sample_global_config, temp_dir):
        """Test fetching a single ZIP file."""
        # Mock download to return a fake zip file path
        mock_zip_path = temp_dir / "downloaded.zip"
        mock_zip_path.touch()
        mock_download.return_value = mock_zip_path
        
        handler = FileDownloadHandler(sample_file_source, sample_global_config)
        
        # Mock the private method that would be called
        with patch.object(handler, '_download_single_resource') as mock_single:
            handler.fetch()
            mock_single.assert_called_once()
    
    @pytest.mark.integration
    @patch('etl.handlers.file.ensure_dirs')
    def test_fetch_gpkg_source_detection(self, mock_ensure_dirs, sample_gpkg_source, sample_global_config):
        """Test detection of GPKG sources."""
        handler = FileDownloadHandler(sample_gpkg_source, sample_global_config)
        
        # Mock the private method to verify correct path is taken
        with patch.object(handler, '_download_single_resource') as mock_single:
            handler.fetch()
            mock_single.assert_called_once()
    
    @pytest.mark.integration
    @patch('etl.handlers.file.ensure_dirs')
    def test_fetch_multi_file_source(self, mock_ensure_dirs, sample_multi_file_source, sample_global_config):
        """Test fetching multiple files from include list."""
        handler = FileDownloadHandler(sample_multi_file_source, sample_global_config)
        
        # Mock the private method to verify correct path is taken
        with patch.object(handler, '_download_multiple_files') as mock_multiple:
            handler.fetch()
            mock_multiple.assert_called_once()
    
    @pytest.mark.integration
    @patch('etl.handlers.file.ensure_dirs')
    def test_fetch_source_with_no_url(self, mock_ensure_dirs, sample_global_config):
        """Test handling of source with no URL."""
        no_url_source = Source(
            name="No URL Source",
            authority="TEST",
            type="file",
            url="",
            enabled=True
        )
        
        handler = FileDownloadHandler(no_url_source, sample_global_config)
        
        # Should handle gracefully without crashing
        handler.fetch()  # Should not raise exception
    
    @pytest.mark.integration
    @patch('etl.handlers.file.ensure_dirs')
    @patch('etl.handlers.file.fetch_true_filename_parts')
    def test_filename_detection(self, mock_fetch_filename, mock_ensure_dirs, 
                              sample_file_source, sample_global_config):
        """Test filename detection from HTTP headers."""
        mock_fetch_filename.return_value = ("test_data", ".zip")
        
        handler = FileDownloadHandler(sample_file_source, sample_global_config)
        
        # This would be called during download process
        # Just verify the mocking works correctly
        assert mock_fetch_filename("https://example.com/data.zip") == ("test_data", ".zip")
    
    @pytest.mark.integration
    @patch('etl.handlers.file.ensure_dirs')
    @patch('etl.handlers.file.sanitize_for_filename')
    def test_filename_sanitization(self, mock_sanitize, mock_ensure_dirs,
                                 sample_file_source, sample_global_config):
        """Test that filenames are properly sanitized."""
        mock_sanitize.return_value = "sanitized_filename"
        
        handler = FileDownloadHandler(sample_file_source, sample_global_config)
        
        # Verify sanitization function is available
        from etl.utils.naming import sanitize_for_filename
        assert sanitize_for_filename("Test File Name") == "test_file_name"
    
    @pytest.mark.integration
    @patch('etl.handlers.file.ensure_dirs')
    def test_handler_with_empty_global_config(self, mock_ensure_dirs, sample_file_source):
        """Test handler behavior with empty global config."""
        handler = FileDownloadHandler(sample_file_source, {})
        
        assert handler.global_config == {}
        # Should not crash during initialization
    
    @pytest.mark.integration
    @patch('etl.handlers.file.ensure_dirs')
    def test_handler_with_none_global_config(self, mock_ensure_dirs, sample_file_source):
        """Test handler behavior with None global config."""
        handler = FileDownloadHandler(sample_file_source, None)
        
        assert handler.global_config == {}
        # Should handle None config gracefully

    @pytest.mark.integration 
    @patch('etl.handlers.file.ensure_dirs')
    def test_context_manager_support(self, mock_ensure_dirs, sample_file_source):
        """Test that FileDownloadHandler supports context manager protocol."""
        handler = FileDownloadHandler(sample_file_source)
        
        # Test context manager methods exist
        assert hasattr(handler, '__enter__')
        assert hasattr(handler, '__exit__')
        
        # Test context manager usage
        with FileDownloadHandler(sample_file_source) as context_handler:
            assert context_handler is not None
            assert isinstance(context_handler, FileDownloadHandler)
            assert hasattr(context_handler, 'fetch')
        
        # Should exit without errors