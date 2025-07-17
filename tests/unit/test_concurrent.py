"""Tests for concurrent download utilities."""

import pytest
import threading
import time
from unittest.mock import Mock, patch

from etl.utils.concurrent import (
    ConcurrentDownloadManager,
    ConcurrentLayerDownloader, 
    ConcurrentCollectionDownloader,
    ConcurrentFileDownloader,
    ConcurrentResult,
    get_layer_downloader,
    get_collection_downloader,
    get_file_downloader,
    configure_concurrent_downloads
)


class TestConcurrentDownloadManager:
    """Test the core ConcurrentDownloadManager class."""
    
    def test_init_with_default_workers(self):
        """Test initialization with default worker count."""
        manager = ConcurrentDownloadManager()
        assert manager.default_max_workers == 5
    
    def test_init_with_custom_workers(self):
        """Test initialization with custom worker count."""
        manager = ConcurrentDownloadManager(10)
        assert manager.default_max_workers == 10
    
    def test_execute_concurrent_empty_tasks(self):
        """Test execution with empty task list."""
        manager = ConcurrentDownloadManager()
        results = manager.execute_concurrent([])
        assert results == []
    
    def test_execute_concurrent_successful_tasks(self):
        """Test execution with successful tasks."""
        manager = ConcurrentDownloadManager()
        
        def dummy_task(value, multiplier=1):
            return value * multiplier
        
        tasks = [
            (dummy_task, (1,), {"multiplier": 2}),
            (dummy_task, (2,), {"multiplier": 3}),
            (dummy_task, (3,), {"multiplier": 4})
        ]
        
        results = manager.execute_concurrent(tasks)
        
        assert len(results) == 3
        assert all(r.success for r in results)
        assert results[0].result == 2
        assert results[1].result == 6  
        assert results[2].result == 12
    
    def test_execute_concurrent_with_failures(self):
        """Test execution with some failing tasks."""
        manager = ConcurrentDownloadManager()
        
        def failing_task():
            raise ValueError("Test error")
        
        def success_task():
            return "success"
        
        tasks = [
            (success_task, (), {}),
            (failing_task, (), {}),
            (success_task, (), {})
        ]
        
        results = manager.execute_concurrent(tasks)
        
        assert len(results) == 3
        assert results[0].success is True
        assert results[1].success is False
        assert results[2].success is True
        assert isinstance(results[1].error, ValueError)
    
    def test_execute_concurrent_max_workers_parameter(self):
        """Test that max_workers parameter is respected."""
        manager = ConcurrentDownloadManager(default_max_workers=5)
        
        # Mock ThreadPoolExecutor to verify max_workers is passed correctly
        with patch('etl.utils.concurrent.ThreadPoolExecutor') as mock_executor:
            mock_executor.return_value.__enter__ = Mock(return_value=Mock())
            mock_executor.return_value.__exit__ = Mock(return_value=None)
            
            # Test with override
            manager.execute_concurrent([], max_workers=10)
            mock_executor.assert_called_with(max_workers=10)
            
            # Test with default
            manager.execute_concurrent([])
            mock_executor.assert_called_with(max_workers=5)
    
    def test_execute_concurrent_fail_fast(self):
        """Test fail_fast behavior."""
        manager = ConcurrentDownloadManager()
        
        def slow_task():
            time.sleep(0.1)
            return "slow"
        
        def failing_task():
            raise ValueError("Fast failure")
        
        tasks = [
            (slow_task, (), {}),
            (failing_task, (), {}),
            (slow_task, (), {})
        ]
        
        start_time = time.time()
        results = manager.execute_concurrent(tasks, fail_fast=True)
        duration = time.time() - start_time
        
        # Should complete faster than if all tasks ran
        assert duration < 0.3  # Should be much less than 0.3 seconds
        
        # Should have at least one failure
        assert any(not r.success for r in results)


class TestConcurrentLayerDownloader:
    """Test the ConcurrentLayerDownloader class."""
    
    def test_init_with_default_workers(self):
        """Test initialization with default worker count."""
        downloader = ConcurrentLayerDownloader()
        assert downloader.manager.default_max_workers == 5
    
    def test_init_with_custom_workers(self):
        """Test initialization with custom worker count."""
        downloader = ConcurrentLayerDownloader(8)
        assert downloader.manager.default_max_workers == 8
    
    def test_download_layers_concurrent_empty(self):
        """Test download with empty layers list."""
        downloader = ConcurrentLayerDownloader()
        results = downloader.download_layers_concurrent(None, [])
        assert results == []
    
    def test_download_layers_concurrent_with_mock_handler(self):
        """Test download with mock handler."""
        downloader = ConcurrentLayerDownloader()
        
        # Create mock handler
        mock_handler = Mock()
        mock_handler._fetch_layer_data = Mock(return_value="layer_data")
        
        layers_info = [
            {"id": "1", "name": "layer1", "metadata": {"test": "data"}},
            {"id": "2", "name": "layer2", "metadata": {"test": "data"}}
        ]
        
        results = downloader.download_layers_concurrent(
            handler=mock_handler,
            layers_info=layers_info,
            max_workers=2
        )
        
        assert len(results) == 2
        assert all(r.success for r in results)
        
        # Verify handler was called correctly
        assert mock_handler._fetch_layer_data.call_count == 2


class TestConcurrentCollectionDownloader:
    """Test the ConcurrentCollectionDownloader class."""
    
    def test_init_with_default_workers(self):
        """Test initialization with default worker count."""
        downloader = ConcurrentCollectionDownloader()
        assert downloader.manager.default_max_workers == 3
    
    def test_download_collections_concurrent_with_mock_handler(self):
        """Test download with mock handler."""
        downloader = ConcurrentCollectionDownloader()
        
        # Create mock handler
        mock_handler = Mock()
        mock_handler._fetch_collection = Mock(return_value="collection_data")
        
        collections = [
            {"id": "collection1"},
            {"id": "collection2"}
        ]
        
        results = downloader.download_collections_concurrent(
            handler=mock_handler,
            collections=collections,
            max_workers=2
        )
        
        assert len(results) == 2
        assert all(r.success for r in results)
        
        # Verify handler was called correctly
        assert mock_handler._fetch_collection.call_count == 2


class TestConcurrentFileDownloader:
    """Test the ConcurrentFileDownloader class."""
    
    def test_init_with_default_workers(self):
        """Test initialization with default worker count."""
        downloader = ConcurrentFileDownloader()
        assert downloader.manager.default_max_workers == 4
    
    def test_download_files_concurrent_with_mock_handler(self):
        """Test download with mock handler."""
        downloader = ConcurrentFileDownloader()
        
        # Create mock handler
        mock_handler = Mock()
        mock_handler._download_single_file_stem = Mock(return_value="file_data")
        
        file_stems = ["file1", "file2", "file3"]
        
        results = downloader.download_files_concurrent(
            handler=mock_handler,
            file_stems=file_stems,
            max_workers=2
        )
        
        assert len(results) == 3
        assert all(r.success for r in results)
        
        # Verify handler was called correctly
        assert mock_handler._download_single_file_stem.call_count == 3


class TestGlobalInstances:
    """Test global instance management."""
    
    def test_get_layer_downloader_singleton(self):
        """Test that get_layer_downloader returns the same instance."""
        downloader1 = get_layer_downloader()
        downloader2 = get_layer_downloader()
        assert downloader1 is downloader2
    
    def test_get_collection_downloader_singleton(self):
        """Test that get_collection_downloader returns the same instance."""
        downloader1 = get_collection_downloader()
        downloader2 = get_collection_downloader()
        assert downloader1 is downloader2
    
    def test_get_file_downloader_singleton(self):
        """Test that get_file_downloader returns the same instance."""
        downloader1 = get_file_downloader()
        downloader2 = get_file_downloader()
        assert downloader1 is downloader2
    
    def test_configure_concurrent_downloads(self):
        """Test configuration of global downloaders."""
        # Configure with custom worker counts
        configure_concurrent_downloads(
            layer_workers=8,
            collection_workers=4,
            file_workers=6
        )
        
        # Verify configuration was applied
        layer_downloader = get_layer_downloader()
        collection_downloader = get_collection_downloader()
        file_downloader = get_file_downloader()
        
        assert layer_downloader.manager.default_max_workers == 8
        assert collection_downloader.manager.default_max_workers == 4
        assert file_downloader.manager.default_max_workers == 6


class TestThreadSafety:
    """Test thread safety of concurrent utilities."""
    
    def test_manager_thread_safety(self):
        """Test that ConcurrentDownloadManager is thread-safe."""
        manager = ConcurrentDownloadManager()
        results = []
        errors = []
        
        def worker_thread(thread_id):
            try:
                def dummy_task(value):
                    return value * thread_id
                
                tasks = [(dummy_task, (i,), {}) for i in range(5)]
                thread_results = manager.execute_concurrent(tasks, max_workers=2)
                results.extend(thread_results)
            except Exception as e:
                errors.append(e)
        
        # Create multiple threads
        threads = []
        for i in range(1, 4):  # 3 threads
            thread = threading.Thread(target=worker_thread, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Verify no errors occurred
        assert len(errors) == 0
        
        # Verify we got results from all threads
        assert len(results) == 15  # 3 threads * 5 tasks each
        assert all(r.success for r in results)
    
    def test_global_instance_thread_safety(self):
        """Test that global instances are thread-safe."""
        instances = []
        errors = []
        
        def worker_thread():
            try:
                layer_downloader = get_layer_downloader()
                collection_downloader = get_collection_downloader()
                file_downloader = get_file_downloader()
                instances.append((layer_downloader, collection_downloader, file_downloader))
            except Exception as e:
                errors.append(e)
        
        # Create multiple threads
        threads = []
        for i in range(10):
            thread = threading.Thread(target=worker_thread)
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Verify no errors occurred
        assert len(errors) == 0
        
        # Verify all threads got the same instances
        assert len(instances) == 10
        
        first_layer, first_collection, first_file = instances[0]
        for layer, collection, file in instances[1:]:
            assert layer is first_layer
            assert collection is first_collection
            assert file is first_file


if __name__ == "__main__":
    pytest.main([__file__])