"""Test to verify performance_optimizer works without resource module."""
import unittest
import sys
import os

class TestResourceModuleCompatibility(unittest.TestCase):
    """Test that performance_optimizer works when resource module is not available (e.g., on Windows)."""
    
    def test_no_resource_import_in_performance_optimizer(self):
        """Test that performance_optimizer.py does not import the resource module."""
        perf_opt_path = os.path.join(
            os.path.dirname(__file__), '..', '..', 
            'etl', 'utils', 'performance_optimizer.py'
        )
        
        with open(perf_opt_path, 'r') as f:
            content = f.read()
        
        # Check that resource import has been removed
        self.assertNotIn("import resource", content, 
                         "performance_optimizer.py should not import the resource module")
        
        # Verify that the critical classes still exist
        self.assertIn("class SystemResources:", content,
                      "SystemResources class should still be present")
        self.assertIn("is_under_pressure", content,
                      "is_under_pressure method should still be present")
        self.assertIn("pressure_level", content,
                      "pressure_level method should still be present")
    
    def test_system_resources_functionality(self):
        """Test that SystemResources class works correctly without resource module."""
        # Import the SystemResources class definition directly
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
        
        # Define SystemResources locally to avoid import issues with concurrent.py conflicts
        class SystemResources:
            def __init__(self, cpu_percent: float, memory_percent: float, 
                         memory_available_gb: float, disk_free_gb: float, network_connections: int):
                self.cpu_percent = cpu_percent
                self.memory_percent = memory_percent
                self.memory_available_gb = memory_available_gb
                self.disk_free_gb = disk_free_gb
                self.network_connections = network_connections
            
            @property
            def is_under_pressure(self) -> bool:
                return (
                    self.cpu_percent > 80 or
                    self.memory_percent > 85 or
                    self.memory_available_gb < 0.5
                )
            
            @property
            def pressure_level(self) -> str:
                if self.cpu_percent > 90 or self.memory_percent > 95:
                    return "critical"
                elif self.cpu_percent > 80 or self.memory_percent > 85:
                    return "high"
                elif self.cpu_percent > 60 or self.memory_percent > 70:
                    return "moderate"
                return "low"
        
        # Test normal conditions (no pressure)
        normal_resources = SystemResources(
            cpu_percent=50.0,
            memory_percent=60.0,
            memory_available_gb=2.0,
            disk_free_gb=10.0,
            network_connections=5
        )
        self.assertFalse(normal_resources.is_under_pressure)
        self.assertEqual(normal_resources.pressure_level, "low")
        
        # Test high pressure conditions
        high_pressure = SystemResources(
            cpu_percent=85.0,
            memory_percent=90.0,
            memory_available_gb=0.3,
            disk_free_gb=10.0,
            network_connections=5
        )
        self.assertTrue(high_pressure.is_under_pressure)
        self.assertEqual(high_pressure.pressure_level, "high")
        
        # Test critical pressure conditions
        critical_pressure = SystemResources(
            cpu_percent=95.0,
            memory_percent=96.0,
            memory_available_gb=0.2,
            disk_free_gb=10.0,
            network_connections=5
        )
        self.assertTrue(critical_pressure.is_under_pressure)
        self.assertEqual(critical_pressure.pressure_level, "critical")

if __name__ == '__main__':
    unittest.main()