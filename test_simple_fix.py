#!/usr/bin/env python3
"""Direct test of ConcurrencyOptimizer without full ETL imports."""

import sys


# Test the specific fix directly
def test_concurrency_optimizer():
    """Test that ConcurrencyOptimizer properly defines ROOT_PATH."""

    # Import only the specific module content we need
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "performance_optimizer", "etl/utils/performance_optimizer.py"
    )

    if spec and spec.loader:
        module = importlib.util.module_from_spec(spec)

        # Mock the dependencies to avoid import issues
        sys.modules["psutil"] = type(
            "MockPsutil",
            (),
            {
                "cpu_count": lambda: 4,
                "virtual_memory": lambda: type(
                    "MockMemory",
                    (),
                    {"total": 8589934592, "percent": 50.0, "available": 4294967296},
                )(),
                "cpu_percent": lambda interval=None: 25.0,
                "disk_usage": lambda path: type(
                    "MockDisk", (), {"free": 107374182400}
                )(),  # 100GB
            },
        )()

        sys.modules["threading"] = type(
            "MockThreading",
            (),
            {
                "RLock": lambda: type(
                    "MockLock",
                    (),
                    {
                        "__enter__": lambda self: None,
                        "__exit__": lambda self, *args: None,
                    },
                )()
            },
        )()

        # Load the module
        spec.loader.exec_module(module)

        # Test the ConcurrencyOptimizer class
        ConcurrencyOptimizer = getattr(module, "ConcurrencyOptimizer")

        # Create an instance
        optimizer = ConcurrencyOptimizer()

        # Check if ROOT_PATH exists
        if hasattr(optimizer, "ROOT_PATH"):
            print(
                f"✅ SUCCESS: ROOT_PATH attribute exists: {optimizer.ROOT_PATH}")
            return True
        else:
            print("❌ FAILED: ROOT_PATH attribute not found")
            return False
    else:
        print("❌ FAILED: Could not load module")
        return False


if __name__ == "__main__":
    success = test_concurrency_optimizer()
    sys.exit(0 if success else 1)
