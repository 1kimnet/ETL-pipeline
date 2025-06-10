"""Plugin architecture for ETL pipeline extensibility.

This module provides a flexible plugin system that allows extending the ETL pipeline
with custom handlers, loaders, processors, and other components without modifying
the core codebase.
"""
from __future__ import annotations

import importlib
import inspect
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, Union
import sys

from .exceptions import DependencyError, ValidationError
from .models import Source

log = logging.getLogger(__name__)


@dataclass
class PluginInfo:
    """Information about a plugin."""
    name: str
    version: str
    description: str
    author: Optional[str] = None
    category: str = "general"
    dependencies: List[str] = field(default_factory=list)
    enabled: bool = True
    config: Dict[str, Any] = field(default_factory=dict)


class PluginInterface(ABC):
    """Base interface for all plugins."""
    
    @property
    @abstractmethod
    def plugin_info(self) -> PluginInfo:
        """Get plugin information."""
        pass
    
    @abstractmethod
    def initialize(self, config: Optional[Dict[str, Any]] = None) -> None:
        """Initialize the plugin with configuration."""
        pass
    
    @abstractmethod
    def cleanup(self) -> None:
        """Clean up plugin resources."""
        pass
    
    def validate_config(self, config: Dict[str, Any]) -> List[str]:
        """Validate plugin configuration. Return list of validation errors."""
        return []


class HandlerPlugin(PluginInterface):
    """Base class for data source handler plugins."""
    
    @abstractmethod
    def can_handle(self, source: Source) -> bool:
        """Check if this handler can process the given source."""
        pass
    
    @abstractmethod
    def fetch_data(self, source: Source) -> Any:
        """Fetch data from the source."""
        pass


class LoaderPlugin(PluginInterface):
    """Base class for data loader plugins."""
    
    @abstractmethod
    def can_load(self, data_format: str) -> bool:
        """Check if this loader can handle the given data format."""
        pass
    
    @abstractmethod
    def load_data(self, data: Any, destination: str, **kwargs) -> str:
        """Load data to the destination."""
        pass


class ProcessorPlugin(PluginInterface):
    """Base class for data processor plugins."""
    
    @abstractmethod
    def process_data(self, data: Any, **kwargs) -> Any:
        """Process data and return the result."""
        pass


class ValidatorPlugin(PluginInterface):
    """Base class for data validator plugins."""
    
    @abstractmethod
    def validate_data(self, data: Any, schema: Optional[Dict] = None) -> List[str]:
        """Validate data against schema. Return list of validation errors."""
        pass


class PluginManager:
    """Manages plugin loading, registration, and lifecycle."""
    
    def __init__(self, plugin_dirs: Optional[List[Union[str, Path]]] = None):
        self.plugin_dirs = [Path(p) for p in (plugin_dirs or [])]
        self._plugins: Dict[str, PluginInterface] = {}
        self._plugin_types: Dict[str, Type[PluginInterface]] = {
            'handler': HandlerPlugin,
            'loader': LoaderPlugin,
            'processor': ProcessorPlugin,
            'validator': ValidatorPlugin
        }
        
        # Add default plugin directories
        self._add_default_plugin_dirs()
        
        log.info("🔌 Plugin manager initialized with %d plugin directories", len(self.plugin_dirs))
    
    def _add_default_plugin_dirs(self):
        """Add default plugin directories."""
        default_dirs = [
            Path.cwd() / "plugins",
            Path.cwd() / "etl" / "plugins",
            Path.home() / ".etl" / "plugins"
        ]
        
        for plugin_dir in default_dirs:
            if plugin_dir.exists() and plugin_dir not in self.plugin_dirs:
                self.plugin_dirs.append(plugin_dir)
    
    def register_plugin_type(self, name: str, plugin_class: Type[PluginInterface]):
        """Register a new plugin type."""
        self._plugin_types[name] = plugin_class
        log.debug("Registered plugin type: %s", name)
    
    def discover_plugins(self) -> List[PluginInfo]:
        """Discover plugins in configured directories."""
        discovered = []
        
        for plugin_dir in self.plugin_dirs:
            if not plugin_dir.exists():
                continue
            
            log.debug("Scanning for plugins in: %s", plugin_dir)
            
            # Look for Python files
            for python_file in plugin_dir.glob("*.py"):
                if python_file.name.startswith("_"):
                    continue
                
                try:
                    plugin_info = self._discover_plugin_from_file(python_file)
                    if plugin_info:
                        discovered.append(plugin_info)
                except Exception as e:
                    log.warning("Failed to discover plugin from %s: %s", python_file, e)
            
            # Look for plugin packages
            for package_dir in plugin_dir.iterdir():
                if not package_dir.is_dir() or package_dir.name.startswith("_"):
                    continue
                
                init_file = package_dir / "__init__.py"
                if init_file.exists():
                    try:
                        plugin_info = self._discover_plugin_from_package(package_dir)
                        if plugin_info:
                            discovered.append(plugin_info)
                    except Exception as e:
                        log.warning("Failed to discover plugin from %s: %s", package_dir, e)
        
        log.info("📦 Discovered %d plugins", len(discovered))
        return discovered
    
    def _discover_plugin_from_file(self, python_file: Path) -> Optional[PluginInfo]:
        """Discover plugin from a Python file."""
        module_name = python_file.stem
        spec = importlib.util.spec_from_file_location(module_name, python_file)
        
        if spec is None or spec.loader is None:
            return None
        
        module = importlib.util.module_from_spec(spec)
        
        # Add parent directory to sys.path temporarily
        parent_dir = str(python_file.parent)
        if parent_dir not in sys.path:
            sys.path.insert(0, parent_dir)
        
        try:
            spec.loader.exec_module(module)
            return self._extract_plugin_info_from_module(module)
        finally:
            # Remove from sys.path
            if parent_dir in sys.path:
                sys.path.remove(parent_dir)
    
    def _discover_plugin_from_package(self, package_dir: Path) -> Optional[PluginInfo]:
        """Discover plugin from a package directory."""
        package_name = package_dir.name
        
        # Add parent directory to sys.path temporarily
        parent_dir = str(package_dir.parent)
        if parent_dir not in sys.path:
            sys.path.insert(0, parent_dir)
        
        try:
            module = importlib.import_module(package_name)
            return self._extract_plugin_info_from_module(module)
        finally:
            # Remove from sys.path
            if parent_dir in sys.path:
                sys.path.remove(parent_dir)
    
    def _extract_plugin_info_from_module(self, module) -> Optional[PluginInfo]:
        """Extract plugin information from a module."""
        # Look for plugin classes
        plugin_classes = []
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if (issubclass(obj, PluginInterface) and 
                obj != PluginInterface and 
                not inspect.isabstract(obj)):
                plugin_classes.append(obj)
        
        if not plugin_classes:
            return None
        
        # Use the first plugin class found
        plugin_class = plugin_classes[0]
        
        # Try to get plugin info from the class
        try:
            plugin_instance = plugin_class()
            return plugin_instance.plugin_info
        except Exception as e:
            log.warning("Failed to instantiate plugin %s: %s", plugin_class.__name__, e)
            return None
    
    def load_plugin(self, plugin_info: PluginInfo, config: Optional[Dict[str, Any]] = None) -> bool:
        """Load and initialize a plugin."""
        if not plugin_info.enabled:
            log.debug("Skipping disabled plugin: %s", plugin_info.name)
            return False
        
        if plugin_info.name in self._plugins:
            log.warning("Plugin %s already loaded", plugin_info.name)
            return False
        
        try:
            # Check dependencies
            self._check_plugin_dependencies(plugin_info)
            
            # Find and instantiate plugin class
            plugin_instance = self._instantiate_plugin(plugin_info)
            
            if plugin_instance is None:
                log.error("Failed to instantiate plugin: %s", plugin_info.name)
                return False
            
            # Validate configuration
            plugin_config = {**plugin_info.config, **(config or {})}
            validation_errors = plugin_instance.validate_config(plugin_config)
            
            if validation_errors:
                log.error("Plugin %s configuration validation failed: %s", 
                         plugin_info.name, validation_errors)
                return False
            
            # Initialize plugin
            plugin_instance.initialize(plugin_config)
            
            # Register plugin
            self._plugins[plugin_info.name] = plugin_instance
            
            log.info("✅ Loaded plugin: %s v%s", plugin_info.name, plugin_info.version)
            return True
            
        except Exception as e:
            log.error("❌ Failed to load plugin %s: %s", plugin_info.name, e)
            return False
    
    def _check_plugin_dependencies(self, plugin_info: PluginInfo):
        """Check if plugin dependencies are available."""
        for dependency in plugin_info.dependencies:
            try:
                importlib.import_module(dependency)
            except ImportError as e:
                raise DependencyError(
                    f"Plugin {plugin_info.name} requires dependency: {dependency}",
                    dependency=dependency
                ) from e
    
    def _instantiate_plugin(self, plugin_info: PluginInfo) -> Optional[PluginInterface]:
        """Instantiate plugin from plugin info."""
        # This is a simplified implementation
        # In a real system, you'd need to track module references
        for plugin_dir in self.plugin_dirs:
            python_file = plugin_dir / f"{plugin_info.name}.py"
            if python_file.exists():
                module = self._import_module_from_file(python_file)
                return self._get_plugin_instance_from_module(module)
        
        return None
    
    def _import_module_from_file(self, python_file: Path):
        """Import module from file."""
        module_name = python_file.stem
        spec = importlib.util.spec_from_file_location(module_name, python_file)
        
        if spec is None or spec.loader is None:
            return None
        
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    
    def _get_plugin_instance_from_module(self, module) -> Optional[PluginInterface]:
        """Get plugin instance from module."""
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if (issubclass(obj, PluginInterface) and 
                obj != PluginInterface and 
                not inspect.isabstract(obj)):
                return obj()
        return None
    
    def unload_plugin(self, plugin_name: str) -> bool:
        """Unload a plugin."""
        if plugin_name not in self._plugins:
            log.warning("Plugin %s not loaded", plugin_name)
            return False
        
        try:
            plugin = self._plugins[plugin_name]
            plugin.cleanup()
            del self._plugins[plugin_name]
            
            log.info("🗑️ Unloaded plugin: %s", plugin_name)
            return True
            
        except Exception as e:
            log.error("Failed to unload plugin %s: %s", plugin_name, e)
            return False
    
    def get_plugin(self, plugin_name: str) -> Optional[PluginInterface]:
        """Get a loaded plugin by name."""
        return self._plugins.get(plugin_name)
    
    def get_plugins_by_type(self, plugin_type: Type[PluginInterface]) -> List[PluginInterface]:
        """Get all loaded plugins of a specific type."""
        return [plugin for plugin in self._plugins.values() if isinstance(plugin, plugin_type)]
    
    def list_loaded_plugins(self) -> List[str]:
        """List names of all loaded plugins."""
        return list(self._plugins.keys())
    
    def load_all_plugins(self, config: Optional[Dict[str, Dict[str, Any]]] = None) -> int:
        """Discover and load all available plugins."""
        discovered = self.discover_plugins()
        loaded_count = 0
        
        for plugin_info in discovered:
            plugin_config = config.get(plugin_info.name) if config else None
            if self.load_plugin(plugin_info, plugin_config):
                loaded_count += 1
        
        log.info("📊 Loaded %d/%d discovered plugins", loaded_count, len(discovered))
        return loaded_count
    
    def unload_all_plugins(self):
        """Unload all plugins."""
        plugin_names = list(self._plugins.keys())
        unloaded_count = 0
        
        for plugin_name in plugin_names:
            if self.unload_plugin(plugin_name):
                unloaded_count += 1
        
        log.info("🧹 Unloaded %d plugins", unloaded_count)
    
    def get_handler_for_source(self, source: Source) -> Optional[HandlerPlugin]:
        """Find a handler plugin that can process the given source."""
        handler_plugins = self.get_plugins_by_type(HandlerPlugin)
        
        for handler in handler_plugins:
            if handler.can_handle(source):
                return handler
        
        return None
    
    def get_loader_for_format(self, data_format: str) -> Optional[LoaderPlugin]:
        """Find a loader plugin that can handle the given data format."""
        loader_plugins = self.get_plugins_by_type(LoaderPlugin)
        
        for loader in loader_plugins:
            if loader.can_load(data_format):
                return loader
        
        return None
    
    def get_plugin_stats(self) -> Dict[str, Any]:
        """Get plugin manager statistics."""
        loaded_plugins = self._plugins
        plugin_types = {}
        
        for plugin in loaded_plugins.values():
            plugin_type = type(plugin).__bases__[0].__name__ if type(plugin).__bases__ else "Unknown"
            plugin_types[plugin_type] = plugin_types.get(plugin_type, 0) + 1
        
        return {
            'total_loaded': len(loaded_plugins),
            'plugin_directories': len(self.plugin_dirs),
            'plugin_types': plugin_types,
            'loaded_plugins': list(loaded_plugins.keys())
        }


# Global plugin manager instance
_plugin_manager = None


def get_plugin_manager(plugin_dirs: Optional[List[Union[str, Path]]] = None) -> PluginManager:
    """Get global plugin manager instance."""
    global _plugin_manager
    
    if _plugin_manager is None:
        _plugin_manager = PluginManager(plugin_dirs)
    
    return _plugin_manager


def register_plugin_type(name: str, plugin_class: Type[PluginInterface]):
    """Register a new plugin type globally."""
    get_plugin_manager().register_plugin_type(name, plugin_class)


def load_plugins_from_config(config: Dict[str, Any]) -> int:
    """Load plugins based on configuration."""
    plugin_manager = get_plugin_manager()
    
    # Set plugin directories if specified
    if 'plugin_directories' in config:
        plugin_manager.plugin_dirs.extend([Path(p) for p in config['plugin_directories']])
    
    # Load plugins
    plugin_configs = config.get('plugins', {})
    return plugin_manager.load_all_plugins(plugin_configs)