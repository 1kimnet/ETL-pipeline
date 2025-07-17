"""HTTP session management with proper timeout handling."""
from __future__ import annotations

import logging
from typing import Optional, Dict, Any
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

log = logging.getLogger(__name__)


class HTTPSessionHandler:
    """Base class for handlers that need HTTP session management with proper timeout handling."""
    
    def __init__(
        self, 
        base_url: Optional[str] = None, 
        pool_connections: int = 10,
        pool_maxsize: int = 10,
        max_retries: int = 3,
        timeout: float = 30
    ):
        self.base_url = base_url
        self.timeout = timeout  # Store timeout for use in requests
        self._session: Optional[requests.Session] = None
        
        # Configure session
        self.session_config = {
            'pool_connections': pool_connections,
            'pool_maxsize': pool_maxsize,
            'max_retries': max_retries,
            'timeout': timeout
        }
    
    @property
    def session(self) -> requests.Session:
        """Get HTTP session with proper configuration."""
        if self._session is None:
            self._session = requests.Session()
            
            # Configure connection pooling and retries
            adapter = HTTPAdapter(
                pool_connections=self.session_config['pool_connections'],
                pool_maxsize=self.session_config['pool_maxsize'],
                max_retries=Retry(
                    total=self.session_config['max_retries'],
                    backoff_factor=0.3,
                    status_forcelist=[500, 502, 503, 504]
                )
            )
            
            self._session.mount('http://', adapter)
            self._session.mount('https://', adapter)
            
            # Set common headers
            self._session.headers.update({
                'User-Agent': 'ETL-Pipeline/1.0 (requests)',
                'Accept': 'application/json, application/geo+json, */*;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive'
            })
            
            # NOTE: Do NOT set session.timeout directly as it has no effect.
            # Instead, pass timeout parameter to each request method.
        
        return self._session
    
    def get(self, url: str, **kwargs) -> requests.Response:
        """Make GET request with proper timeout handling."""
        # Use stored timeout if not provided in kwargs
        if 'timeout' not in kwargs:
            kwargs['timeout'] = self.timeout
        
        return self.session.get(url, **kwargs)
    
    def post(self, url: str, **kwargs) -> requests.Response:
        """Make POST request with proper timeout handling."""
        # Use stored timeout if not provided in kwargs
        if 'timeout' not in kwargs:
            kwargs['timeout'] = self.timeout
        
        return self.session.post(url, **kwargs)
    
    def close_session(self) -> None:
        """Close the HTTP session."""
        if self._session:
            self._session.close()
            self._session = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_session()


# Global session instances for cleanup
_active_sessions = []


def cleanup_http_sessions():
    """Cleanup function to be called on application shutdown."""
    for session_handler in _active_sessions:
        session_handler.close_session()
    _active_sessions.clear()


def get_http_session(base_url: Optional[str] = None, **config) -> HTTPSessionHandler:
    """Get a managed HTTP session."""
    session_handler = HTTPSessionHandler(base_url, **config)
    _active_sessions.append(session_handler)
    return session_handler


def close_all_http_sessions():
    """Close all HTTP sessions."""
    cleanup_http_sessions()