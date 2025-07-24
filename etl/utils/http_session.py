"""HTTP session management with connection pooling and proper cleanup."""

from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from typing import Dict, Generator, Optional
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)


class HTTPSessionManager:
    """Manages HTTP sessions with connection pooling and automatic cleanup."""

    def __init__(self):
        self._sessions: Dict[str, requests.Session] = {}
        self._lock = threading.RLock()
        self._default_config = {
            "pool_connections": 10,
            "pool_maxsize": 10,
            "max_retries": 3,
            "backoff_factor": 0.3,
            "timeout": 30,
        }

    def get_session(
            self,
            base_url: Optional[str] = None,
            **config) -> requests.Session:
        """Get or create a session for the given base URL."""
        # Use domain as key for session reuse
        if base_url:
            parsed = urlparse(base_url)
            session_key = f"{parsed.scheme}://{parsed.netloc}"
        else:
            session_key = "default"

        with self._lock:
            if session_key not in self._sessions:
                self._sessions[session_key] = self._create_session(**config)
                log.debug("Created new HTTP session for: %s", session_key)

            return self._sessions[session_key]

    def _create_session(self, **config) -> requests.Session:
        """Create a new session with proper configuration."""
        # Merge with default config
        session_config = {**self._default_config, **config}

        session = requests.Session()

        # Configure connection pooling
        adapter = HTTPAdapter(
            pool_connections=session_config["pool_connections"],
            pool_maxsize=session_config["pool_maxsize"],
            max_retries=Retry(
                total=session_config["max_retries"],
                backoff_factor=session_config["backoff_factor"],
                status_forcelist=[500, 502, 503, 504],
            ),
        )

        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Set default timeout
        session.timeout = session_config["timeout"]

        # Set common headers
        session.headers.update(
            {
                "User-Agent": "ETL-Pipeline/1.0 (requests)",
                "Accept": "application/json, application/geo+json, */*;q=0.9",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
            }
        )

        # Store timeout in session for use in request method override
        session._etl_timeout = session_config["timeout"]

        return session

    def close_session(self, base_url: Optional[str] = None):
        """Close a specific session."""
        if base_url:
            parsed = urlparse(base_url)
            session_key = f"{parsed.scheme}://{parsed.netloc}"
        else:
            session_key = "default"

        with self._lock:
            if session_key in self._sessions:
                session = self._sessions.pop(session_key)
                session.close()
                log.debug("Closed HTTP session for: %s", session_key)

    def close_all_sessions(self):
        """Close all sessions."""
        with self._lock:
            for session_key, session in self._sessions.items():
                try:
                    session.close()
                    log.debug("Closed HTTP session for: %s", session_key)
                except Exception as e:
                    log.warning(
                        "Failed to close session %s: %s", session_key, e)
            self._sessions.clear()

    def __del__(self):
        """Cleanup on destruction."""
        self.close_all_sessions()


# Global session manager instance
_session_manager = HTTPSessionManager()


def get_http_session(
        base_url: Optional[str] = None,
        **config) -> requests.Session:
    """Get a managed HTTP session with timeout override."""
    session = _session_manager.get_session(base_url, **config)

    # Override the request method to ensure timeout is always passed
    if not hasattr(session, "_etl_request_override"):
        original_request = session.request

        def request_with_timeout(method, url, **kwargs):
            # Use session timeout if no timeout specified
            if "timeout" not in kwargs and hasattr(session, "_etl_timeout"):
                kwargs["timeout"] = session._etl_timeout
            return original_request(method, url, **kwargs)

        session.request = request_with_timeout
        session._etl_request_override = True

    return session


@contextmanager
def http_session(
    base_url: Optional[str] = None, **config
) -> Generator[requests.Session, None, None]:
    """Context manager for HTTP sessions with automatic cleanup."""
    session = get_http_session(base_url, **config)
    try:
        yield session
    finally:
        # Session cleanup is handled by the manager
        pass


def close_http_session(base_url: Optional[str] = None):
    """Close a specific HTTP session."""
    _session_manager.close_session(base_url)


def close_all_http_sessions():
    """Close all HTTP sessions."""
    _session_manager.close_all_sessions()


class HTTPSessionHandler:
    """Base class for handlers that need HTTP session management."""

    def __init__(self, base_url: Optional[str] = None, **session_config):
        self.base_url = base_url
        self.session_config = session_config
        self._session: Optional[requests.Session] = None

    @property
    def session(self) -> requests.Session:
        """Get the HTTP session for this handler."""
        if self._session is None:
            self._session = get_http_session(
                self.base_url, **self.session_config)
        return self._session

    def close_session(self):
        """Close the HTTP session."""
        if self._session:
            close_http_session(self.base_url)
            self._session = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_session()


# Cleanup function for application shutdown
def cleanup_http_sessions():
    """Cleanup function to be called on application shutdown."""
    close_all_http_sessions()
