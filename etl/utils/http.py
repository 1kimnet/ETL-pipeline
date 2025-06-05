"""
HTTP utility functions for the ETL pipeline, such as fetching
Content-Disposition headers and parsing filenames.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Tuple, Optional, Final
from urllib.parse import unquote
from urllib.request import Request, urlopen

# Regex to find filename in Content-Disposition header
# Handles filename="fname.ext" and filename*=UTF-8''fname.ext (URL encoded)
# Also handles cases where filename might not be quoted.
CONTENT_DISPOSITION_FILENAME_RE: Final[re.Pattern[str]] = re.compile(
    r'filename\*?=(?:UTF-8\'\')?([^;]+)', re.IGNORECASE
)
# A simpler regex if the above is too greedy or complex, focusing on quoted filenames:
# CONTENT_DISPOSITION_FILENAME_RE: Final[re.Pattern[str]] = re.compile(
# r'filename="([^"]+)"', re.IGNORECASE
# )


def _parse_filename_from_content_disposition(header_value: str) -> Optional[str]:
    """
    Parses a filename from a Content-Disposition header string.
    Handles basic quoted and unquoted filenames, and the filename* (UTF-8) variant.
    """
    if not header_value:
        return None
    
    match = CONTENT_DISPOSITION_FILENAME_RE.search(header_value)
    if match:
        potential_filename: str = match.group(1).strip('" ')
        # If it's URL encoded (from filename*=UTF-8''...), unquote it
        if "UTF-8''" in match.group(0).lower(): # Check if it was filename*
            return unquote(potential_filename)
        else:
            # For plain filename="value", ensure it's not accidentally percent-encoded
            # by the server sending a raw URL part. Unquoting here is generally safe.
            return unquote(potential_filename) 
    return None

def fetch_true_filename_parts(download_url: str, timeout: int = 10) -> Tuple[str, str]:
    """
    Determines the 'true' filename (stem and extension) for a download.
    It prioritizes Content-Disposition, then falls back to unquoting the URL's path.

    Args:
        download_url: The URL to fetch the filename information from.
        timeout: Timeout in seconds for the HTTP HEAD request.

    Returns:
        A tuple (stem, extension_with_dot). E.g., ("lämningar_län_södermanland", ".gpkg")
        Returns a default name if no reliable filename can be determined.
    """
    true_filename_str: Optional[str] = None
    final_url_after_redirects: str = download_url # Initialize with original URL

    logging.debug("Attempting to determine true filename for URL: %s", download_url)

    # 1. Try HEAD request for Content-Disposition
    try:
        # Create a context that does not verify SSL certificates if needed for specific servers.
        # WARNING: This is generally insecure and should only be used if you trust the server
        # and cannot resolve SSL certificate issues otherwise.
        # context = ssl._create_unverified_context() # Use with caution
        
        req = Request(download_url, method="HEAD")
        # Add a common user-agent to avoid potential blocks from servers
        req.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3')

        with urlopen(req, timeout=timeout) as resp: # Potentially add context=context here
            # urlopen may follow redirects, get the final URL
            final_url_after_redirects = resp.geturl()
            cd_header: Optional[str] = resp.getheader("Content-Disposition")
            if cd_header:
                logging.debug("Found Content-Disposition for %s: %s", final_url_after_redirects, cd_header)
                true_filename_str = _parse_filename_from_content_disposition(cd_header)
                if true_filename_str:
                    logging.info("    Derived filename from Content-Disposition: %s", true_filename_str)
    except Exception as e: # Catching a broad range of exceptions (socket.timeout, URLError, etc.)
        logging.warning(
            f"    HEAD request for Content-Disposition failed for {download_url}: {e}. Will fall back to URL basename."
        )

    # 2. Fallback to unquoting URL basename (from final URL after potential redirects)
    if not true_filename_str:
        url_path_name: str = Path(final_url_after_redirects).name
        if url_path_name:
            true_filename_str = unquote(url_path_name)
            logging.info("    Derived filename by unquoting URL basename ('%s'): %s", 
                         final_url_after_redirects, true_filename_str)
        else:
            logging.warning("    Could not derive filename from URL path for %s", final_url_after_redirects)
            # Fallback to a generic name if all else fails
            return "downloaded_file_from_url", ".unknown"

    if not true_filename_str: # Should be extremely rare if URL has a path component
         return "unknown_filename", ".tmp"

    # 3. Split into stem and extension
    # Ensure we handle cases where filename might not have an extension
    if '.' in true_filename_str:
        true_stem, true_ext_part = true_filename_str.rsplit('.', 1)
        return true_stem, "." + true_ext_part.lower() # Ensure extension is lowercased
    else: # No extension found in the derived filename
        return true_filename_str, ""
