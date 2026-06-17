import hashlib
import re
import time
import os
from urllib.parse import urljoin, urlparse


def now_ts_str():
    """
    Get the current timestamp as a formatted string.

    Returns the current local time formatted as "YYYY-MM-DD_HH_MM_SS", suitable for use
    in filenames or logging.

    Returns
    -------
    str
        The current timestamp as a string in the format "YYYY-MM-DD_HH_MM_SS".

    Notes
    -----
    - Uses the local time as returned by time.strftime.
    """
    return time.strftime("%Y-%m-%d_%H_%M_%S")


def normalize_domain(url: str) -> str:
    """
    Normalize the domain of a URL for use in filenames or storage paths.

    This function parses the input URL to extract the network location (domain),
    then replaces dots and slashes with underscores to create a filesystem-safe string.

    Parameters
    ----------
    url : str
        The URL from which to extract and normalize the domain.

    Returns
    -------
    str
        The normalized domain string, with dots and slashes replaced by underscores.

    Notes
    -----
    - Falls back to the first path segment if netloc is empty.
    - Useful for organizing files or blobs by domain.
    """
    p = urlparse(url)
    host = p.netloc or p.path.split('/')[0]
    repl = re.sub(r'[./]', '_', host)
    return repl


def hash_text(txt: str) -> str:
    """
    Compute the SHA-256 hash of the given text and return it as a hexadecimal string.

    This function encodes the input text as UTF-8 (ignoring errors), computes its SHA-256 hash,
    and returns the result as a lowercase hexadecimal string.

    Parameters
    ----------
    txt : str
        The input text to hash.

    Returns
    -------
    str
        The SHA-256 hash of the input text, as a hexadecimal string.

    Notes
    -----
    - Useful for generating unique identifiers for articles, URLs, or other text content.
    """
    return hashlib.sha256(txt.encode("utf-8", errors="ignore")).hexdigest()


def ensure_dir(path: str):
    """
    Ensure that a directory exists at the specified path.

    This function creates the directory and any necessary parent directories if they do not already exist.
    If the directory already exists, no error is raised.

    Parameters
    ----------
    path : str
        The path of the directory to ensure exists.

    Returns
    -------
    None

    Notes
    -----
    - Uses os.makedirs with exist_ok=True for safe, idempotent directory creation.
    """
    os.makedirs(path, exist_ok=True)


def filter_article_like(text: str, tag_hint: str | None, min_len=400):
    """
    Heuristically determine if a text block is likely to be a news article.

    This function checks if the provided text meets minimum length requirements or if the
    associated HTML tag hint suggests it is an article (e.g., contains "article" in the tag).

    Parameters
    ----------
    text : str
        The text content to evaluate.
    tag_hint : str or None
        Optional HTML tag or class hint associated with the text (e.g., tag name or class attribute).
    min_len : int, optional
        Minimum length (in characters) for the text to be considered article-like (default is 400).

    Returns
    -------
    bool
        True if the text is likely to be an article, False otherwise.

    Notes
    -----
    - Returns True if the text is at least min_len characters long, or if tag_hint contains "article".
    """
    # Heuristics: long enough, contains paragraphs, or came from <article>
    return len(text.strip()) >= min_len or (tag_hint and "article" in tag_hint.lower())
