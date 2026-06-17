import time
import random
import requests
import tldextract
from bs4 import BeautifulSoup
from readability import Document
from urllib.parse import urljoin
from urllib.parse import urlparse
from .utils import filter_article_like
from requests.adapters import HTTPAdapter, Retry


# A more realistic browser-like header makes Reuters/BBC happier
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "DNT": "1",
    "Referer": "https://www.google.com/",
    "Pragma": "no-cache",
}


def _get(url, timeout=25, retries=2, backoff=1.0):
    """
    Perform an HTTP GET request with retries and exponential backoff.

    This function attempts to fetch the content of the specified URL using the requests library,
    applying browser-like headers. If the request fails, it retries up to the specified number
    of times, waiting with exponential backoff between attempts.

    Parameters
    ----------
    url : str
        The URL to fetch.
    timeout : int, optional
        Timeout in seconds for each request attempt (default is 25).
    retries : int, optional
        Number of retry attempts on failure (default is 2).
    backoff : float, optional
        Base backoff time in seconds between retries (default is 1.0).

    Returns
    -------
    requests.Response
        The HTTP response object if the request is successful.

    Raises
    ------
    Exception
        The last exception encountered if all retries fail.

    Notes
    -----
    - Uses realistic browser headers to improve compatibility with news sites.
    - Waits with exponential backoff and jitter between retries.
    """
    last = None
    for i in range(retries + 1):
        try:
            # r = requests.get(url, headers=HEADERS, timeout=timeout)
            r = http_get(url, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            last = e
            if i < retries:
                time.sleep(backoff * (1.5 ** i) + random.uniform(0, 0.2))
    raise last


def get_links(landing_url: str, limit: int = 5):
    """
    Extract candidate article links from a news landing page.

    This function fetches the HTML content of the provided landing page URL,
    parses it with BeautifulSoup, and collects links that appear to be news articles.
    It filters out non-article links, off-site URLs, and common noise patterns.
    Only links from the same base domain are considered, and duplicates are avoided.

    Parameters
    ----------
    landing_url : str
        The URL of the news landing page to scrape for article links.
    limit : int, optional
        Maximum number of article links to return (default is 5).

    Returns
    -------
    list of tuple
        A list of tuples, each containing:
            - full URL (str)
            - class attribute (str)
            - role attribute (str)

    Notes
    -----
    - Uses browser-like headers for HTTP requests.
    - Filters out links containing common noise patterns (e.g., "#", "/live/", "/video").
    - Only returns links from the same base domain as the landing page.
    - Stops collecting links once the specified limit is reached.
    """
    r = _get(landing_url, timeout=25, retries=1)
    soup = BeautifulSoup(r.text, "lxml")

    links = []
    ext = tldextract.extract(landing_url)
    base_domain = f"{ext.domain}.{ext.suffix}"
    seen = set()

    # pick candidates early that look like articles
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        full = urljoin(landing_url, href)
        if not full.startswith("http"):
            continue
        # same-site guard, tolerate subdomains
        if base_domain not in full:
            continue
        # avoid obvious noise
        if any(x in full for x in ["#", "/live/", "/video", "/av/", "/media", "/terms", "/privacy", "/corrections"]):
            continue
        if full in seen:
            continue
        seen.add(full)
        cls = a.get("class") or ""
        role = a.get("role") or ""
        links.append((full, " ".join(cls) if isinstance(cls, list) else cls, role))
        if len(links) >= limit:
            break

    return links


def _extract_fallback(html: str):
    """
    Extract main article text from HTML as a fallback when readability fails.

    This function parses the provided HTML content using BeautifulSoup and concatenates
    the text from all paragraph (<p>) tags. It is used as a backup extraction method
    when the primary readability-based extraction yields insufficient content.
    If readability is too sparse, pull paragraph text directly.

    Parameters
    ----------
    html : str
        The raw HTML content of the web page.

    Returns
    -------
    str
        The concatenated text content from all paragraph tags in the HTML.

    Notes
    -----
    - Intended for use when readability extraction is too sparse or incomplete.
    - Joins paragraph texts with spaces and strips extra whitespace.
    """
    soup = BeautifulSoup(html, "lxml")
    paras = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
    text = " ".join(paras)
    return text


def fetch_article(url: str, class_hint: str = "", role_hint: str = ""):
    """
    Fetch and extract the main content and title from a news article URL.

    This function downloads the HTML content of the specified article URL, attempts to extract
    the article's title and main text using the readability library, and falls back to a
    paragraph-based extraction if the readability result is too short. It also applies a heuristic
    to determine if the extracted content is likely to be a valid news article.

    Parameters
    ----------
    url : str
        The URL of the news article to fetch and extract.
    class_hint : str, optional
        Optional class attribute hint from the link element (default is "").
    role_hint : str, optional
        Optional role attribute hint from the link element (default is "").

    Returns
    -------
    dict
        Dictionary containing:
            - 'url': the article URL (str)
            - 'title': the extracted article title (str)
            - 'text': the extracted main article text (str)
            - 'is_article_like': whether the content is likely a valid article (bool)

    Notes
    -----
    - Uses the readability library for initial extraction and BeautifulSoup for fallback.
    - Applies a minimum length and structural heuristic to validate article-likeness.
    - Falls back to concatenating all paragraph text if readability extraction is insufficient.
    """
    r = _get(url, timeout=30, retries=1)
    html = r.text

    # readability pass
    doc = Document(html)
    title = (doc.short_title() or "").strip()
    summary_html = doc.summary()
    soup = BeautifulSoup(summary_html, "lxml")
    text = " ".join(p.get_text(" ", strip=True) for p in soup.find_all(["p", "h2", "h3"])).strip()

    # fallback if text is too short
    if len(text) < 200:
        text = _extract_fallback(html).strip()

    # relax heuristic: long enough OR has 'article' container OR many sentences
    tag_hint = soup.find("article").name if soup.find("article") else (class_hint or role_hint)
    is_article = filter_article_like(text, tag_hint, min_len=200)

    return {
        "url": url,
        "title": title,
        "text": text,
        "is_article_like": bool(is_article)
    }

# one shared session for the whole module
def _make_session() -> requests.Session:
    """
    Creates a shared requests Session with realistic browser-like headers and retry policy.

    The session is configured to mimic a modern Chrome browser and will retry on common transient
    status codes (401, 403, 429, 5xx) with a backoff factor of 0.5 and maximum of 4 retries.

    Returns
    -------
    requests.Session
        A shared requests Session with the configured headers and retry policy.
    """
    s = requests.Session()
    # realistic browser-y headers to avoid 401/403 on reuters and friends
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1",
        "Referer": "https://www.google.com/",
    })

    # retry on common transient status codes (includes 401/403/429/5xx)
    retries = Retry(
        total=4,
        backoff_factor=0.5,
        status_forcelist=[401, 403, 429, 500, 502, 503, 504],
        allowed_methods={"GET", "HEAD"},
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

_SESSION = _make_session()

def http_get(url: str, timeout: float = 12.0, **kwargs) -> requests.Response:
    """
    Drop-in replacement for requests.get with headers + retries.
    """
    resp = _SESSION.get(url, timeout=timeout, allow_redirects=True, **kwargs)

    # super-light Reuters nicety: if they deny the bare root, try /world/
    # (no RSS, no extra logic — just a single alternate URL)
    try:
        netloc = urlparse(url).netloc
        path = urlparse(url).path or "/"
    except Exception:
        netloc, path = "", "/"

    if resp.status_code in (401, 403) and netloc.endswith("reuters.com") and path == "/":
        alt = "https://www.reuters.com/world/"
        resp = _SESSION.get(alt, timeout=timeout, allow_redirects=True, **kwargs)

    return resp

