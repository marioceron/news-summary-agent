import os
import json
import re
import time
import logging
from typing import Tuple

# Try lightweight local detection first to avoid LLM calls.
try:
    from langdetect import detect  # pip install langdetect
except Exception:
    detect = None

# OPENAI_API_KEY may be None
_OPENAI_KEY = os.getenv("OPENAI_API_KEY")
_OPENAI_MODEL = os.getenv("OPENAI_TRANSLATE_MODEL", "gpt-4o-mini")  # optional override

# NEW: set a default timeout (seconds) and 1 quick retry
_OPENAI_TIMEOUT = int(os.getenv("OPENAI_TIMEOUT", "15"))
_OPENAI_RETRIES = int(os.getenv("OPENAI_RETRIES", "1"))

_DEFAULT_EN_DOMAINS = set(d.strip().lower() for d in os.getenv(
    "DEFAULT_EN_DOMAINS",
    "reuters.com;apnews.com;bbc.com;theguardian.com;cnbc.com;espn.com;skysports.com;aljazeera.com"
).split(";") if d.strip())


def _detect_lang(text: str) -> str:
    """
    Best-effort language detection for a given text string.
    Avoid forcing 'en' for very short strings if a longer
    companion string exists (caller should pass title+text together for better signal).

    This function attempts to detect the language of the input text using the langdetect library,
    if available. For very short strings or if langdetect is unavailable or fails, it falls back
    to a simple keyword-based heuristic to distinguish between English and Spanish. If the language
    cannot be confidently determined, returns "unknown".

    Parameters
    ----------
    text : str
        The text to analyze for language detection.

    Returns
    -------
    str
        The detected language code ("en" for English, "es" for Spanish, or "unknown" if undetermined).

    Notes
    -----
    - For best results, pass both title and body text together to improve detection accuracy.
    - The function avoids forcing "en" for very short strings.
    - If langdetect is not installed or fails, uses a keyword-based heuristic.
    """
    s = (text or "").strip()
    if len(s) < 60:
        return "en"
    # langdetect is noisy under ~20 chars, but your bodies are much longer.
    if detect:
        try:
            return detect(s) or "unknown"
        except Exception:
            pass
    # Fallback heuristic
    s_low = f" {s.lower()} "
    es_hits = sum(w in s_low for w in [" el ", " la ", " de ", " que ", " en ", " y ", " los ", " para "])
    en_hits = sum(w in s_low for w in [" the ", " of ", " and ", " in ", " for ", " with ", " to "])
    if es_hits > max(1, en_hits) * 1.5:
        return "es"
    if en_hits > max(1, es_hits) * 1.5:
        return "en"
    # Default to unknown so we give the translator a chance if needed
    return "unknown"


def _coerce_json(text: str) -> dict:
    """
    Robustly extract and parse a JSON object from a string, tolerating common formatting issues.

    This function attempts to parse a JSON object from the input string, handling cases such as:
      - Markdown-style code fences (e.g., ```json ... ```)
      - Leading 'json' tokens or lines
      - Extra prose or text before or after the JSON object

    If direct parsing fails, it searches for the largest {...} block in the string and tries to parse that.
    Returns an empty dictionary if no valid JSON object can be extracted.

    Parameters
    ----------
    text : str
        The input string potentially containing a JSON object.

    Returns
    -------
    dict
        The parsed JSON object as a dictionary, or an empty dictionary if parsing fails.

    Notes
    -----
    - Designed for use with LLM or API responses that may include extra formatting or explanations.
    - Ignores text outside the main JSON object.
    """
    if not text:
        return {}
    cleaned = text.strip()

    # Strip backticks fences if present
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", cleaned)  # remove ```json or ``` fence start
        cleaned = re.sub(r"\s*```$", "", cleaned)                # remove closing ```

    # If starts with a literal 'json' line, drop it
    cleaned = re.sub(r"^\s*json\s*\n", "", cleaned, flags=re.IGNORECASE)

    # Try full parse first
    try:
        return json.loads(cleaned)
    except Exception:
        pass

    # Fallback: extract the largest {...} block
    m = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except Exception:
            pass

    return {}


def _translate_with_openai(title: str, text: str) -> Tuple[str, str]:
    """
    Translate a news article's title and text to English using OpenAI's GPT model.

    Cheap translation: enforce JSON object, low temperature, small body window.
    Retries a couple times if parsing fails.
    This function sends the provided title and text to the OpenAI API, requesting a JSON object
    with 'title_en' and 'text_en' keys. It enforces a low temperature for deterministic output,
    limits the body length for cost and speed, and retries up to three times if parsing fails.
    If translation or JSON parsing fails after retries, it returns the original title and text.

    Parameters
    ----------
    title : str
        The title of the article to translate.
    text : str
        The main text content of the article to translate.

    Returns
    -------
    tuple
        (title_en, text_en)
        title_en : str
            The translated article title in English.
        text_en : str
            The translated article text in English.

    Notes
    -----
    - Uses the OPENAI_API_KEY and OPENAI_TRANSLATE_MODEL environment variables for configuration.
    - Limits the text sent to the API to the first 5000 characters.
    - Falls back to the original title and text if translation fails.
    - Logs translation attempts and failures for debugging.
    """
    from openai import OpenAI
    client = OpenAI(api_key=_OPENAI_KEY, timeout=_OPENAI_TIMEOUT)

    max_body = text[:5000] if text else ""
    user_prompt = (
        "Translate the following news article to English.\n"
        "Return a JSON object with exactly these keys: title_en, text_en.\n"
        "Keep names, places and numerals faithful. Do not add commentary.\n\n"
        f"TITLE:\n{title or ''}\n\nBODY:\n{max_body}"
    )

    last_err = None
    for attempt in range(_OPENAI_RETRIES + 1):
        try:
            rsp = client.chat.completions.create(
                model=_OPENAI_MODEL,
                messages=[{"role": "user", "content": user_prompt}],
                temperature=0.0,
                # Ask for JSON object; many SDKs/models support this now
                response_format={"type": "json_object"},
                timeout=30,
            )
            content = (rsp.choices[0].message.content or "").strip()
            logging.info("[TRANSLATE] OpenAI raw: %s", content)
            data = _coerce_json(content)
            if isinstance(data, dict) and ("title_en" in data or "text_en" in data):
                title_out = (data.get("title_en") or title or "").strip()
                text_out = (data.get("text_en") or max_body or text or "").strip()
                logging.info("[TRANSLATE] Parsed JSON OK")
                return (title_out, text_out)
            raise ValueError("JSON missing expected keys")
        except Exception as e:
            last_err = e
            logging.warning("[TRANSLATE] attempt %d failed: %r", attempt + 1, e)
            time.sleep(0.8 * (attempt + 1))  # small backoff

    logging.warning("[TRANSLATE] Falling back to original after failures: %r", last_err)
    return (title or "", max_body or "")


def ensure_english(title: str, text: str, url: str = "") -> Tuple[str, str, str]:
    """
    Ensure that a news article's title and text are in English, translating if necessary.
    Only calls OpenAI if detected language is not clearly English.

    This function detects the language of the combined title and text. If the language is clearly English,
    it returns the original title and text. If the language is not English and an OpenAI API key is available,
    it uses OpenAI's GPT model to translate the title and text to English. If translation fails or the API key
    is missing, it returns the original values. The detected language code is always returned.

    Parameters
    ----------
    title : str
        The title of the article.
    text : str
        The main text content of the article.
    url : str, optional (default="")
        The URL of the article (used for logging purposes).

    Returns
    -------
    tuple
        (title_en, text_en, lang_detected)
        title_en : str
            The English version of the article title.
        text_en : str
            The English version of the article text.
        lang_detected : str
            The detected language code ("en", "es", "unknown", etc.).

    Notes
    -----
    - Uses a combination of langdetect and heuristics for language detection.
    - Only calls the OpenAI translation API if the language is not clearly English.
    - Falls back to the original title and text if translation is not possible.
    - Logs translation attempts and decisions for debugging.
    """
    host = ""
    try:
        from urllib.parse import urlparse
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        pass   
    # Fast path: known English host
    if any(h in host for h in _DEFAULT_EN_DOMAINS):
        return (title or "", text or "", "en")
    
    # Use combined text for detection signal
    combined = (" ".join([title or "", text or ""])).strip()
    lang = _detect_lang(combined)

    if lang in ("en", "unknown"):
        # If 'unknown', still try to be smart: quick heuristic—if there are clear non-ASCII characters,
        # give the translator a chance. Otherwise treat as English to save cost.
        if lang == "unknown" and any(ord(c) > 127 for c in (combined[:2000])):
            lang = "non-en"
        else:
            logging.info("[TRANSLATE] Treating as English: %r", (title or "", (text or "")[:120], lang))
            return (title or "", text or "", "en")

    if not _OPENAI_KEY:
        logging.info("[TRANSLATE] Non-English (%s) but OPENAI_API_KEY missing; using original", lang)
        return (title or "", text or "", lang)

    try:
        title_en, text_en = _translate_with_openai(title or "", text or "")
        logging.info("[TRANSLATE] -> English (lang=%s), title_len=%d text_len=%d",
                     lang, len(title_en or ""), len(text_en or ""))
        return (title_en or title or "", text_en or text or "", lang)
    except Exception as e:
        logging.warning("[TRANSLATE] OpenAI translate error: %r; using original", e)
        return (title or "", text or "", lang)