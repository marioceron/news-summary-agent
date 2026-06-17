# modules/dedupe.py
from typing import Dict, List, Tuple, Callable
from modules import state

def filter_only_new(
    articles: List[Dict],
    key_fn: Callable[[Dict], str] = None
) -> Tuple[List[Dict], int]:
    """
    Returns (new_articles, touched_count).
    """
    store = state.get_store()
    store.ensure_schema()
    return store.filter_only_new(articles, key_fn=key_fn)
