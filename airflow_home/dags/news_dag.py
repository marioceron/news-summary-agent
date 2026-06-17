import sys
import os
import logging
sys.path.insert(0, "/opt/airflow/modules")
from airflow.decorators import dag, task
from datetime import datetime
import numpy as np
from datetime import timedelta
import tldextract
from modules import scraper, storage, summarizer, dedupe, state
from modules.utils import hash_text
# from sentence_transformers import SentenceTransformer


@dag(
    schedule="*/15 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["hackathon-news"],
    default_args={"owner": "news-agent"},
)
def news_summary_agent():
    """
    Airflow DAG for automated news scraping, deduplication, summarization, and email delivery.

    This DAG orchestrates a pipeline that:
      1. Loads configuration from environment variables and Database
      2. Optionally initializes persistent state for deduplication.
      3. Scrapes news articles from configured outlets.
      4. Stores raw article extracts to Azure Blob Storage.
      5. Selects articles relevant to user-specified topics using semantic similarity.
      6. Deduplicates articles and filters out previously processed items.
      7. Summarizes relevant articles for email presentation.
      8. Sends the summarized news digest to the user via email.

    The pipeline runs every 15 minutes and is designed for robust, automated operation,
    with logging and retries for transient failures.

    Returns
    -------
    None

    Notes
    -----
    - Each step is implemented as an Airflow task for modularity and observability.
    - Configuration is controlled via environment variables for flexibility.
    - Uses custom modules for scraping, storage, deduplication, summarization, and email delivery.
    """
    
    @task
    def load_config():
        """
        Load the configuration from environment variables.

        Returns a dictionary containing the following:

        - outlets: list of news outlets to scrape
        - send_to: email address to send the news summaries
        - topics: list of topics to filter the news summaries
        - user_name: name of the user
        - container: name of the Azure blob storage container
        - your_name: name of the user for the email subject

        Environment variables:

        - LIST_NEWS_OUTLETS: semicolon-separated list of news outlets
        - SEND_TO: email address to send the news summaries
        - INTEREST_TOPICS: semicolon-separated list of topics to filter the news summaries
        - USER_NAME: name of the user
        - AZURE_CONTAINER_NAME: name of the Azure blob storage container
        - YOUR_NAME_FOR_SUBJECT: name of the user for the email subject
        """
        outlets = os.getenv("LIST_NEWS_OUTLETS", "").split(";")
        outlets = [u.strip() for u in outlets if u.strip()]
        send_to = os.getenv("SEND_TO")
        topics = [t.strip().lower() for t in os.getenv("INTEREST_TOPICS", "general").split(";") if t.strip()]
        user_name = os.getenv("USER_NAME", "user")
        container = os.getenv("AZURE_CONTAINER_NAME", "news-extracts")
        your_name = os.getenv("YOUR_NAME_FOR_SUBJECT", "Your Name")

        cfg = {
            "outlets": outlets,
            "send_to": send_to,
            "topics": topics,
            "user_name": user_name,
            "container": container,
            "your_name": your_name,
        }
        return cfg

    @task
    def init_state():
        """
        Initialize the persistent state for the news summary agent.

        This function sets up or resets the state database or storage
        used to track processed articles and deduplication information.
        It should be called before any processing to ensure the state
        is ready for use by downstream tasks.

        Returns
        -------
        bool
            True if the state was initialized successfully.
        """
        # Do DB work at runtime, not parse time
        from modules import state
        # Make sure the store points at the right DSN
        dsn = os.getenv("STATE_DB_URL") or "postgresql+psycopg2://admin:admin@db:5432/airflow"
        logging.info("[STATE] using DSN: %s", dsn.replace("admin:admin@", "****:****@"))
        store = state.get_store(dsn=dsn) if "get_store" in dir(state) else state.get_store()
        store.ensure_schema()
        return True

    @task
    def scrape(cfg):
        """
        Scrape news articles from given outlets.

        Parameters
        ----------
        cfg : dict
            Configuration dictionary containing the following:

            - outlets: list of news outlets to scrape
            - send_to: email address to send the news summaries
            - topics: list of topics to filter the news summaries
            - user_name: name of the user
            - container: name of the Azure blob storage container
            - your_name: name of the user for the email subject

        Returns
        -------
        list
            A list of dictionaries, each containing the article data and its source.
        """
        results = []
        for site in cfg["outlets"]:
            found, kept = 0, 0
            try:
                links = scraper.get_links(site, limit=5)
                print(f"[SCRAPE] {site} -> candidate links: {len(links)}")
            except Exception as e:
                print(f"[SCRAPE] {site} FAILED to fetch links: {e!r}")
                continue

            for url, cls, role in links:
                try:
                    art = scraper.fetch_article(url, cls, role)
                    found += 1
                except Exception as e:
                    print(f"[SCRAPE] fetch failed: {url} -> {e!r}")
                    continue

                if art.get("is_article_like") and len(art.get("text", "")) >= 200:
                    ext = tldextract.extract(url)
                    source = f"{ext.domain}.{ext.suffix}"
                    results.append({**art, "source": source})
                    kept += 1
                else:
                    print(f"[SCRAPE] discarded (not article-like or too short): {url}")

            print(f"[SCRAPE] {site} -> fetched {found}, kept {kept}")

        print(f"[SCRAPE] total kept: {len(results)}")
        logging.info("[SCRAPE] total articles fetched: %d", len(results))
        return results

    @task
    def store_to_azure(cfg, articles):
        """
        Store raw news article extracts to Azure Blob Storage.

        Parameters
        ----------
        cfg : dict
            Configuration dictionary containing user and container information.
        articles : list
            List of article dictionaries, each containing at least 'url' and 'text' fields.

        Returns
        -------
        list
            A list of dictionaries, each with 'url' and the corresponding 'azure_path' where the article was stored.

        Notes
        -----
        - Each article is stored using the storage module's `store_extract` function.
        - Articles that fail to store do not block the rest from being processed.
        - Logging is used to record successful storage operations.
        """
        # Raw extracts to Azure in required path format
        paths = []
        for a in articles:
            try:
                p = storage.store_extract(a["url"], a["text"], cfg["user_name"], cfg["container"])
                paths.append({"url": a["url"], "azure_path": p})
                logging.info("[STORE] stored %s -> %s", a["url"], p)
            except Exception:
                # don't block the rest
                pass
        return paths

    @task(
        retries=2,
        retry_exponential_backoff=True,
        execution_timeout=timedelta(minutes=10),
    )
    def select_relevant(cfg, articles):
        """
        Filter and select news articles relevant to user-specified topics using semantic similarity.

        This function uses a sentence transformer model to embed both user topics and article content,
        then computes cosine similarity to determine relevance. Articles are translated to English if needed,
        and a keyword-based boost is applied to improve topic detection. Only articles above a similarity
        threshold are kept.

        Parameters
        ----------
        cfg : dict
            Configuration dictionary containing user topics and other settings.
        articles : list
            List of article dictionaries, each containing at least 'title' and 'text'.

        Returns
        -------
        list
            A list of article dictionaries that are relevant to the user's topics, each with added fields:
            - 'topic': the best-matching topic label
            - 'topic_score': the similarity score
            - 'title_en', 'text_en', 'lang': English translation and detected language

        Notes
        -----
        - Uses a pre-trained sentence transformer model for embeddings.
        - Applies a keyword boost for more accurate topic assignment.
        - Articles with very short content are skipped.
        - The similarity threshold can be set via the TOPIC_SIM_THRESHOLD environment variable.
        """        
        if not articles:
            return []
        # Lazy import heavy libs at runtime, not at DAG-parse time
        from sentence_transformers import SentenceTransformer
        import numpy as np
        from modules import translator
        import logging
        import time
        import os

        # Config
        user_labels = [t.strip().lower() for t in cfg["topics"]]  # e.g., ["politics","business","sports"]
        THRESH = float(os.getenv("TOPIC_SIM_THRESHOLD", "0.20"))
        BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", "32"))
        MAX_BODY_CHARS = int(os.getenv("CLASSIFY_BODY_MAX", "1200"))
        MAX_ARTICLES = int(os.getenv("MAX_CLASSIFY_ARTICLES", "200"))  # safety valve
        SBERT_MAX_SEQ_LEN = int(os.getenv("SBERT_MAX_SEQ_LEN", "256"))
        # Map each short label to a richer sentence the embedder can latch onto
        LABEL_TEXTS = {
            "politics": (
                "An article about politics: governments, elections, public policy, "
                "diplomacy, parliaments, presidents, lawmakers, protests, geopolitical news."
            ),
            "business": (
                "An article about business and finance: companies, markets, earnings, mergers, "
                "economics, inflation, central banks, jobs, investors, industries."
            ),
            "sports": (
                "An article about sports: matches, athletes, teams, tournaments, scores, "
                "leagues, transfers, injuries, championships."
            ),
        }
        # Build label sentences in the order of user_labels; if missing, fall back to the raw label
        label_texts = [LABEL_TEXTS.get(lbl, f"An article about {lbl}.") for lbl in user_labels]

        # Light keyword backstop (after translation). Tweak if you like.
        KEYWORDS = {
            "politics": ["president", "minister", "congress", "senate", "election", "policy", "diplomacy",
                        "government", "parliament", "UN", "treaty", "sanction", "governor", "mayor"],
            "business": ["stock", "revenue", "profit", "earnings", "IPO", "merger", "acquisition",
                        "bank", "loan", "market", "inflation", "GDP", "trade", "company", "startup"],
            "sports":   ["match", "goal", "coach", "player", "league", "cup", "tournament",
                        "score", "fixture", "transfer", "injury", "win", "defeat", "Ryder Cup", "World Cup"],
        }
        # Trim to a sane cap to avoid runaway processing if a source floods
        items = (articles or [])[:MAX_ARTICLES]
        t0 = time.time()
        model = SentenceTransformer("/opt/airflow/model_cache/all-MiniLM-L6-v2")
        # shorter sequence for speed (adjust as you like)
        try:
            model.max_seq_length = SBERT_MAX_SEQ_LEN
        except Exception:
            pass

        label_vecs = model.encode(user_labels, normalize_embeddings=True)
        bodies, back_refs = [], []
        picked = []

        # 1) Pre-translate (only when needed) and build bodies first
        for idx, a in enumerate(items):
                title = (a.get("title") or "").strip()
                text = (a.get("text") or "").strip()
                url = (a.get("url") or "")

                # Translate first
                t_en, x_en, lang = translator.ensure_english(title, text, url=url)
                a["lang"] = lang
                a["title_en"] = t_en
                a["text_en"] = x_en

                # Skip non-articles/landing pages (helps avoid noise like site nav pages)
                body = (t_en + " " + (x_en[:MAX_BODY_CHARS] if x_en else "")).strip()
                if not body:
                    continue
                bodies.append(body)
                back_refs.append(idx)
        # 2) Batch-encode article bodies
        if bodies:
            vecs = model.encode(bodies, normalize_embeddings=True, batch_size=BATCH_SIZE)
            vecs = np.asarray(vecs)  # (N, D)
            sims = vecs @ label_vecs.T  # (N, L)

            for i, idx in enumerate(back_refs):
                a = items[idx]
                row = sims[i]
                best_i = int(np.argmax(row))
                best_sim = float(row[best_i])
                best_topic = user_labels[best_i]
                # if best_sim >= THRESH:
                #    picked.append({**a, "topic": best_topic, "topic_score": best_sim})

                # Keyword boost: if obvious keywords for best_topic appear, give it a nudge
                kw_boost = 0.0
                if best_topic in KEYWORDS:
                    lower_body = body.lower()
                    hits = sum(1 for kw in KEYWORDS[best_topic] if kw.lower() in lower_body)
                    if hits >= 2:
                        kw_boost = 0.06  # small, safe bump
                score = best_sim + kw_boost

                logging.info(
                    "[FILTER] #%d topic=%s sim=%.3f boost=%.3f score=%.3f | %s",
                    idx, best_topic, best_sim, kw_boost, score, (t_en or title)[:90]
                )

                if score >= THRESH:
                    picked.append({**a, "topic": best_topic, "topic_score": score})

        logging.info("[FILTER] topics=%s -> kept %d/%d items (threshold=%.2f) in %.2fs",
                     user_labels, len(picked), len(items), THRESH, time.time() - t0)
        return picked

    @task
    def deduplicate_and_only_new(cfg, articles):
        """
        Deduplicate articles and filter out items already processed in previous runs.

        This function checks each article to determine if it has already been seen
        (using a persistent state database) or if it is a near-duplicate of another article
        (using a deduplication index). Only new and unique articles are returned for further processing.

        Parameters
        ----------
        cfg : dict
            Configuration dictionary (not directly used, but included for Airflow consistency).
        items : list
            List of article dictionaries, each containing at least 'title', and 'text'.

        Returns
        -------
        list
            A list of new, unique article dictionaries to be summarized.
            Return items to summarize (dedup + not seen before).

        Notes
        -----
        - Uses the dedupe module to detect near-duplicates based on title and text.
        - Updates the state and deduplication index for each new article.
        - Logs the number of unique items kept.
        """
        import os
        import logging

        if not articles:
            return []
        new_articles, touched = dedupe.filter_only_new(
            articles,
            key_fn=lambda a: f"{a.get('source','')}\n{a.get('title_en') or a.get('title','')}\n{(a.get('text_en') or a.get('text',''))[:1200]}"
        )
        logging.info("[STATE] kept %d new of %d (touched=%d)", len(new_articles), len(articles), touched)
        return new_articles

    @task
    def summarize_for_email(cfg, items):
        """
        Generate concise summaries for news articles to be included in an email digest.

        This function takes a list of article dictionaries, extracts their titles, texts, and topics,
        and uses the summarizer module to produce a short summary for each article. The output is a list
        of dictionaries suitable for email presentation.

        Parameters
        ----------
        cfg : dict
            Configuration dictionary (not directly used, but included for Airflow consistency).
        items : list
            List of article dictionaries, each containing at least 'title'/'title_en', 'text'/'text_en', and optionally 'topic'.

        Returns
        -------
        list
            A list of dictionaries, each with:
                - 'title': the article title (in English if available)
                - 'topic': the assigned topic or "general"
                - 'summary': the generated summary text

        Notes
        -----
        - Uses the summarizer module to generate summaries.
        - Falls back to original title/text if English versions are not available.
        - Skips articles if summarization fails.
        - Logs the number of summaries produced.
        """
        import logging
        out = []
        for it in items or []:
            try:
                t = it.get("title_en") or it.get("title") or ""
                x = it.get("text_en") or it.get("text") or ""
                topic = it.get("topic")  # however you assign topic downstream
                summary = summarizer.summarize(t, x, topic or "general")
                out.append({"title": t, "topic": topic or "general", "summary": summary})
                logging.info("[SUM] produced %d summaries", len(out))
            except Exception:
                # if LLM call fails, skip that item but keep the run healthy
                pass
        return out

    # Retry is done here to handle transient email sending issues
    @task(
        retries=4,
        retry_exponential_backoff=True,
        retry_delay=timedelta(minutes=1),
        max_retry_delay=timedelta(minutes=10)
    )
    def send_email(cfg, summaries):
        """
        Send the summarized news articles to the user via email.

        This function takes the generated summaries and user configuration,
        then sends an email digest using the emailer module.

        Parameters
        ----------
        cfg : dict
            Configuration dictionary containing recipient email, topics, and user name for the subject.
        summaries : list
            List of summary dictionaries, each containing at least 'title', 'topic', and 'summary'.

        Returns
        -------
        None

        Notes
        -----
        - Uses the modules.emailer.send_email function to send the email.
        - Logs the number of items sent and the recipient address.
        - Retries sending on failure, with exponential backoff.
        """
        import logging
        from modules.emailer import send_email as _send
        logging.info("[EMAIL] sending %d items to %s", len(summaries), cfg["send_to"])
        _send(summaries, cfg["topics"], cfg["your_name"])

    cfg = load_config()
    _init = init_state()
    scraped = scrape(cfg)
    _stored = store_to_azure(cfg, scraped)
    relevant = select_relevant(cfg, scraped)
    unique = deduplicate_and_only_new(cfg, relevant)
    summaries = summarize_for_email(cfg, unique)

    send_email(cfg, summaries)


news_summary_agent()
