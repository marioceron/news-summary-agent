import os
import smtplib
import ssl
from email.mime.text import MIMEText
from email.utils import parseaddr
from jinja2 import Template

TPL_PATH = "/opt/airflow/include/email_template.txt.j2"


def _render(items: list[dict], topics: list[str], your_name: str):
    """
    Render the email subject and plain text body using a Jinja2 template.

    This function loads the email template from TPL_PATH, renders it with the provided
    news items, topics, and sender name, and extracts the subject and plain text body
    from the rendered output.

    Parameters
    ----------
    items : list of dict
        List of news summary dictionaries to include in the email.
    topics : list of str
        List of topics relevant to the news digest.
    your_name : str
        Name of the sender to personalize the email.

    Returns
    -------
    tuple
        subject : str
            The rendered email subject line.
        plain : str
            The rendered plain text email body.

    Notes
    -----
    - The template must include a "Subject:" line as the first line.
    - Uses Jinja2 for template rendering.
    """
    with open(TPL_PATH) as f:
        tmpl = Template(f.read())
    body = tmpl.render(items=items, topics=topics, your_name=your_name)
    first, *rest = body.splitlines()
    subject = first.replace("Subject:", "").strip()
    plain = "\n".join(rest).lstrip()
    return subject, plain


def _split_recipients(s: str | None):
    """
    Split a string of email recipients into a list of individual addresses.

    This function takes a string containing email addresses separated by commas or semicolons,
    trims whitespace, and returns a list of cleaned email addresses. If the input is None or empty,
    an empty list is returned.

    Parameters
    ----------
    s : str or None
        String containing email addresses separated by commas or semicolons.

    Returns
    -------
    list of str
        List of individual, trimmed email addresses.

    Notes
    -----
    - Handles both comma and semicolon as separators.
    - Ignores empty or whitespace-only entries.
    """
    if not s:
        return []
    return [addr.strip() for addr in s.replace(";", ",").split(",") if addr.strip()]


def send_email(items: list[dict], topics: list[str], your_name: str):
    """
    Send a news digest email with the provided news summaries and topics.

    This function prepares and sends an email using SMTP, rendering the subject and body
    from a Jinja2 template. It collects SMTP configuration from environment variables,
    builds the message, and sends it to the recipients specified in the SEND_TO variable.

    Parameters
    ----------
    items : list of dict
        List of news summary dictionaries to include in the email.
    topics : list of str
        List of topics relevant to the news digest.
    your_name : str
        Name of the sender to personalize the email.

    Returns
    -------
    bool
        True if the email was sent (or dry-run was successful), otherwise raises an exception.

    Notes
    -----
    - Uses SMTP for email delivery; configuration is controlled via environment variables.
    - Supports dry-run mode for testing (DRY_RUN_EMAIL).
    - Raises RuntimeError if required environment variables are missing.
    """
    # Transport is forced to SMTP for this setup
    return _send_via_smtp(items, topics, your_name)


def _send_via_smtp(items, topics, your_name):
    """
    Send a news digest email using SMTP with the provided news summaries and topics.

    This function renders the email subject and body using a Jinja2 template, collects SMTP
    configuration from environment variables, builds the email message, and sends it to the
    recipients specified in the SEND_TO environment variable. Supports both SSL and TLS connections,
    as well as a dry-run mode for testing.

    Parameters
    ----------
    items : list of dict
        List of news summary dictionaries to include in the email.
    topics : list of str
        List of topics relevant to the news digest.
    your_name : str
        Name of the sender to personalize the email.

    Returns
    -------
    bool
        True if the email was sent successfully or if dry-run mode is enabled.

    Notes
    -----
    - SMTP configuration is controlled via environment variables:
        SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD,
        EMAIL_FROM, SMTP_USE_SSL, SMTP_USE_TLS, SMTP_TIMEOUT,
        SMTP_DEBUG, DRY_RUN_EMAIL, SEND_TO.
    - Requires SMTP_USER and SMTP_PASSWORD to be set.
    - Raises RuntimeError if required environment variables are missing.
    - In dry-run mode, prints the email content instead of sending.
    - Supports both SSL (typically port 465) and TLS (typically port 587).
    """
    subject, plain = _render(items, topics, your_name)

    # Env
    host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    port = int(os.getenv("SMTP_PORT", "465"))
    use_ssl_flag = os.getenv("SMTP_USE_SSL", "false").lower() in ("1", "true", "yes")
    use_tls_flag = os.getenv("SMTP_USE_TLS", "false").lower() in ("1", "true", "yes")
    timeout = int(os.getenv("SMTP_TIMEOUT", "45"))
    debug = int(os.getenv("SMTP_DEBUG", "0"))
    dry_run = os.getenv("DRY_RUN_EMAIL", "false").lower() in ("1", "true", "yes")

    user = os.getenv("SMTP_USER")
    pwd = os.getenv("SMTP_PASSWORD")
    from_hdr = os.getenv("EMAIL_FROM", "")
    _, from_addr = parseaddr(from_hdr)
    to_addrs = _split_recipients(os.getenv("SEND_TO"))

    if not to_addrs:
        raise RuntimeError("SEND_TO is required for SMTP and may contain comma/semicolon-separated addresses.")

    if not user or not pwd:
        raise RuntimeError("SMTP_USER and SMTP_PASSWORD are required (use an AOL App Password).")

    # AOL/Yahoo typically require From == authenticated user
    if not from_addr or from_addr.lower() != user.lower():
        from_hdr = f'"News Agent" <{user}>'
        from_addr = user

    # Decide TLS/SSL behavior
    use_ssl = use_ssl_flag or (port == 465)
    use_tls = use_tls_flag or (not use_ssl and port == 587)

    # Build message
    msg = MIMEText(plain, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = from_hdr
    msg["To"] = ", ".join(to_addrs)

    # Dry-run support (logs but does not send)
    if dry_run:
        print("[DRY_RUN_EMAIL] Subject:", subject)
        print("[DRY_RUN_EMAIL] From:", from_hdr)
        print("[DRY_RUN_EMAIL] To:", ", ".join(to_addrs))
        print("[DRY_RUN_EMAIL] Body:\n", plain)
        return True

    # Send
    if use_ssl:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(host, port, timeout=timeout, context=context) as s:
            if debug: s.set_debuglevel(1)
            s.login(user, pwd)
            s.send_message(msg)
    else:
        with smtplib.SMTP(host, port, timeout=timeout) as s:
            if debug: s.set_debuglevel(1)
            s.ehlo()
            if use_tls:
                s.starttls(context=ssl.create_default_context())
                s.ehlo()
            s.login(user, pwd)
            s.send_message(msg)

    return True
