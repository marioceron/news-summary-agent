import os
import io
from azure.storage.blob import BlobServiceClient
from .utils import now_ts_str, normalize_domain


def _client():
    """
    Create and return an Azure BlobServiceClient for blob storage operations.

    This function initializes the BlobServiceClient using either the
    AZURE_STORAGE_CONNECTION_STRING environment variable or, if not set,
    the AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY environment variables.

    Returns
    -------
    BlobServiceClient
        An authenticated client for Azure Blob Storage operations.

    Notes
    -----
    - Prefers connection string authentication if available.
    - Falls back to account/key authentication if connection string is not set.
    - Raises an exception if required environment variables are missing.
    """
    cs = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if cs:
        return BlobServiceClient.from_connection_string(cs)
    acct = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    return BlobServiceClient(account_url=f"https://{acct}.blob.core.windows.net", credential=key)


def store_extract(url: str, text: str, user_name: str, container: str):
    """
    Store the extracted text of a news article in Azure Blob Storage.

    This function generates a unique blob path based on the user name, normalized domain,
    and current timestamp, then uploads the provided text content to the specified Azure
    Blob Storage container.

    Parameters
    ----------
    url : str
        The URL of the news article being stored.
    text : str
        The extracted text content to upload.
    user_name : str
        The user name used to organize blobs in storage.
    container : str
        The name of the Azure Blob Storage container.

    Returns
    -------
    str
        The path of the blob where the extract was stored.

    Notes
    -----
    - Overwrites any existing blob at the same path.
    - Uses UTF-8 encoding for the text content.
    - The blob path format is: {user_name}/{normalized_domain}/extract-{timestamp}.txt
    """
    ts = now_ts_str()
    page = normalize_domain(url)
    path = f"{user_name}/{page}/extract-{ts}.txt"
    bs = _client()
    blob = bs.get_blob_client(container=container, blob=path)
    blob.upload_blob(io.BytesIO(text.encode("utf-8")), overwrite=True)
    return path
