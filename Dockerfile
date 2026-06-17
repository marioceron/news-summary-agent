# Dockerfile
FROM apache/airflow:2.10.2-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libxml2-dev libxslt1-dev poppler-utils \
 && rm -rf /var/lib/apt/lists/*

USER airflow
WORKDIR /opt/airflow

# Install Python dependencies
ARG AIRFLOW_VERSION=2.10.2
ARG PYTHON_VERSION=3.11
ARG CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir --constraint "${CONSTRAINTS_URL}" -r /opt/airflow/requirements.txt \
 && pip install --no-cache-dir --constraint "${CONSTRAINTS_URL}" psycopg2-binary \
 && pip cache purge \
 && rm -rf /tmp/*

# Ensure these exist at build time (mounts may override at runtime)
RUN mkdir -p /opt/airflow/dags /opt/airflow/modules /opt/airflow/include \
    /opt/airflow/logs /opt/airflow/plugins && \
    chmod -R 777 /opt/airflow/logs /opt/airflow/plugins

# Create writable dirs and hand them to airflow user (uid 50000)
USER root
RUN mkdir -p /opt/airflow/logs /opt/airflow/plugins /state \
 && chown -R 50000:0 /opt/airflow /state

USER 50000

# Pre-cache the embedding model so it's self-contained
RUN python - <<'PY'
from sentence_transformers import SentenceTransformer
SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2').save('/opt/airflow/model_cache/all-MiniLM-L6-v2')
print("Embedding model cached.")
PY
