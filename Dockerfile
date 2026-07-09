# ── Stage 1: build pyodbc wheel ──────────────────────────────────────────────
FROM python:3.11-slim AS builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc g++ unixodbc-dev && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip wheel --no-cache-dir --wheel-dir /tmp/wheels -r /tmp/requirements.txt gunicorn

# ── Stage 2: lean runtime image ──────────────────────────────────────────────
FROM python:3.11-slim

# 1) Add Microsoft repo and install ODBC Driver 18 + Azure CLI
#    so the driver's own runtime deps (libssl3, libstdc++, etc.) stay intact.
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl gnupg2 ca-certificates apt-transport-https lsb-release && \
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | \
        gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/microsoft-prod.gpg] \
https://packages.microsoft.com/debian/12/prod bookworm main" \
        > /etc/apt/sources.list.d/mssql-release.list && \
    curl -sL https://aka.ms/InstallAzureCLIDeb | bash && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
        msodbcsql17 msodbcsql18 unixodbc libltdl7 && \
    rm -rf /var/lib/apt/lists/* && \
    # Sanity-check: the .so must be loadable
    ls /opt/microsoft/msodbcsql17/lib64/ && \
    ls /opt/microsoft/msodbcsql18/lib64/ && \
    odbcinst -j && \
    az --version

# 2) Install Python deps from pre-built wheels (no compiler needed)
COPY --from=builder /tmp/wheels /tmp/wheels
RUN pip install --no-cache-dir /tmp/wheels/*.whl && rm -rf /tmp/wheels

WORKDIR /app

COPY migration_utility/ ./migration_utility/
COPY src/ ./src/
COPY resources/ ./resources/

WORKDIR /app/migration_utility

EXPOSE 8000

CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "2", "--threads", "8", "--timeout", "180", "app:app"]
