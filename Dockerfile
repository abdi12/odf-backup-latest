# Debian-based image (docker / docker compose). For OpenShift builds see Dockerfile.ubi.
FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

# Microsoft ODBC Driver 18 for SQL Server
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl gnupg ca-certificates unixodbc tzdata \
 && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
      | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
 && curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list \
      > /etc/apt/sources.list.d/mssql-release.list \
 && apt-get update \
 && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
 && apt-get purge -y curl gnupg && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/odf-backup
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app

# Group 0 owns everything so the image also runs under an arbitrary UID (OpenShift).
RUN mkdir -p /data && chgrp -R 0 /opt/odf-backup /data && chmod -R g=u /opt/odf-backup /data
USER 10001
VOLUME ["/data"]

ENTRYPOINT ["python", "-m", "app"]
CMD ["run"]
