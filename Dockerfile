# Airflow basis (2.5.3, Debian 11)
FROM apache/airflow:2.5.3

ARG AIRFLOW_VERSION=2.5.3

# ── Python deps met Airflow constraints ─────────────────────────────────── ────
COPY requirements.txt /tmp/requirements.txt
RUN PYVER=$(python -c 'import sys; print(".".join(map(str, sys.version_info[:2])))') && \
    pip install --no-cache-dir -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYVER}.txt"

# ── OS deps: ODBC + GDAL/OGR + tools ─────────────────────────────────────────
USER root
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      curl gnupg ca-certificates \
      unixodbc unixodbc-dev odbc-postgresql \
      gdal-bin libgdal-dev proj-bin \
      wget nano htop \
 && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
 && echo "deb [arch=amd64] https://packages.microsoft.com/debian/11/prod bullseye main" \
      > /etc/apt/sources.list.d/mssql-release.list \
 && apt-get update \
 && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
 && ldconfig \
 && ogr2ogr --version && which ogr2ogr \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

# (optioneel) extra user – laat staan als je ‘m gebruikt
RUN useradd -m -s /bin/bash michel \
 && echo 'michel:190576' | chpasswd \
 && echo 'michel ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

# Voor sommige GDAL/proj builds is dit handig, kan blijven staan
ENV PROJ_LIB=/usr/share/proj

USER airflow


