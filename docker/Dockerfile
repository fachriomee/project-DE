FROM apache/airflow:2.10.1

# Switch to root user to install system packages if needed
USER root

# Install build dependencies, Docker, and other utilities
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    build-essential \
    libsasl2-dev \
    libldap2-dev \
    libssl-dev \
    krb5-user \
    libkrb5-dev \
    iputils-ping \
    curl \
    net-tools \
    wget \
    telnet \
    libpq-dev \
    nano && \
    curl -fsSL https://get.docker.com | sh && \
    rm -rf /var/lib/apt/lists/*

# Add the airflow user to the docker group
RUN usermod -aG docker airflow

# Ensure /dev/net/tun exists
RUN mkdir -p /dev/net && mknod /dev/net/tun c 10 200 && chmod 600 /dev/net/tun

# Switch back to the Airflow user
USER airflow

# Install Python packages and Airflow providers in a single RUN
RUN python -m pip install --upgrade pip && \
    pip install --upgrade marshmallow && \
    pip install --upgrade openpyxl && \
    pip install psycopg2-binary && \
    pip install pymysql && \
    pip install pyarrow && \
    pip install "pandas" "requests" "pymongo" "clickhouse_driver" "redis" && \
    pip install "apache-airflow-providers-apache-spark" \
                "apache-airflow-providers-apache-hive" \
                "apache-airflow-providers-apache-hdfs" \
                "hdfs" \
                "apache-airflow-providers-celery" \
                "apache-airflow-providers-docker" && \
    pip install "python-dotenv" && \
    pip list && \
    pip cache purge

# Install PostgreSQL provider
RUN pip install apache-airflow-providers-postgres

# Upgrade the Airflow database
RUN airflow db upgrade