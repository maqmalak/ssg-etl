FROM apache/airflow:2.11.0

# Accept host UID and GID
ARG HOST_UID=50000
ARG HOST_GID=50000

USER root

# Install system build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Ensure group exists (create only if missing)
RUN getent group airflow || groupadd -g ${HOST_GID} airflow

# Ensure user exists (create only if missing)
RUN id -u airflow >/dev/null 2>&1 || \
    useradd -u ${HOST_UID} -g airflow -m airflow

# If user already exists, adjust UID/GID only if needed
RUN CURRENT_UID=$(id -u airflow) && \
    CURRENT_GID=$(id -g airflow) && \
    if [ "$CURRENT_UID" != "$HOST_UID" ]; then usermod -u ${HOST_UID} airflow; fi && \
    if [ "$CURRENT_GID" != "$HOST_GID" ]; then groupmod -g ${HOST_GID} airflow; fi

USER airflow

# Copy and install Python dependencies
COPY requirements.txt /opt/airflow/
ENV PYTHONPATH=/opt/airflow/scripts:$PYTHONPATH
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
