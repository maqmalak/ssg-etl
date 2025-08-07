FROM apache/airflow:3.0.3

# Accept host UID at build time
ARG HOST_UID=50000
ARG HOST_GID=50000

USER root

# Update system and install build essentials
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Change airflow user and group to match host UID:GID
RUN groupmod -g ${HOST_GID} airflow && \
    usermod -u ${HOST_UID} -g ${HOST_GID} airflow

USER airflow

# Install Python dependencies
COPY requirements.txt /opt/airflow/
ENV PYTHONPATH=/opt/airflow/scripts:$PYTHONPATH
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
