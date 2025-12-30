FROM apache/airflow:2.11.0

# Accept host UID and GID
ARG HOST_UID=50000
ARG HOST_GID=0

USER root

# -----------------------------
# System packages (your list)
# -----------------------------
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        unixodbc \
        unixodbc-dev \
        libodbc1 \
        odbcinst \
        iproute2 \
        net-tools \
        odbcinst1debian2 \
        freetds-bin \
        freetds-common \
        freetds-dev \
        libsybdb5 \
        libct4 \
        openjdk-17-jre-headless \
        tdsodbc \
        build-essential \
        ca-certificates \
        procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# -----------------------------
# Install Spark CLIENT binaries
# (required for SparkSubmitOperator: spark-submit)
# -----------------------------
ARG SPARK_VERSION=3.5.0
ARG HADOOP_PROFILE=hadoop3

ENV SPARK_HOME=/opt/spark
ENV PATH=${SPARK_HOME}/bin:${PATH}
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN curl -fSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}.tgz" -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    mv "/opt/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}" "${SPARK_HOME}" && \
    rm -f /tmp/spark.tgz

# Optional: make Spark pick up JDBC jars mounted from your repo
# You already mount ./sparkFiles -> /opt/airflow/sparkFiles
ENV SPARK_CLASSPATH=/opt/airflow/sparkFiles/jdbc-drivers/*

# -----------------------------
# Microsoft ODBC Driver 17
# -----------------------------
RUN curl -sSL https://packages.microsoft.com/debian/12/prod/pool/main/m/msodbcsql17/msodbcsql17_17.10.5.1-1_amd64.deb -o msodbcsql17.deb && \
    ACCEPT_EULA=Y dpkg -i msodbcsql17.deb && \
    rm msodbcsql17.deb

# Configure ODBC for FreeTDS
RUN echo "[FreeTDS]\nDescription=FreeTDS Driver\nDriver=/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so\nSetup=/usr/lib/x86_64-linux-gnu/odbc/libtdsS.so\nUsageCount=1" >> /etc/odbcinst.ini

# Configure FreeTDS
RUN echo "[global]\nTDS_Version = 7.0\nclient charset = UTF-8" > /etc/freetds/freetds.conf

# -----------------------------
# User/group alignment (your logic)
# -----------------------------
RUN if [ "${HOST_GID}" = "0" ]; then \
        getent group airflow || ( \
            groupadd -g ${HOST_GID} airflow 2>/dev/null || groupadd -g 50000 airflow \
        ); \
    else \
        getent group airflow || groupadd -g ${HOST_GID} airflow; \
    fi

RUN id -u airflow >/dev/null 2>&1 || \
    useradd -u ${HOST_UID} -g airflow -m airflow

RUN CURRENT_UID=$(id -u airflow) && \
    CURRENT_GID=$(id -g airflow) && \
    if [ "$CURRENT_UID" != "$HOST_UID" ]; then usermod -u ${HOST_UID} airflow; fi && \
    if [ "$CURRENT_GID" != "$HOST_GID" ]; then groupmod -g ${HOST_GID} airflow; fi

USER airflow

# -----------------------------
# Python deps (your requirements)
# -----------------------------
COPY requirements.txt /opt/airflow/
ARG PYTHONPATH
ENV PYTHONPATH=/opt/airflow/scripts${PYTHONPATH:+:$PYTHONPATH}

RUN pip install --upgrade pip && \
    pip install --no-cache-dir --timeout 1000 --retries 10 -r /opt/airflow/requirements.txt
