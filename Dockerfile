FROM apache/airflow:2.9.1

ENV PROJECT_ROOT="/opt/airflow"

COPY requirements.txt /
COPY .env $PROJECT_ROOT
COPY ./config/wheels/* $PROJECT_ROOT/config/wheels/

RUN pip install --upgrade pip && \
    pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

USER root

RUN apt-get update && apt-get install -y \
    postgresql-client \
    openvpn \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \

USER airflow

# Set the working directory
WORKDIR $PROJECT_ROOT

