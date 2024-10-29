FROM apache/airflow:2.9.1

COPY requirements.txt /requirements.txt
COPY ./config/wheels/* /opt/airflow/config/wheels/

RUN pip install --upgrade pip && \
    pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
# RUN pip install pytest pytest-mock

USER root

RUN apt-get update && apt-get install -y \
    postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \

USER airflow

# Set the working directory
WORKDIR /opt/airflow

