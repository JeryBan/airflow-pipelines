FROM apache/airflow:2.9.1

COPY requirements.txt /requirements.txt

RUN pip install --upgrade pip && \
    pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
# RUN pip install pytest pytest-mock

RUN apt-get update && apt-get install -y \
    postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /opt/airflow
