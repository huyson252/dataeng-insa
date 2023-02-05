FROM apache/airflow:2.1.3
COPY requirements.txt /requirements.txt
USER root
RUN apt-get update \
    && sudo apt-get install -y libpq-dev python-dev
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt