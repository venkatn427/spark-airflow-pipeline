FROM apache/airflow:3.2.1

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=/opt/spark/bin:${JAVA_HOME}/bin:${PATH}

USER airflow
RUN mkdir -p /home/airflow/.ivy2/cache /home/airflow/.ivy2/jars /tmp/.ivy
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
