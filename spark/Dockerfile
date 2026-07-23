FROM apache/spark:4.0.0

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends software-properties-common \
    && add-apt-repository ppa:deadsnakes/ppa \
    && apt-get update \
    && apt-get install -y --no-install-recommends python3.13 python3.13-venv python3.13-dev \
    && rm -rf /var/lib/apt/lists/*

ENV PYSPARK_PYTHON=python3.13

USER spark
