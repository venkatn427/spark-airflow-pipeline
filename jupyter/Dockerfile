FROM eclipse-temurin:17-jre-jammy

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       python3.11 python3.11-venv python3-pip curl \
    && rm -rf /var/lib/apt/lists/* \
    && python3.11 -m pip install --upgrade pip

ENV JAVA_HOME=/opt/java/openjdk
ENV PYSPARK_PYTHON=python3.11
ENV PYSPARK_DRIVER_PYTHON=python3.11

COPY requirements.txt /tmp/requirements.txt
RUN python3.11 -m pip install --no-cache-dir -r /tmp/requirements.txt

# Download S3A JARs at build time into /opt/spark-jars/jars/.
# Then write their absolute paths to jars.txt so notebooks can reference them
# via spark.jars (direct paths) — completely bypassing ivy at runtime, which
# eliminates the ":: resolving dependencies ::" output from notebook cells.
RUN python3.11 -c "\
from pyspark.sql import SparkSession; \
spark = SparkSession.builder.master('local') \
    .config('spark.jars.packages', \
            'org.apache.hadoop:hadoop-aws:3.4.0,' \
            'com.amazonaws:aws-java-sdk-bundle:1.12.262') \
    .config('spark.jars.ivy', '/opt/spark-jars') \
    .getOrCreate(); \
spark.stop()" \
&& find /opt/spark-jars/jars -name '*.jar' | sort | tr '\n' ',' | sed 's/,$//' \
   > /opt/spark-jars/jars.txt \
&& echo "Baked JARs:" && cat /opt/spark-jars/jars.txt | tr ',' '\n' \
&& chmod -R 755 /opt/spark-jars

RUN useradd -m -s /bin/bash jupyter
USER jupyter
WORKDIR /home/jupyter/notebooks

EXPOSE 8888

CMD ["python3.11", "-m", "jupyter", "lab", \
     "--ip=0.0.0.0", "--port=8888", "--no-browser", \
     "--ServerApp.token=''", "--ServerApp.password=''", \
     "--ServerApp.root_dir=/home/jupyter/notebooks"]
