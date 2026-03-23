FROM apache/flink:1.20

# Install Python dev tools + JDK headers for building native extensions
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3-pip python3-dev gcc && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Download Flink Kafka connector JAR (fat JAR, includes kafka-clients)
# No avro-confluent JARs needed — Avro deserialization happens in Python via fastavro
RUN cd /opt/flink/lib && \
    curl -LO https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar

# Install PyFlink — skip pemja (JNI bridge, not needed for Table/DataStream API)
RUN pip3 install --no-cache-dir \
    apache-flink==1.20.1 \
    requests==2.31.0 \
    --no-deps 2>/dev/null || true && \
    pip3 install --no-cache-dir \
    apache-flink-libraries==1.20.1 \
    apache-beam==2.48.0 \
    cloudpickle==2.2.1 \
    fastavro==1.12.1 \
    numpy==1.24.4 \
    pandas \
    protobuf==4.23.4 \
    pyarrow==11.0.0 \
    py4j==0.10.9.7 \
    requests==2.31.0 \
    httplib2 \
    python-dateutil \
    dill==0.3.1.1 \
    grpcio \
    avro-python3 \
    pytz \
    ruamel.yaml \
    objsize \
    regex \
    typing-extensions \
    find-libpython

# Copy job files
COPY src/flink_jobs/customer_aggregation_job.py /opt/flink/jobs/
COPY src/flink_jobs/account_aggregation_job.py /opt/flink/jobs/
