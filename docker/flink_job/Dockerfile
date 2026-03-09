FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

WORKDIR /app

COPY src/flink_job/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/flink_job/ .

CMD ["python", "segmentation_job.py"]
