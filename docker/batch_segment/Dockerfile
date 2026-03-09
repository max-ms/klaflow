FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir confluent-kafka[avro]==2.3.0 requests==2.31.0

COPY src/flink_job/batch_segment.py .

CMD ["python", "batch_segment.py"]
