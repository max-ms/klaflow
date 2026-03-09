FROM python:3.11-slim

WORKDIR /app

COPY docker/batch_aggregation/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/flink_jobs/batch_aggregation.py .

CMD ["python", "batch_aggregation.py"]
