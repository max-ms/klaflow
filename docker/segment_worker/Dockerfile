FROM python:3.11-slim

WORKDIR /app

COPY src/segment_worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/segment_worker/ .

CMD ["python", "segment_evaluator.py"]
