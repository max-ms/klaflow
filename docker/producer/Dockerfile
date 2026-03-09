FROM python:3.11-slim

WORKDIR /app

COPY src/producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/producer/ .

CMD ["python", "producer.py"]
