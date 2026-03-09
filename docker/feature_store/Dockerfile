FROM python:3.11-slim

WORKDIR /app

COPY src/feature_store/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/feature_store/ .
COPY src/ml_models/score_customers.py ./score_customers.py

CMD ["python", "feature_writer.py"]
