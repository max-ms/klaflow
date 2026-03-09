FROM python:3.11-slim

WORKDIR /app

COPY src/ml_models/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ml_models/ .

CMD ["uvicorn", "score_customers:app", "--host", "0.0.0.0", "--port", "8000"]
