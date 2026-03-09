FROM python:3.11-slim

WORKDIR /app

COPY src/api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/api/ .
COPY src/decision_engine/ ./decision_engine/

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
