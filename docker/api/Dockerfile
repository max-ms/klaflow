FROM python:3.11-slim

WORKDIR /app

COPY src/api/requirements.txt ./requirements-api.txt
COPY src/agent/requirements.txt ./requirements-agent.txt
RUN pip install --no-cache-dir -r requirements-api.txt -r requirements-agent.txt

COPY src/api/ .
COPY src/decision_engine/ ./decision_engine/
COPY src/agent/ ./agent/

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
