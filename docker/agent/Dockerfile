FROM python:3.11-slim

WORKDIR /app

COPY src/agent/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/agent/ .
COPY src/api/ ./api/

CMD ["python", "-c", "from agent import run_campaign; print('Agent service ready')"]
