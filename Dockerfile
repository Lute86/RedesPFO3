FROM python:3.12-slim

WORKDIR /app

# Install curl for healthcheck (Flask es HTTP, no necesita netcat)
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies first (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY server.py client.py ./
COPY static ./static

# Default port — overridden by docker-compose via SERVER_PORT env var
EXPOSE 5000

CMD ["python3", "-u", "server.py"]
