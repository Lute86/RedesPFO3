FROM python:3.12-slim

WORKDIR /app

# Install dependencies first (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY servidor.py .

# Default port — overridden by docker-compose via SERVER_PORT env var
EXPOSE 5100

CMD ["python3", "-u", "servidor.py"]
