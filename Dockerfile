# Dockerfile
FROM python:3.12-slim

# Install OpenJDK for PySpark
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/ 

WORKDIR /app

# Copy dependency manifests first (layer cache)
COPY pyproject.toml .python-version ./
COPY src/ ./src/

# Install dependencies
RUN uv sync --no-dev

# Copy remaining source
COPY main.py .

EXPOSE 9092 9093

CMD ["uv", "run", "python", "main.py"]
