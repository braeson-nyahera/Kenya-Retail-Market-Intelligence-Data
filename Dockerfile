# Dockerfile
FROM python:3.12-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/ 

WORKDIR /app

# Copy dependency manifests first (layer cache)
COPY pyproject.toml uv.lock .python-version ./
COPY src/ ./src/

# Install dependencies
RUN uv sync --frozen --no-dev

# Copy remaining source
COPY main.py .

EXPOSE 9092 9093

CMD ["uv", "run", "python", "main.py"]   