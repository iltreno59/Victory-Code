# syntax=docker/dockerfile:1

# 1) Base image
FROM python:3.11-slim

# 2) Environment
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# 3) Workdir
WORKDIR /app

# 4) System deps (ca-certificates, locales if needed)
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# 5) Copy only requirement hints first (to leverage Docker layer cache)
# We do not have a requirements.txt, install directly

# 6) Install Python deps
RUN pip install --no-cache-dir \
    requests \
    beautifulsoup4 \
    urllib3

# 7) Copy project files
COPY . /app

# 8) Ensure images directory exists at runtime
RUN mkdir -p /app/images

# 9) Default entrypoint: run the parser; pass args through
ENTRYPOINT ["python", "-u", "/app/parse_polrkrf.py"]

# Example: docker run --rm -v $(pwd)/images:/app/images -v $(pwd)/metadata.jsonl:/app/metadata.jsonl IMAGE 100
