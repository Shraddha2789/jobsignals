# ── JobSignals — Production Dockerfile ─────────────────────────────────────
FROM python:3.11-slim

WORKDIR /app

# Install dependencies before copying source (layer cache optimisation)
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY . .

# Runtime environment — overridden by Railway/Render env vars in production
ENV PYTHONPATH=/app \
    API_HOST=0.0.0.0 \
    API_PORT=8000 \
    API_RELOAD=false \
    API_LOG_LEVEL=info

EXPOSE 8000

# Web service: start the FastAPI server
# Override CMD in Railway worker service to: python -m scripts.scheduler
CMD ["python", "-m", "api.main"]
