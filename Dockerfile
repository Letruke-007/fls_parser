FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

COPY app /app

RUN useradd --create-home --uid 10001 appuser \
 && mkdir -p /data/results /data/jobs \
 && chown -R appuser:appuser /app /data

USER appuser

EXPOSE 8000

ENV STORAGE_DIR=/data \
    RETENTION_SECONDS=3600 \
    WORKERS=2 \
    LOG_LEVEL=INFO \
    CALLBACK_TIMEOUT_SECONDS=20 \
    MAX_FILE_SIZE_BYTES=10485760 \
    ALLOWED_ORIGINS=http://localhost:8000,http://127.0.0.1:8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=3).read()" || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]
